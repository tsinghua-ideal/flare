use core::panic::Location;
use std::any::{Any, TypeId};
use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, hash_map::DefaultHasher, HashMap, HashSet};
use std::cmp::{min, Ordering, Reverse};
use std::hash::{Hash, Hasher};
use std::mem::forget;
use std::ops::{Deref, DerefMut};
use std::raw::TraitObject;
use std::string::ToString;
use std::sync::{
    atomic::{self, AtomicBool, AtomicU32, AtomicUsize},
    Arc, SgxMutex as Mutex, SgxRwLock as RwLock, Weak,
};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use sgx_types::*;
use rand::{Rng, SeedableRng};

use crate::{CACHE, Fn, OP_MAP};
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::custom_thread::PThread;
use crate::dependency::{Dependency, OneToOneDependency, ShuffleDependencyTrait};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::utils;
use crate::utils::random::{BernoulliCellSampler, BernoulliSampler, PoissonSampler, RandomSampler};

mod aggregated_op;
pub use aggregated_op::*;
mod count_op;
pub use count_op::*;
mod co_grouped_op;
pub use co_grouped_op::*;
mod flatmapper_op;
pub use flatmapper_op::*;
mod fold_op;
pub use fold_op::*;
mod local_file_reader;
pub use local_file_reader::*;
mod mapper_op;
pub use mapper_op::*;
mod map_partitions_op;
pub use map_partitions_op::*;
mod pair_op;
pub use pair_op::*;
mod parallel_collection_op;
pub use parallel_collection_op::*;
mod partitionwise_sampled_op;
pub use partitionwise_sampled_op::*;
mod reduced_op;
pub use reduced_op::*;
mod shuffled_op;
pub use shuffled_op::*;
mod union_op;
pub use union_op::*;
mod zip_op;
pub use zip_op::*;

type PT<T, TE> = Text<T, TE>;
pub type OText<T> = Text<T, T>; 
type ResIter<T> = Box<dyn Iterator<Item = Box<dyn Iterator<Item = T>>>>;

pub const MAX_ENC_BL: usize = 1024;
pub const MERGE_FACTOR: usize = 64;
pub const CACHE_LIMIT: usize = 4_000_000;
pub type Result<T> = std::result::Result<T, &'static str>;

extern "C" {
    pub fn ocall_cache_to_outside(ret_val: *mut u8,   //write successfully or not
        rdd_id: usize,
        part_id: usize,
        data_ptr: usize,
    ) -> sgx_status_t;

    pub fn ocall_cache_from_outside(ret_val: *mut usize,  //ptr outside enclave
        rdd_id: usize,
        part_id: usize,
    ) -> sgx_status_t;
}

pub fn default_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn load_opmap() -> &'static mut BTreeMap<OpId, Arc<dyn OpBase>> {
    unsafe { 
        OP_MAP.load(atomic::Ordering::Relaxed)
            .as_mut()
    }.unwrap()
}

pub fn insert_opmap(op_id: OpId, op_base: Arc<dyn OpBase>) {
    let op_map = load_opmap();
    op_map.insert(op_id, op_base);
}

#[inline(always)]
pub fn encrypt(pt: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.encrypt(nonce, pt).expect("encryption failure")
}

#[inline(always)]
pub fn ser_encrypt<T>(pt: Vec<T>) -> Vec<u8> 
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
    match TypeId::of::<u8>() == TypeId::of::<T>() {
        true => {
            let (ptr, len, cap) = pt.into_raw_parts();
            let rebuilt = unsafe {
                let ptr = ptr as *mut u8;
                Vec::from_raw_parts(ptr, len, cap)
            };
            encrypt(&rebuilt)
        },
        false => encrypt(bincode::serialize(&pt).unwrap().as_ref()),
    } 
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn ser_decrypt<T>(ct: Vec<u8>) -> Vec<T> 
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
    if ct.len() == 0 {
        return Vec::new();
    }
    match TypeId::of::<u8>() == TypeId::of::<T>() {
        true => {
            let pt = decrypt(&ct);
            let (ptr, len, cap) = pt.into_raw_parts();
            let rebuilt = unsafe {
                let ptr = ptr as *mut T;
                Vec::from_raw_parts(ptr, len, cap)
            };
            rebuilt
        },
        false => bincode::deserialize(decrypt(&ct).as_ref()).unwrap(),
    }

}

fn batch_encrypt<T, TE, FE>(mut data: Vec<T>, fe: FE) -> Vec<TE> 
where
    FE: Func(Vec<T>)->TE
{
    let mut len = data.len();
    let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL+1);
    while len >= MAX_ENC_BL {
        len -= MAX_ENC_BL;
        let remain = data.split_off(MAX_ENC_BL);
        let input = data;
        data = remain;
        data_enc.push(fe(input));
    }
    if len != 0 {
        data_enc.push(fe(data));
    }
    data_enc
}

fn batch_decrypt<T, TE, FD>(data_enc: Vec<TE>, fd: FD) -> Vec<T> 
where
    FD: Func(TE)->Vec<T>
{
    let mut data = Vec::new();
    for block in data_enc {
        let mut pt = fd(block);
        data.append(&mut pt); //need to check security
    }
    data
}

//The result_enc stays inside
pub fn res_enc_to_ptr<T: Clone>(result_enc: T) -> *mut u8 {
    let result_ptr;
    if crate::immediate_cout {
        crate::ALLOCATOR.set_switch(true);
        result_ptr = Box::into_raw(Box::new(result_enc.clone())) as *mut u8;
        crate::ALLOCATOR.set_switch(false);
    } else {
        result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
    }
    result_ptr
}

//acc stays outside enclave, and v stays inside enclave
pub fn merge_enc<T: Clone>(acc: &mut Vec<T>, v: &T) {
    crate::ALLOCATOR.set_switch(true);
    let v = v.clone();
    acc.push(v);
    crate::ALLOCATOR.set_switch(false);
}

pub fn combine_enc<T: Clone>(acc: &mut Vec<T>, mut other: Vec<T>) {
    crate::ALLOCATOR.set_switch(true);
    acc.append(&mut other);
    drop(other);
    crate::ALLOCATOR.set_switch(false);
}

pub fn create_enc<T: Clone>() -> Vec<T> {
    crate::ALLOCATOR.set_switch(true);
    let acc = Vec::new();
    crate::ALLOCATOR.set_switch(false);
    acc
}

//The result_enc stays outside
pub fn to_ptr<T: Clone>(result_enc: T) -> *mut u8 {
    crate::ALLOCATOR.set_switch(true);
    let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
    crate::ALLOCATOR.set_switch(false);
    result_ptr
}

#[track_caller]
fn get_text_loc_id() -> u64 {
    let loc = Location::caller();
    let file = loc.file().replace("_", "");
    let line = loc.line();
    let num = 0;
    let id = OpId::new(
        &file,
        line,
        num,
    ).h;
    id
}

// prepare for column step 2, shuffle (transpose)
// sub_parts and buckets_enc stay outside enclave
pub fn column_sort_step_2<K, V, KE, VE>(sub_parts: Vec<(KE, VE)>, max_len: usize, num_output_splits: usize, fe: Box<dyn Func(Vec<(K, V)>) -> (KE, VE)>, fd: Box<dyn Func((KE, VE)) -> Vec<(K, V)>>) -> Vec<Vec<(KE, VE)>>
where
    K: Data + Ord,
    V: Data,
    KE: Data,
    VE: Data,
{
    //TODO: can be optimized to avoid unneccessary copy, as column_sort_step_4 shows
    crate::ALLOCATOR.set_switch(true);
    let mut buckets_enc = vec![Vec::new(); num_output_splits];
    crate::ALLOCATOR.set_switch(false);

    let mut i = 0;
    for sub_part in sub_parts.iter() {
        let mut buckets = vec![Vec::with_capacity(max_len.saturating_sub(1) / num_output_splits + 1); num_output_splits];
        let sub_part = (fd)(sub_part.clone());
        for kc in sub_part {
            buckets[i % num_output_splits].push(kc);
            i += 1;
        }

        for (j, bucket_enc) in buckets.into_iter().map(|x| (fe)(x)).enumerate() {
            crate::ALLOCATOR.set_switch(true);
            buckets_enc[j].push(bucket_enc.clone());
            crate::ALLOCATOR.set_switch(false);
        }
    }
    crate::ALLOCATOR.set_switch(true);
    drop(sub_parts);
    crate::ALLOCATOR.set_switch(false);
    buckets_enc
}

// sub_parts and buckets_enc stay outside enclave
pub fn column_sort_step_4_6_8<K, V, KE, VE>(sub_parts: Vec<(KE, VE)>, cnt_per_partition: usize, max_len: usize, n: usize, num_output_splits: usize, fe: Box<dyn Func(Vec<(K, V)>) -> (KE, VE)>, fd: Box<dyn Func((KE, VE)) -> Vec<(K, V)>>) -> Vec<Vec<(KE, VE)>>
where
    K: Data + Ord,
    V: Data,
    KE: Data,
    VE: Data,
{
    let n_last = n - (sub_parts.len() - 1) * max_len;
    let chunk_size = cnt_per_partition / num_output_splits;
    let mut next_i = 0;
    let mut n_cur = 0;
    let mut n_acc = 0;
    let last_j = sub_parts.len() - 1;
    let mut r = Vec::new();

    crate::ALLOCATOR.set_switch(true);
    let mut buckets_enc = vec![Vec::new(); num_output_splits];
    let mut iter = sub_parts.into_iter();
    let mut iter_cur = iter.next();
    crate::ALLOCATOR.set_switch(false);
    let mut j = 0;

    while iter_cur.is_some() {
        crate::ALLOCATOR.set_switch(true);
        let sub_part_enc = iter_cur.unwrap();
        crate::ALLOCATOR.set_switch(false);

        let n_cur = if j == last_j {
            n_last
        } else {
            max_len
        };
        if n_acc + n_cur >= chunk_size {
            //form a chunk
            if n_acc + n_cur > chunk_size {
                //need to split
                let mut sub_part = r;
                sub_part.append(&mut (fd)(sub_part_enc.clone()));
                r = sub_part.split_off(chunk_size - n_acc);
                let sub_part_enc = (fe)(sub_part);
                crate::ALLOCATOR.set_switch(true);
                buckets_enc[next_i].push(sub_part_enc.clone());
                crate::ALLOCATOR.set_switch(false);
            } else {
                if r.len() > 0 {
                    let r_enc = (fe)(r);
                    r = Vec::new();
                    crate::ALLOCATOR.set_switch(true);
                    buckets_enc[next_i].push(r_enc.clone());
                    crate::ALLOCATOR.set_switch(false);
                }
                crate::ALLOCATOR.set_switch(true);
                buckets_enc[next_i].push(sub_part_enc);
                crate::ALLOCATOR.set_switch(false);
            }
            next_i += 1;
            n_acc = r.len();
        } else {
            if r.len() > 0 {
                let r_enc = (fe)(r);
                r = Vec::new();
                crate::ALLOCATOR.set_switch(true);
                buckets_enc[next_i].push(r_enc.clone());
                crate::ALLOCATOR.set_switch(false);
            }
            crate::ALLOCATOR.set_switch(true);
            buckets_enc[next_i].push(sub_part_enc);
            crate::ALLOCATOR.set_switch(false);
            n_acc += n_cur;
        };


        crate::ALLOCATOR.set_switch(true);
        iter_cur = iter.next();
        crate::ALLOCATOR.set_switch(false);
        j += 1;
    }

    if r.len() > 0 {
        let r_enc = (fe)(r);
        crate::ALLOCATOR.set_switch(true);
        buckets_enc[next_i].push(r_enc.clone());
        crate::ALLOCATOR.set_switch(false);
    }

    buckets_enc
}

pub struct SortHelper<K, V, KE, VE>
where
    K: Data + Ord,
    V: Data,
    KE: Data,
    VE: Data,
{
    data: Vec<Arc<Mutex<(KE, VE)>>>,
    max_len: usize,
    max_key: K,
    num_padding: Arc<AtomicUsize>,
    ascending: bool,
    fe: Box<dyn Func(Vec<(K, V)>) -> (KE, VE)>,
    fd: Box<dyn Func((KE, VE)) -> Vec<(K, V)>>,
    dist: Option<Vec<usize>>,
}

impl<K, V, KE, VE> SortHelper<K, V, KE, VE>
where
    K: Data + Ord,
    V: Data,
    KE: Data,
    VE: Data,
{
    pub fn new(data: Vec<(KE, VE)>, max_len: usize, max_key: K, ascending: bool, fe: Box<dyn Func(Vec<(K, V)>) -> (KE, VE)>, fd: Box<dyn Func((KE, VE)) -> Vec<(K, V)>>) -> Self {
        //the pointer of data is located inside enclave
        //while the vec content is outside enclave
        //if core dump, check this
        let data = data
            .into_iter()
            .map(|x| Arc::new(Mutex::new(x)))
            .collect::<Vec<_>>();
        //note that data is outside enclave
        SortHelper { data, max_len, max_key, num_padding: Arc::new(AtomicUsize::new(0)), ascending, fe, fd, dist: None}
    }

    //optimize for merging sorted arrays
    pub fn new_with(data: Vec<Vec<(KE, VE)>>, max_len: usize, max_key: K, ascending: bool, fe: Box<dyn Func(Vec<(K, V)>) -> (KE, VE)>, fd: Box<dyn Func((KE, VE)) -> Vec<(K, V)>>) -> Self {
        //both outer and inner pointers of (KE, VE) is located inside enclave
        //while the content (KE, VE) is outside enclave
        //if core dump, check this
        let dist = Some(data.iter()
            .map(|p| p.len())
            .scan(0usize, |acc, x| {
                *acc = *acc + x;
                Some(*acc)
            }).collect::<Vec<_>>());
        let data = data.into_iter().map(|p| p.into_iter().map(|x|Arc::new(Mutex::new(x)))).flatten().collect::<Vec<_>>();
        //note that data is outside enclave
        SortHelper { data, max_len, max_key, num_padding: Arc::new(AtomicUsize::new(0)), ascending, fe, fd, dist}
    }

    pub fn sort(&self) {
        let n = self.data.len();
        self.bitonic_sort_arbitrary(0, n, self.ascending);
    }

    fn bitonic_sort_arbitrary(&self, lo: usize, n: usize, dir: bool) {
        if n > 1 {
            match &self.dist {
                Some(dist) => {
                    let m = match dist.binary_search(&(lo + n / 2)) {
                        Ok(idx) => dist[idx] - lo,
                        Err(idx) => dist[idx] - lo,
                    };
                    if m < n {
                        self.bitonic_sort_arbitrary(lo, m, !dir);
                    }
                    if m > 0 {
                        self.bitonic_sort_arbitrary(lo + m, n - m, dir);
                    }
                    self.bitonic_merge_arbitrary(lo, n, dir);
                },
                None => {
                    let m = n / 2;
                    self.bitonic_sort_arbitrary(lo, m, !dir);
                    self.bitonic_sort_arbitrary(lo + m, n - m, dir);
                    self.bitonic_merge_arbitrary(lo, n, dir);
                }
            };
        }
    }

    fn bitonic_merge_arbitrary(&self, lo: usize, n: usize, dir: bool) {
        if n > 1 {
            let max_len = self.max_len;
            let max_key = self.max_key.clone();
            let num_padding = self.num_padding.clone();

            let is_sorted = self.dist.as_ref().map_or(false, |dist| {
                let idx = dist.binary_search(&(lo + n)).unwrap();
                idx == 0 || dist[idx - 1] == lo 
            });
            if is_sorted {
                let mut d_i = Vec::new();
                let mut cnt = 0;
                for i in lo..(lo + n) {
                    d_i.append(&mut (self.fd)(self.data[i].lock().unwrap().clone()));
                    if d_i.len() >= max_len {
                        let mut r = d_i.split_off(max_len);
                        std::mem::swap(&mut r, &mut d_i);
                        if !dir {
                            r.reverse();
                        }
                        let new_d_i = (self.fe)(r);
                        crate::ALLOCATOR.set_switch(true);
                        *self.data[cnt].lock().unwrap() = new_d_i.clone();
                        crate::ALLOCATOR.set_switch(false);
                        cnt += 1;
                    }
                }
                while cnt < n {
                    d_i.resize(max_len, (max_key.clone(), Default::default()));
                    if !dir {
                        d_i.reverse();
                    }
                    let new_d_i = (self.fe)(d_i);
                    crate::ALLOCATOR.set_switch(true);
                    *self.data[cnt].lock().unwrap() = new_d_i.clone();
                    crate::ALLOCATOR.set_switch(false);
                    d_i = Vec::new();
                    cnt += 1;
                }

                if !dir {
                    for i in lo..(lo + n / 2) {
                        let l = lo + n - 1 - i;
                        let mut data_i = self.data[i].lock().unwrap();
                        let mut data_l = self.data[l].lock().unwrap();
                        crate::ALLOCATOR.set_switch(true);
                        std::mem::swap(&mut *data_i, &mut *data_l);
                        crate::ALLOCATOR.set_switch(false);
                    }
                }
            } else {
                let m = n.next_power_of_two() >> 1;
                for i in lo..(lo + n - m) {
                    let l = i + m;
                    //merge
                    let mut data_i = self.data[i].lock().unwrap();
                    let mut data_l = self.data[l].lock().unwrap();
    
                    let d_i = (self.fd)(data_i.clone());
                    let d_l = (self.fd)(data_l.clone());
    
                    let real_len = d_i.len() + d_l.len();
                    let dummy_len = max_len * 2 - real_len;
                    if real_len < max_len * 2 {
                        num_padding.fetch_add(dummy_len, atomic::Ordering::SeqCst);
                    }
    
                    let mut new_d_i = Vec::with_capacity(max_len);
                    let mut new_d_l = Vec::with_capacity(max_len);
                    //desending, append dummy elements first
                    if !dir {
                        new_d_i.append(&mut vec![(max_key.clone(), Default::default()); std::cmp::min(dummy_len, max_len)]);
                        new_d_l.append(&mut vec![(max_key.clone(), Default::default()); dummy_len.saturating_sub(max_len)]);
                    }
    
                    let mut iter_i = if d_i
                        .first()
                        .zip(d_i.last())
                        .map(|(x, y)| (x.0 < y.0) != dir)
                        .unwrap_or(false)
                    {
                        Box::new(d_i.into_iter().rev()) as Box<dyn Iterator<Item = (K, V)>>
                    } else {
                        Box::new(d_i.into_iter()) as Box<dyn Iterator<Item = (K, V)>>
                    };
    
                    let mut iter_l = if d_l
                        .first()
                        .zip(d_l.last())
                        .map(|(x, y)| (x.0 < y.0) != dir)
                        .unwrap_or(false)
                    {
                        Box::new(d_l.into_iter().rev()) as Box<dyn Iterator<Item = (K, V)>>
                    } else {
                        Box::new(d_l.into_iter()) as Box<dyn Iterator<Item = (K, V)>>
                    };
    
                    let mut cur_i = iter_i.next();
                    let mut cur_l = iter_l.next();
    
                    while cur_i.is_some() || cur_l.is_some() {
                        let should_puti = cur_l.is_none()
                            || cur_i.is_some()
                                && (cur_i.as_ref().unwrap().0 < cur_l.as_ref().unwrap().0) == dir;
                        let kv = if should_puti {
                            let t = cur_i.unwrap();
                            cur_i = iter_i.next();
                            t
                        } else {
                            let t = cur_l.unwrap();
                            cur_l = iter_l.next();
                            t
                        };
    
                        if new_d_i.len() < max_len {
                            new_d_i.push(kv);
                        } else {
                            new_d_l.push(kv);
                        }
                    }
    
                    //ascending, append dummy elements finally
                    if dir {
                        new_d_i.resize(max_len, (max_key.clone(), Default::default()));
                        new_d_l.resize(max_len, (max_key.clone(), Default::default()));
                    }
    
                    let new_d_i = (self.fe)(new_d_i);
                    let new_d_l = (self.fe)(new_d_l);
                    crate::ALLOCATOR.set_switch(true);
                    *data_i = new_d_i.clone();
                    *data_l = new_d_l.clone();
                    crate::ALLOCATOR.set_switch(false);
                }
    
                self.bitonic_merge_arbitrary(lo, m, dir);
                self.bitonic_merge_arbitrary(lo + m, n - m, dir);
            }
        }
    }

    pub fn take(&mut self) -> (Vec<(KE, VE)>, usize) {
        let mut data = std::mem::take(&mut self.data);
        let num_padding = self.num_padding.load(atomic::Ordering::SeqCst);
        let num_real_elem = data.len() * self.max_len - num_padding;
        let num_real_sub_part = data.len() - num_padding / self.max_len;
        let num_padding_remaining = num_real_sub_part * self.max_len - num_real_elem;

        crate::ALLOCATOR.set_switch(true);
        //remove dummy elements
        let dummy_sub_parts = data.split_off(num_real_sub_part);
        drop(dummy_sub_parts);
        let mut res = Vec::with_capacity(data.len());
        crate::ALLOCATOR.set_switch(false);
    
        for x in data.into_iter() {
            let x = Arc::try_unwrap(x).unwrap().into_inner().unwrap();
            crate::ALLOCATOR.set_switch(true);
            res.push(x);
            crate::ALLOCATOR.set_switch(false);
        }

        let last = res.last_mut();
        if last.is_some() {
            let last = last.unwrap();
            let mut sub_part = (self.fd)(last.clone());
            sub_part.truncate(self.max_len - num_padding_remaining);
            let sub_part_enc = (self.fe)(sub_part);
            crate::ALLOCATOR.set_switch(true);
            let t = std::mem::take(last);
            drop(t);
            *last = sub_part_enc.clone();
            crate::ALLOCATOR.set_switch(false);
        }

        (res, num_real_elem)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    caching_op_id: OpId,
    caching_part_id: usize,
    pub cached_rdd_id: usize,
    cached_op_id: OpId,
    cached_part_id: usize,
}

impl CacheMeta {
    pub fn new(
        caching_rdd_id: usize,
        caching_op_id: OpId,
        caching_part_id: usize,
        cached_rdd_id: usize,
        cached_op_id: OpId,
        cached_part_id: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            caching_op_id,
            caching_part_id,
            cached_rdd_id,
            cached_op_id,
            cached_part_id,
        }
    }

    pub fn transform(self) -> Self {
        CacheMeta {
            caching_rdd_id: 0,
            caching_op_id: Default::default(),
            caching_part_id: Default::default(),
            cached_rdd_id: self.caching_rdd_id,
            cached_op_id: self.caching_op_id,
            cached_part_id: self.caching_part_id,
        }
    }

}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DepInfo {
    is_shuffle: u8,
    identifier: usize,
    parent_rdd_id: usize,
    child_rdd_id: usize, 
    parent_op_id: OpId,
    child_op_id: OpId,
}

impl DepInfo {
    pub fn new(is_shuffle: u8,
        identifier: usize,
        parent_rdd_id: usize,
        child_rdd_id: usize,
        parent_op_id: OpId,
        child_op_id: OpId,
    ) -> Self {
        DepInfo {
            is_shuffle,
            identifier,
            parent_rdd_id,
            child_rdd_id,
            parent_op_id,
            child_op_id, 
        }
    }

    pub fn padding_new(is_shuffle: u8) -> Self {
        DepInfo {
            is_shuffle,
            identifier: 0,
            parent_rdd_id: 0,
            child_rdd_id: 0, 
            parent_op_id: Default::default(),
            child_op_id: Default::default(),
        }
    }

    pub fn get_op_key(&self) -> (OpId, OpId) {
        (
            self.parent_op_id,
            self.child_op_id,
        )
    }

    pub fn get_identifier(&self) -> usize {
        self.identifier
    }

    /// 0 for narrow, 1 for shuffle write, 2 for shuffle read, 3, 4 for action
    /// 20-23 for column sort
    pub fn dep_type(&self) -> u8 {
        self.is_shuffle
    }

}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Input {
    data: usize,
    parallel_num: usize,
}

impl Input {
    pub fn new<T: Data>(data: &T, 
        parallel_num: usize,
    ) -> Self {
        let data = data as *const T as usize;
        Input {
            data,
            parallel_num,
        }
    }

    pub fn get_enc_data<T>(&self) -> &T {
        unsafe { (self.data as *const T).as_ref() }.unwrap()
    }

    pub fn get_parallel(&self) -> usize {
        self.parallel_num
    }
}

#[derive(Default, Clone)]
pub struct Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    data: T,
    id: u64,
    bfe: Option<Box<dyn Func(T) -> TE>>,
    bfd: Option<Box<dyn Func(TE) -> T>>,
}

impl<T, TE> Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    #[track_caller]
    pub fn new(data: T, bfe: Option<Box<dyn Func(T) -> TE>>, bfd: Option<Box<dyn Func(TE) -> T>>) -> Self {
        let id = get_text_loc_id();
        Text {
            data,
            id,
            bfe,
            bfd,
        }
    }

    #[track_caller]
    pub fn rec(tail_info: &TailCompInfo, bfe: Option<Box<dyn Func(T) -> TE>>, bfd: Option<Box<dyn Func(TE) -> T>>) -> Self {
        let id = get_text_loc_id();
        let data = match tail_info.get(id) {
            Some(data_enc) => {
                match &bfd {
                    Some(bfd) => bfd(data_enc),
                    None => {
                        let data_enc = Box::new(data_enc) as Box<dyn Any>;
                        *data_enc.downcast::<T>().unwrap()
                    },
                }
            },
            None => {
                Default::default()
            },
        };

        Text {
            data,
            id,
            bfe,
            bfd,
        }
    }

    pub fn get_ct(&self) -> TE {
        match &self.bfe {
            Some(bfe) => bfe(self.data.clone()),
            None => {
                let data_enc = Box::new(self.data.clone()) as Box<dyn Any>;
                *data_enc.downcast::<TE>().unwrap()
            },
        }
    }

    pub fn get_pt(&self) -> T {
        self.data.clone()
    }

    pub fn get_tail_info(&self) -> (u64, &T) {
        (self.id, &self.data)
    }

    pub fn update_from_tail_info(&mut self, tail_info: &TailCompInfo) {
        let data_enc = tail_info.get(self.id).unwrap();
        self.data = match &self.bfd {
            Some(bfd) => bfd(data_enc),
            None => {
                let data_enc = Box::new(data_enc) as Box<dyn Any>;
                *data_enc.downcast::<T>().unwrap()
            },
        };
    }

}

impl<T, TE> Deref for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T, TE> DerefMut for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct TailCompInfo {
    m: HashMap<u64, Vec<u8>>,
}

impl TailCompInfo {
    pub fn new() -> Self {
        TailCompInfo {
            m: HashMap::new(),
        }
    }

    pub fn insert<T, TE>(&mut self, text: &Text<T, TE>)
    where
        T: Data,
        TE: Data,
    {
        let ser = bincode::serialize(&text.get_ct()).unwrap();
        self.m.insert(text.id, ser);
    }

    pub fn remove<TE: Data>(&mut self, id: u64) -> TE {
        let ser = self.m.remove(&id).unwrap();
        bincode::deserialize(&ser).unwrap()
    }

    pub fn get<TE: Data>(&self, id: u64) -> Option<TE> {
        match self.m.get(&id) {
            Some(ser) => Some(bincode::deserialize(&ser).unwrap()),
            None => None,
        }
    }

    pub fn clear(&mut self) {
        self.m.clear();
    }

}

#[derive(Clone)]
pub struct OpCache {
    //<(cached_rdd_id, part_id), data, op id>, data can not be Any object, required by lazy_static
    in_map: Arc<RwLock<HashMap<(usize, usize), (usize, OpId)>>>,
    //temperarily save the point of data, which is ready to sent out
    // <(cached_rdd_id, part_id), data ptr>
    out_map: Arc<RwLock<HashMap<(usize, usize), usize>>>,
    //<(cached_rdd_id, part_id), (start_block_id, end_block_id)>
    //contains all blocks whose corresponding values are cached inside enclave
    block_map: Arc<RwLock<HashMap<(usize, usize), (usize, usize)>>>,
}

impl OpCache{
    pub fn new() -> Self {
        OpCache {
            in_map: Arc::new(RwLock::new(HashMap::new())),
            out_map: Arc::new(RwLock::new(HashMap::new())),
            block_map: Arc::new(RwLock::new(HashMap::new())), 
        }
    }

    pub fn get(&self, key: (usize, usize)) -> Option<(usize, OpId)> {
        self.in_map.read().unwrap().get(&key).map(|x| *x)
    }

    pub fn insert(&self, key: (usize, usize), data: usize, op_id: OpId) -> Option<(usize, OpId)> {
        self.in_map.write().unwrap().insert(key, (data, op_id))
    }

    //free the value?
    pub fn remove(&self, key: (usize, usize)) -> Option<(usize, OpId)> {
        self.in_map.write().unwrap().remove(&key)
    }

    pub fn insert_ptr(&self, key: (usize, usize), data_ptr: usize) {
        self.out_map.write().unwrap().insert(key, data_ptr);
    }

    pub fn remove_ptr(&self, key: (usize, usize)) -> Option<usize> {
        self.out_map.write().unwrap().remove(&key)
    }

    pub fn insert_bid(&self, rdd_id: usize, part_id: usize, start_bid: usize, end_bid: usize) {
        let mut res =  self.block_map.write().unwrap();
        if let Some(set) = res.get_mut(&(rdd_id, part_id)) {
            *set = (std::cmp::min(start_bid, set.0), std::cmp::max(end_bid, set.1));
            return;
        }
        res.insert((rdd_id, part_id), (start_bid, end_bid));
    }

    pub fn remove_bid(&self, rdd_id: usize, part_id: usize) -> Option<(usize, usize)> {
        let mut block_map = self.block_map.write()
            .unwrap();
        block_map.remove(&(rdd_id, part_id))
    }

    pub fn get_bid(&self, rdd_id: usize, part_id: usize) -> Option<(usize, usize)> {
        self.block_map.read()
            .unwrap()
            .get(&(rdd_id, part_id))
            .map(|v| *v)
    }

    pub fn clear(&self) {
        let keys = self.in_map.read().unwrap().keys().map(|x| *x).collect::<Vec<_>>();
        let mut map = self.in_map.write().unwrap();
        for key in keys {
            if let Some((data_ptr, op_id)) = map.remove(&key) {
                let op = load_opmap().get(&op_id).unwrap();
                op.call_free_res_enc(data_ptr as *mut u8, false, &DepInfo::padding_new(0));
            }
        }
    }

}
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpId {
    h: u64,
}

impl OpId {
    pub fn new(file: &str, line: u32, num: usize) -> Self {
        let h = default_hash(&(file.to_string(), line, num));
        OpId {
            h,
        }
    }
}

pub struct NextOpId<'a> {
    tid: u64,
    rdd_ids: &'a Vec<usize>,
    op_ids: &'a Vec<OpId>,
    part_ids: &'a Vec<usize>,
    cur_idx: usize,
    cache_meta: CacheMeta,
    captured_vars: HashMap<usize, Vec<Vec<u8>>>,
    is_shuffle: bool,
}

impl<'a> NextOpId<'a> {
    pub fn new(tid: u64, 
        rdd_ids: &'a Vec<usize>, 
        op_ids: &'a Vec<OpId>, 
        part_ids: &'a Vec<usize>, 
        cache_meta: CacheMeta, 
        captured_vars: HashMap<usize, Vec<Vec<u8>>>, 
        dep_info: &DepInfo,
    ) -> Self {
        let is_shuffle = dep_info.is_shuffle == 1;
        NextOpId {
            tid,
            rdd_ids,
            op_ids,
            part_ids,
            cur_idx: 0,
            cache_meta,
            captured_vars,
            is_shuffle,
        }
    }

    fn get_cur_rdd_id(&self) -> usize {
        self.rdd_ids[self.cur_idx]
    }

    fn get_cur_op_id(&self) -> OpId {
        self.op_ids[self.cur_idx]
    }
    
    fn get_part_id(&self) -> usize {
        self.part_ids[self.cur_idx]
    }

    pub fn get_cur_op(&self) -> &'static Arc<dyn OpBase> {
        let cur_op_id = self.get_cur_op_id();
        load_opmap().get(&cur_op_id).unwrap()
    }

    pub fn get_next_op(&mut self) -> &'static Arc<dyn OpBase> {
        self.cur_idx += 1;
        let next_op_id = self.get_cur_op_id();
        load_opmap().get(&next_op_id).unwrap()
    }

    pub fn get_ser_captured_var(&self) -> Option<&Vec<Vec<u8>>> {
        let cur_rdd_id = self.get_cur_rdd_id();
        self.captured_vars.get(&cur_rdd_id)
    }

    pub fn get_caching_doublet(&self) -> (usize, usize) {
        (
            self.cache_meta.caching_rdd_id,
            self.cache_meta.caching_part_id,
        )
    }

    pub fn get_cached_doublet(&self) -> (usize, usize) {
        (
            self.cache_meta.cached_rdd_id,
            self.cache_meta.cached_part_id,
        )
    }

    pub fn have_cache(&self) -> bool {
        self.get_cur_rdd_id() == self.cache_meta.cached_rdd_id
        && self.cache_meta.cached_rdd_id != 0
    }

    pub fn need_cache(&self) -> bool {
        self.get_cur_rdd_id() == self.cache_meta.caching_rdd_id
        && self.cache_meta.caching_rdd_id != 0
    }

    pub fn is_caching_final_rdd(&self) -> bool {
        self.cur_idx == 0
        //It seems not useful unless a cached rdd in shuffle task is simultaniously relied on in another result task
        && !self.is_shuffle
    }

    pub fn is_head(&self) -> bool {
        self.cur_idx == self.rdd_ids.len() - 1
    }
}

#[derive(Default)]
pub struct Context {
    last_loc_file: RwLock<&'static str>,
    last_loc_line: AtomicU32,
    num: AtomicUsize,
    in_loop: AtomicBool,
    is_tail_comp: AtomicBool,
}

impl Context {
    pub fn new() -> Result<Arc<Self>> {
        Ok(Arc::new(Context {
            last_loc_file: RwLock::new("null"),
            last_loc_line: AtomicU32::new(0), 
            num: AtomicUsize::new(0),
            in_loop: AtomicBool::new(false),
            is_tail_comp: AtomicBool::new(false),
        }))
    }

    pub fn add_num(self: &Arc<Self>, addend: usize) -> usize {
        self.num.fetch_add(addend, atomic::Ordering::SeqCst)
    }

    pub fn set_num(self: &Arc<Self>, num: usize) {
        self.num.store(num, atomic::Ordering::SeqCst)
    }

    pub fn enter_loop(self: &Arc<Self>) {
        self.in_loop.store(true, atomic::Ordering::SeqCst);
    }

    pub fn leave_loop(self: &Arc<Self>) {
        self.in_loop.store(false, atomic::Ordering::SeqCst);
    }

    pub fn set_is_tail_comp(self: &Arc<Self>, is_tail_comp: bool) {
        self.is_tail_comp.store(is_tail_comp, atomic::Ordering::SeqCst)
    }

    pub fn get_is_tail_comp(self: &Arc<Self>) -> bool {
        self.is_tail_comp.load(atomic::Ordering::SeqCst)
    }

    pub fn new_op_id(self: &Arc<Self>, loc: &'static Location<'static>) -> OpId {
        use atomic::Ordering::SeqCst;
        let file = loc.file();
        let line = loc.line();
        
        let num = if *self.last_loc_file.read().unwrap() != file || self.last_loc_line.load(SeqCst) != line {
            *self.last_loc_file.write().unwrap() = file;
            self.last_loc_line.store(line, SeqCst);
            self.num.store(0, SeqCst);
            0
        } else {
            self.num.load(SeqCst)
        };
        
        let op_id = 
        OpId::new(
            file,
            line,
            num,
        );
        //println!("op, file = {:?}, line = {:?}, num = {:?}, op_id = {:?}", file, line, num, op_id);
        op_id
    }

    #[track_caller]
    pub fn make_op<T, TE, FE, FD>(self: &Arc<Self>, fe: FE, fd: FD, num_splits: usize) -> SerArc<dyn OpE<Item = T, ItemE = TE>> 
    where
        T: Data,
        TE: Data,
        FE: SerFunc(Vec<T>) -> TE,
        FD: SerFunc(TE) -> Vec<T>,
    {
        let new_op = SerArc::new(ParallelCollection::new(self.clone(), fe, fd, num_splits));
        if !self.get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    /// Load from a distributed source and turns it into a parallel collection.
    #[track_caller]
    pub fn read_source<C, FE, FD, I: Data, O: Data, OE: Data>(
        self: &Arc<Self>,
        config: C,
        func: Option< Box<dyn Func(I) -> O >>,
        sec_func: Option< Box<dyn Func(I) -> Vec<OE> >>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        C: ReaderConfiguration<I>,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>,
    {
        config.make_reader(self.clone(), func, sec_func, fe, fd)
    }

    #[track_caller]
    pub fn union<T: Data, TE: Data>(rdds: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> impl OpE<Item = T, ItemE = TE> {
        Union::new(rdds)
    }

}

pub(crate) struct OpVals {
    pub id: OpId,
    pub split_num: AtomicUsize,
    pub deps: Vec<Dependency>,
    pub context: Weak<Context>,
    pub in_loop: bool,
}

impl OpVals {
    #[track_caller]
    pub fn new(sc: Arc<Context>, split_num: usize) -> Self {
        let loc = Location::caller(); 
        OpVals {
            id: sc.new_op_id(loc),
            split_num: AtomicUsize::new(split_num),
            deps: Vec::new(),
            context: Arc::downgrade(&sc),
            in_loop: sc.in_loop.load(atomic::Ordering::SeqCst),
        }
    }
}

pub trait OpBase: Send + Sync {
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo);
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo);
    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo);
    fn fix_split_num(&self, split_num: usize) {
        unreachable!()
    }
    fn get_op_id(&self) -> OpId;
    fn get_context(&self) -> Arc<Context>;
    fn get_deps(&self) -> Vec<Dependency> {
        unreachable!()
    }
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        unreachable!()
    }
    fn get_next_shuf_dep(&self, dep_info: &DepInfo) -> Option<Arc<dyn ShuffleDependencyTrait>> {
        let cur_key = dep_info.get_op_key();
        let next_deps = self.get_next_deps();
        let res = match next_deps.read().unwrap().get(&cur_key) {
            Some(dep) => match dep {
                Dependency::ShuffleDependency(shuf_dep) => Some(shuf_dep.clone()),
                Dependency::NarrowDependency(nar_dep) => None,
            },
            None => None,
        };
        res
    }
    //supplement
    fn sup_next_shuf_dep(&self, dep_info: &DepInfo, reduce_num: usize) {
        let cur_key = dep_info.get_op_key();
        let next_deps = self.get_next_deps().read().unwrap().clone();
        match next_deps.get(&cur_key) {
            None => {  //not exist, add the dependency
                let child = load_opmap().get(&cur_key.1).unwrap();
                for value in child.get_deps() {
                    match value {
                        Dependency::ShuffleDependency(shuf_dep) => {
                            if dep_info.identifier == shuf_dep.get_identifier() {
                                self.get_next_deps().write().unwrap().insert(
                                    cur_key, 
                                    Dependency::ShuffleDependency(
                                        shuf_dep.set_parent_and_child(cur_key.0, cur_key.1)
                                    )
                                );
                                break;
                            }
                        },
                        Dependency::NarrowDependency(_) => (),
                    };
                }  
            },
            Some(dep) => { //already exist, check and change the partitioner
                match dep {
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep.change_partitioner(reduce_num),
                    Dependency::NarrowDependency(nar_dep) => panic!("should not be narrow dep"),
                };
            },
        }
    }
    fn sup_next_nar_dep(&self, child: OpId) {
        let next_deps = self.get_next_deps().read().unwrap().clone();
        let parent = self.get_op_id();
        let next_dep = next_deps.get(&(parent, child));
        match next_dep {
            None => {
                self.get_next_deps().write().unwrap().insert(
                    (parent, child),
                    Dependency::NarrowDependency(
                        Arc::new(OneToOneDependency::new(parent, child))
                    )
                );
            },
            Some(dep) => match dep {
                Dependency::NarrowDependency(_) => (),
                Dependency::ShuffleDependency(_) => panic!("dependency conflict!"),
            },
        }
    }
    fn is_in_loop(&self) -> bool {
        unreachable!()
    }
    fn number_of_splits(&self) -> usize {
        unreachable!()
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        unreachable!()
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        unreachable!()
    }
    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        unreachable!()
    }
    fn pre_merge(&self, dep_info: DepInfo, tid: u64, input: Input) -> usize {
        let shuf_dep = self.get_next_shuf_dep(&dep_info).unwrap();
        shuf_dep.pre_merge(tid, input)
    }
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        unreachable!()
    }
}

impl dyn OpBase {
    pub fn to_arc_op<T: ?Sized + Any>(self: Arc<Self>) -> Option<Arc<T>> {
        if let Some(mut u) = self.__to_arc_op(TypeId::of::<T>()) {
            Some(unsafe{
                Arc::from_raw(*std::mem::transmute::<_, &mut *mut T>(&mut u))
            })
        } else {
            None
        }
    }
}

impl PartialOrd for dyn OpBase {
    fn partial_cmp(&self, other: &dyn OpBase) -> Option<Ordering> {
        Some(self.get_op_id().cmp(&other.get_op_id()))
    }
}

impl PartialEq for dyn OpBase {
    fn eq(&self, other: &dyn OpBase) -> bool {
        self.get_op_id() == other.get_op_id()
    }
}

impl Eq for dyn OpBase {}

impl Ord for dyn OpBase {
    fn cmp(&self, other: &dyn OpBase) -> Ordering {
        self.get_op_id().cmp(&other.get_op_id())
    }
}

impl<I: OpE + ?Sized> OpBase for SerArc<I> {
    //need to avoid freeing the encrypted data for subsequent "clone_enc_data_out"
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).build_enc_data_sketch(p_buf, p_data_enc, dep_info);
    }
    //need to free the encrypted data. But how to deal with task failure?
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).clone_enc_data_out(p_out, p_data_enc, dep_info);
    }
    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        (**self).call_free_res_enc(res_ptr, is_enc, dep_info);
    }
    fn fix_split_num(&self, split_num: usize) {
        (**self).fix_split_num(split_num)
    }
    fn get_op_id(&self) -> OpId {
        (**self).get_op_base().get_op_id()
    }
    fn get_context(&self) -> Arc<Context> {
        (**self).get_op_base().get_context()
    }
    fn get_deps(&self) -> Vec<Dependency> {
        (**self).get_op_base().get_deps()
    }
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        (**self).get_op_base().get_next_deps()
    }
    fn is_in_loop(&self) -> bool {
        (**self).get_op_base().is_in_loop()
    }
    fn number_of_splits(&self) -> usize {
        (**self).get_op_base().number_of_splits()
    }
    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        (**self).get_op_base().iterator_start(call_seq, input, dep_info)
    }
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        (**self).randomize_in_place(input, seed, num)
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        (**self).set_sampler(with_replacement, fraction)
    }
    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        (**self).etake(input, should_take, have_take)
    }
    fn pre_merge(&self, dep_info: DepInfo, tid: u64, input: Input) -> usize {
        (**self).pre_merge(dep_info, tid, input)
    }
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        (**self).clone().__to_arc_op(id)
    }
}

impl<I: OpE + ?Sized> Op for SerArc<I> {
    type Item = I::Item;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        (**self).get_op()
    } 
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        (**self).get_op_base()
    }
    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        (**self).compute_start(call_seq, input, dep_info)
    }
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        (**self).compute(call_seq, input)
    }
    fn cache(&self, data: Vec<Self::Item>) {
        (**self).cache(data);
    }
}

impl<I: OpE + ?Sized> OpE for SerArc<I> {
    type ItemE = I::ItemE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        (**self).get_ope()
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        (**self).get_fe()
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        (**self).get_fd()
    }
}


pub trait Op: OpBase + 'static {
    type Item: Data;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_op_base(&self) -> Arc<dyn OpBase>;
    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item>;
    fn cache(&self, data: Vec<Self::Item>) {
        ()
    }
}

pub trait OpE: Op {
    type ItemE: Data;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>;

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE>;

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>>;
    
    fn cache_from_outside(&self, key: (usize, usize)) -> Option<&'static Vec<Self::ItemE>> {
        let mut ptr: usize = 0;
        let sgx_status = unsafe { 
            ocall_cache_from_outside(&mut ptr, key.0, key.1)
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] OCALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        if ptr == 0 {
            return None;
        }
        /*
        let ct_ = unsafe {
            Box::from_raw(ptr as *mut u8 as *mut Vec<Self::ItemE>)
        };
        let ct = ct_.clone();
        forget(ct_);
        self.batch_decrypt(*ct)
        */
        Some(unsafe {
            (ptr as *const u8 as *const Vec<Self::ItemE>).as_ref()
        }.unwrap())
    }

    fn cache_to_outside(&self, key: (usize, usize), value: Vec<Self::Item>) -> Option<PThread> {
        //let handle = unsafe {
        //    PThread::new(Box::new(move || {
        let ct = self.batch_encrypt(value);
        //println!("finish encryption, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        let acc = match CACHE.remove_ptr(key) {
            Some(ptr) => {
                crate::ALLOCATOR.set_switch(true);
                let mut acc = *unsafe { Box::from_raw(ptr as *mut Vec<Self::ItemE>) };
                combine_enc(&mut acc, ct);
                crate::ALLOCATOR.set_switch(false);
                to_ptr(acc)
            },
            None => to_ptr(ct), 
        } as usize;
        CACHE.insert_ptr(key, acc);
        //println!("finish copy out, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        //    }))
        //}.unwrap();
        //Some(handle)
        None
    }

    fn get_and_remove_cached_data(&self, key: (usize, usize)) -> ResIter<Self::Item> {
        let fd = self.get_fd();
        let bid_range = CACHE.remove_bid(key.0, key.1);
        
        let mut res_iter = {
            let ct = self.cache_from_outside(key).unwrap();
            let len = ct.len();
            let seperator = bid_range.unwrap_or((len, len));
    
            //cache outside enclave
            Box::new((0..seperator.0).chain(seperator.1..len).map(move |i| {
                //println!("get cached data outside enclave");
                Box::new((fd)(ct[i].clone()).into_iter()) as Box<dyn Iterator<Item = _>>
            })) as ResIter<_>
        };

        //cache inside enclave
        if let Some(val) = CACHE.remove(key) {
            res_iter = Box::new(vec![unsafe {
                    let v = Box::from_raw(val.0 as *mut u8 as *mut Vec<Self::Item>);
                    Box::new(v.into_iter())
                    as Box<dyn Iterator<Item = _>>
                }].into_iter().chain(res_iter));
        };
        Box::new(res_iter)
    }

    fn set_cached_data(&self, call_seq: &NextOpId, res_iter: ResIter<Self::Item>, is_caching_final_rdd: bool) -> ResIter<Self::Item> {
        let ope = self.get_ope();
        let op_id = self.get_op_id();
        let key = call_seq.get_caching_doublet();
        let (lower_bound, upper_bound) = res_iter.size_hint();
        let esti_bound = upper_bound.unwrap_or(lower_bound);
        //set number of blocks that are cached inside enclave
        let num_inside = 1;

        Box::new(res_iter.enumerate().map(move |(idx, iter)| {
            let res = iter.collect::<Vec<_>>();
            //cache inside enclave
            if idx >= esti_bound.saturating_sub(num_inside) {
                let mut data = CACHE.remove(key).map_or(vec![], |x| *unsafe{Box::from_raw(x.0 as *mut u8 as *mut Vec<Self::Item>)});
                data.append(&mut res.clone());
                CACHE.insert(key, Box::into_raw(Box::new(data)) as *mut u8 as usize, op_id);
                CACHE.insert_bid(key.0, key.1, idx, idx+1);
            }
            //println!("After cache inside enclave, memroy usage: {:?} B", crate::ALLOCATOR.get_memory_usage());

            //cache outside enclave
            if is_caching_final_rdd {
                Box::new(res.into_iter()) as Box<dyn Iterator<Item = _>>
            } else {
                let _handle = ope.cache_to_outside(key, res.clone());
                //println!("After launch encryption thread, memroy usage: {:?} B", crate::ALLOCATOR.get_memory_usage());
                Box::new(res.into_iter()) as Box<dyn Iterator<Item = _>>
            }
        }))
    }

    fn step0_of_clone(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match dep_info.dep_type() {
            0 | 2 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            }, 
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.send_sketch(&mut buf, p_data_enc);
            },
            3 | 4 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            }
            _ => panic!("invalid is_shuffle"),
        };
        forget(buf);
    }

    fn step1_of_clone(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Self::ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }, 
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.send_enc_data(p_out, p_data_enc);
            },
            3 | 4 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Self::ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }
            _ => panic!("invalid is_shuffle"),
        }  
    }

    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool) {
        if is_enc {
            crate::ALLOCATOR.set_switch(true);
            let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Self::ItemE>) };
            drop(res);
            crate::ALLOCATOR.set_switch(false);
        } else {
            let _res = unsafe { Box::from_raw(res_ptr as *mut Vec<Self::Item>) };
        }
    }

    fn batch_encrypt(&self, mut data: Vec<Self::Item>) -> Vec<Self::ItemE> {
        let mut acc = create_enc();
        let mut len = data.len();
        while len >= MAX_ENC_BL {
            len -= MAX_ENC_BL;
            let remain = data.split_off(MAX_ENC_BL);
            let input = data;
            data = remain;
            merge_enc(&mut acc, &(self.get_fe())(input));
        }
        if len != 0 {
            merge_enc(&mut acc, &(self.get_fe())(data));
        }
        acc
    }

    fn batch_decrypt(&self, data_enc: Vec<Self::ItemE>) -> Vec<Self::Item> {
        batch_decrypt(data_enc, self.get_fd())
    }

    fn narrow(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        println!("regular narrow");
        let result_iter = self.compute(call_seq, input);
        //acc stays outside enclave
        let mut acc = create_enc();
        for result in result_iter {
            let block_enc = self.batch_encrypt(result.collect::<Vec<_>>());
            combine_enc(&mut acc, block_enc);
        }
        //cache to outside
        let key = call_seq.get_caching_doublet();
        if let Some(ct_ptr) = CACHE.remove_ptr(key) {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, key.0, key.1, ct_ptr); }
            //TODO: Handle the case res != 0
        }
        to_ptr(acc)
    } 

    fn shuffle(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
        let tid = call_seq.tid;
        let now = Instant::now();

        let result_iter = Box::new(self.compute(call_seq, input)
            .map(|x| Box::new(x.collect::<Vec<_>>()) as Box<dyn Any>)) as Box<dyn Iterator<Item = Box<dyn Any>>>;
        let result_ptr = shuf_dep.do_shuffle_task(tid, result_iter, input.get_parallel());

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("tid: {:?}, shuffle write: {:?}s, cur mem: {:?}B", call_seq.tid, dur, crate::ALLOCATOR.get_memory_usage());
        //let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
        result_ptr
    }

    fn randomize_in_place_(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        let sample_enc = unsafe{ (input as *const Vec<Self::ItemE>).as_ref() }.unwrap(); 
        let mut sample = self.batch_decrypt(sample_enc.to_vec());
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };
        utils::randomize_in_place(&mut sample, &mut rng);
        sample = sample.into_iter().take(num as usize).collect();
        let sample_enc = self.batch_encrypt(sample);
        to_ptr(sample_enc)
    }

    fn take_(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        let data_enc = unsafe{ (input as *const Vec<Self::ItemE>).as_ref() }.unwrap(); 
        let mut data = self.batch_decrypt(data_enc.to_vec());
        data = data.into_iter().take(should_take).collect();
        *have_take = data.len();
        let data_enc = self.batch_encrypt(data);
        to_ptr(data_enc)
    }

    /// Return a new RDD containing only the elements that satisfy a predicate.
    #[track_caller]
    fn filter<F>(&self, predicate: F) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + Clone + Copy + 'static,
        Self: Sized,
    {
        let filter_fn = Fn!(move |_index: usize, 
                                  items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> {
            Box::new(items.filter(predicate))
        });
        let new_op = SerArc::new(MapPartitions::new(self.get_op(), filter_fn, self.get_fe(), self.get_fd()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn map<U, UE, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = U, ItemE = UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        F: SerFunc(Self::Item) -> U,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let new_op = SerArc::new(Mapper::new(self.get_op(), f, fe, fd));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn flat_map<U, UE, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = U, ItemE = UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let new_op = SerArc::new(FlatMapper::new(self.get_op(), f, fe, fd));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn reduce<F, UE, FE, FD>(&self, f: F, fe: FE, fd: FD) -> Result<PT<Self::Item, UE>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Self::Item) -> UE,
        FD: SerFunc(UE) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();        
        let reduce_partition = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc { 
                None => vec![],
                Some(e) => vec![e],
            }
        });         
        let new_op = SerArc::new(Reduced::new(self.get_ope(), reduce_partition, fe.clone(), fd.clone()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::new(Default::default(), Some(Box::new(fe)), Some(Box::new(fd))))
    }

    #[track_caller]
    fn secure_reduce<F, UE, FE, FD>(&self, f: F, fe: FE, fd: FD, tail_info: &TailCompInfo) -> Result<PT<Self::Item, UE>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Self::Item) -> UE,
        FD: SerFunc(UE) -> Self::Item,
    {
        Ok(Text::rec(tail_info, Some(Box::new(fe)), Some(Box::new(fd))))
    }

    #[track_caller]
    fn fold<F, UE, FE, FD>(&self, init: Self::Item, f: F, fe: FE, fd: FD) -> Result<PT<Self::Item, UE>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Self::Item) -> UE,
        FD: SerFunc(UE) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                iter.fold(zero.clone(), &cf)
        });
        let new_op = SerArc::new(Fold::new(self.get_ope(), reduce_partition, fe.clone(), fd.clone()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::new(Default::default(), Some(Box::new(fe)), Some(Box::new(fd))))
    }

    #[track_caller]
    fn secure_fold<F, UE, FE, FD>(&self, init: Self::Item, f: F, fe: FE, fd: FD, tail_info: &TailCompInfo) -> Result<PT<Self::Item, UE>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Self::Item) -> UE,
        FD: SerFunc(UE) -> Self::Item,
    {
        Ok(Text::rec(tail_info, Some(Box::new(fe)), Some(Box::new(fd))))
    }

    #[track_caller]
    fn aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD) -> Result<PT<U, UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
        FE: SerFunc(U) -> UE,
        FD: SerFunc(UE) -> U,
    {
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn)
        );
        let zero = init.clone();
        let combine = Fn!(
            move |iter: Box<dyn Iterator<Item = U>>| iter.fold(zero.clone(), &comb_fn)
        );
        let new_op = SerArc::new(Aggregated::new(self.get_ope(), reduce_partition, combine, fe.clone(), fd.clone()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::new(Default::default(), Some(Box::new(fe)), Some(Box::new(fd))))
    }

    #[track_caller]
    fn secure_aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD, tail_info: &TailCompInfo) -> Result<PT<U, UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
        FE: SerFunc(U) -> UE,
        FD: SerFunc(UE) -> U,
    {
        Ok(Text::rec(tail_info, Some(Box::new(fe)), Some(Box::new(fd))))
    }

    #[track_caller]
    fn collect(&self) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>> 
    where
        Self: Sized,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(move |data| batch_encrypt(data, fe.clone()));
        let bfd = Box::new(move |data_enc| batch_decrypt(data_enc, fd.clone()));
        Ok(Text::new(vec![], Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn secure_collect(&self, tail_info: &TailCompInfo) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>> 
    where
        Self: Sized,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(move |data| batch_encrypt(data, fe.clone()));
        let bfd = Box::new(move |data_enc| batch_decrypt(data_enc, fd.clone()));
        Ok(Text::rec(tail_info, Some(bfe), Some(bfd)))
    }
    
    #[track_caller]
    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let new_op = SerArc::new(Count::new(self.get_ope()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(0)
    } 

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
        Self::ItemE: Data + Eq + Hash + Ord,
    {
        let fe_c = self.get_fe();
        let fe_wrapper_mp0 = Box::new(move |v: Vec<(Option<Self::Item>, Option<Self::Item>)>| {
            let (vx, vy): (Vec<Option<Self::Item>>, Vec<Option<Self::Item>>) = v.into_iter().unzip();
            let ct_x = (fe_c)(vx.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>());
            (ct_x, vy)
        }); 
        let fd_c = self.get_fd();
        let fd_wrapper_mp0 = Box::new(move |v: (Self::ItemE, Vec<Option<Self::Item>>)| {
            let (vx, vy) = v;
            let pt_x = (fd_c)(vx).into_iter().map(|x| Some(x));
            pt_x.zip(vy.into_iter()).collect::<Vec<_>>()
        });
        let fe_wrapper_rd = fe_wrapper_mp0.clone();
        let fd_wrapper_rd = fd_wrapper_mp0.clone();
        let fe = self.get_fe();       
        let fe_wrapper_mp1 = Box::new(move |v: Vec<Self::Item>| {
            let ct = (fe)(v);
            ct
        });
        let fd = self.get_fd();
        let fd_wrapper_mp1 = Box::new(move |v: Self::ItemE| {
            let pt = (fd)(v);
            pt
        });
        
        let mapped = self.map(Box::new(Fn!(|x| (Some(x), None)))
            as Box<
                dyn Func(Self::Item) -> (Option<Self::Item>, Option<Self::Item>),
            >, fe_wrapper_mp0, fd_wrapper_mp0);
        self.get_context().add_num(1);
        let reduced_by_key = mapped.reduce_by_key(Box::new(Fn!(|(_x, y)| y)),
            num_partitions,
            fe_wrapper_rd,
            fd_wrapper_rd);
        self.get_context().add_num(1);
        reduced_by_key.map(Box::new(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, _y) = x;
            x.unwrap()
        })), fe_wrapper_mp1, fd_wrapper_mp1)
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct(&self) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
        Self::ItemE: Data + Eq + Hash + Ord,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    #[track_caller]
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        let new_op = SerArc::new(PartitionwiseSampled::new(self.get_op(), sampler, true, self.get_fe(), self.get_fd()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn take_sample(
        &self,
        with_replacement: bool,
        _num: u64,
        _seed: Option<u64>,
    ) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        let _initial_count = self.count()?;
        let op = self.sample(with_replacement, 0 as f64); //padding
        let _count = op.count()?;
        op.collect()
    }

    #[track_caller]
    fn secure_take_sample(
        &self,
        with_replacement: bool,
        _num: u64,
        _seed: Option<u64>,
        tail_info: &TailCompInfo,
    ) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        let op = self.sample(with_replacement, 0 as f64); //padding
        let r = op.secure_collect(tail_info);
        r
    }

    #[track_caller]
    fn take(&self, num: usize) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>> 
    where
        Self: Sized,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(move |data| batch_encrypt(data, fe.clone()));
        let bfd = Box::new(move |data_enc| batch_decrypt(data_enc, fd.clone()));
        Ok(Text::new(vec![], Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn secure_take(&self, num: usize, tail_info: &TailCompInfo) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>> 
    where
        Self: Sized,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(move |data| batch_encrypt(data, fe.clone()));
        let bfd = Box::new(move |data_enc| batch_decrypt(data_enc, fd.clone()));
        Ok(Text::rec(tail_info, Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn union(
        &self,
        other: Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>,
    ) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
    {
        let new_op = SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>,
            other,
        ]));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn zip<S, SE, FE, FD>(
        &self,
        second: Arc<dyn OpE<Item = S, ItemE = SE>>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = (Self::Item, S), ItemE = (Self::ItemE, SE)>>
    where
        Self: Clone,
        S: Data,
        SE: Data,
        FE: SerFunc(Vec<(Self::Item, S)>) -> (Self::ItemE, SE),
        FD: SerFunc((Self::ItemE, SE)) -> Vec<(Self::Item, S)>,
    {
        let new_op = SerArc::new(Zipped::new(
            Arc::new(self.clone()) as Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>,
            second,
            fe,
            fd,
        ));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn key_by<T, F>(&self, func: F) -> SerArc<dyn OpE<Item = (T, Self::Item), ItemE = (Vec<u8>, Self::ItemE)>>
    where
        Self: Sized,
        T: Data,
        F: SerFunc(&Self::Item) -> T,
    {
        let fe = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<(T, Self::Item)>| {
            let len = v.len();
            let (vx, vy): (Vec<T>, Vec<Self::Item>) = v.into_iter().unzip();
            let ct_x = ser_encrypt(vx);
            let ct_y = (fe)(vy);
            (ct_x, ct_y)
        });

        let fd = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: (Vec<u8>, Self::ItemE)| {
            let (vx, vy) = v;
            let pt_x: Vec<T> = ser_decrypt(vx);
            let mut pt_y = (fd)(vy);
            pt_y.resize_with(pt_x.len(), Default::default); //this case occurs when a cogrouped op follows it
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });
       
        self.map(Fn!(move |k: Self::Item| -> (T, Self::Item) {
            let t = (func)(&k);
            (t, k)
        }), fe_wrapper_mp, fd_wrapper_mp)
    }

}