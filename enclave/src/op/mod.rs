use core::marker::PhantomData;
use core::panic::Location;
use std::any::{Any, TypeId};
use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, hash_map::DefaultHasher, HashMap, VecDeque};
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
use std::thread::{self, JoinHandle};
use std::untrusted::time::InstantEx;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use deepsize::DeepSizeOf;
use sgx_types::*;
use rand::{Rng, SeedableRng};

use crate::aggregator::Aggregator;
use crate::{CACHE, Fn, OP_MAP};
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::custom_thread::PThread;
use crate::dependency::{Dependency, OneToOneDependency, ShuffleDependencyTrait};
use crate::obliv_comp::{VALID_BIT, obliv_global_filter_stage1, obliv_global_filter_stage2_t, obliv_global_filter_stage3_t, obliv_global_filter_stage3_kv, obliv_agg_stage1};
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
mod filter_op;
pub use filter_op::*;
mod flatmapper_op;
pub use flatmapper_op::*;
mod fold_op;
pub use fold_op::*;
mod local_file_reader;
pub use local_file_reader::*;
mod joined_op;
pub use joined_op::*;
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

pub type ItemE = Vec<u8>;
type ResIter<T> = Box<dyn Iterator<Item = (Box<dyn Iterator<Item = T>>, Vec<bool>)>>;

pub const MAX_ENC_BL: usize = 1024;
pub const CACHE_LIMIT: usize = 4_000_000;
pub const ENABLE_CACHE_INSIDE: bool = false;
pub const MAX_THREAD: usize = 1;
pub const PARA_THRESHOLD: f64 = 3.0;
pub type Result<T> = std::result::Result<T, &'static str>;

extern "C" {
    pub fn ocall_cache_to_outside(ret_val: *mut u8,   //write successfully or not
        rdd_id: usize,
        part_id: usize,
        data_ptr: usize,
        marks_ptr: usize,
    ) -> sgx_status_t;

    pub fn ocall_cache_from_outside(data_ptr: *mut usize,  //ptr outside enclave
        rdd_id: usize,
        part_id: usize,
        marks_ptr: *mut usize,
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
pub fn ser_encrypt<T>(pt: &T) -> Vec<u8>
where
    T: ?Sized + serde::Serialize,
{
    encrypt(bincode::serialize(pt).unwrap().as_ref())
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn ser_decrypt<T>(ct: &[u8]) -> T
where
    T: serde::de::DeserializeOwned,
{
    bincode::deserialize(decrypt(ct).as_ref()).unwrap()
}

pub fn batch_encrypt<T: Data>(data: &[T], is_enc_outside: bool) -> Vec<ItemE> 
{
    if is_enc_outside {
        let acc = create_enc();
        data.chunks(MAX_ENC_BL).map(|x| ser_encrypt(x)).fold(acc, |mut acc, x| {
            crate::ALLOCATOR.set_switch(true);
            acc.push(x.clone());
            crate::ALLOCATOR.set_switch(false);
            acc
        })
    } else {
        data.chunks(MAX_ENC_BL).map(|x| ser_encrypt(x)).collect::<Vec<_>>()
    }
}

pub fn batch_decrypt<T: Data>(data_enc: &[ItemE], is_enc_outside: bool) -> Vec<T> 
{
    if is_enc_outside {
        data_enc.iter().map(|x| ser_decrypt::<Vec<T>>(&x.clone())).flatten().collect::<Vec<_>>()
    } else {
        data_enc.iter().map(|x| ser_decrypt::<Vec<T>>(x)).flatten().collect::<Vec<_>>()
    }
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

pub fn batch_encrypt_and_align_marks<T: Data>(data: &[T], marks: &[bool]) -> (Vec<ItemE>, Vec<ItemE>) 
{
    let data_enc = batch_encrypt(data, true);
    let marks_enc = if marks.is_empty() {
        let v = ser_encrypt(&Vec::<bool>::new());
        crate::ALLOCATOR.set_switch(true);
        let tmp = vec![v.clone(); data_enc.len()];  //for alignment
        crate::ALLOCATOR.set_switch(false);
        tmp
    } else {
        batch_encrypt(marks, true)
    };
    (data_enc, marks_enc)
}

#[inline(always)]
pub fn align_marks_and_batch_encrypt<T: Data>(data: Vec<T>, mut marks: Vec<bool>) -> Vec<ItemE> 
{
    let len = data.len();
    assert!(len == marks.len() || marks.is_empty());
    marks.resize(len, true);
    batch_encrypt(&data.into_iter().zip(marks.into_iter()).collect::<Vec<_>>(), true)
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

pub fn read_sorted_bucket<T: Data>(buckets_enc: &Vec<Vec<ItemE>>, outer_parallel: usize) -> (Vec<Vec<Vec<T>>>, usize) {
    let mut parts = Vec::with_capacity(buckets_enc.len());
    let mut max_len = 0;
    for bucket_enc in buckets_enc {
        let bucket = Box::new(bucket_enc.clone().into_iter().map(|bl_enc| ser_decrypt::<Vec<T>>(&bl_enc)));
        let (sub_parts, local_max_len) = compose_subpart(bucket, outer_parallel, false, |a, b| Ordering::Equal);
        //sub_parts is already sorted
        max_len = std::cmp::max(max_len, local_max_len);
        parts.push(sub_parts);
    }
    (parts, max_len)
}

pub fn compose_subpart<T, F>(data: Box<dyn Iterator<Item = Vec<T>>>, outer_parallel: usize, need_sort: bool, cmp_f: F) -> (Vec<Vec<T>>, usize) 
where
    T: Data,
    F: FnMut(&T, &T) -> Ordering + Clone,
{
    let mut sub_parts: Vec<Vec<T>> = Vec::new();
    let mut sub_part_size = 0;
    let mut max_len = 0;
    for mut bl in data {
        let block_size = bl.deep_size_of();
        let mut should_create_new = true;
        if let Some(sub_part) = sub_parts.last_mut() {
            if sub_part_size + block_size <= CACHE_LIMIT/MAX_THREAD/outer_parallel {
                sub_part.append(&mut bl);
                sub_part_size += block_size;
                should_create_new = false;
            }
        }
        if should_create_new {
            max_len = std::cmp::max(sub_parts.last().map_or(0, |sub_part| sub_part.len()), max_len);
            if need_sort && sub_parts.len() > 0 {
                sub_parts.last_mut().unwrap().sort_by(cmp_f.clone());
            }
            sub_parts.push(bl);
            sub_part_size = block_size;
        }
    }
    max_len = std::cmp::max(sub_parts.last().map_or(0, |sub_part| sub_part.len()), max_len);
    if need_sort && sub_parts.len() > 0 {
        sub_parts.last_mut().unwrap().sort_by(cmp_f);
    }
    (sub_parts, max_len)
}

pub fn split_with_interval<T, F>(mut data: Vec<T>, cache_limit: usize, cmp_f: F) -> (Vec<Vec<T>>, usize) 
where
    T: Data,
    F: FnMut(&T, &T) -> Ordering + Clone,
{
    let mut sub_parts = Vec::new();
    let mut max_len = 0;
    let mut sub_part = Vec::new();
    while !data.is_empty() {
        let b = data.len().saturating_sub(MAX_ENC_BL);
        let mut block = data.split_off(b);
        sub_part.append(&mut block);
        if sub_part.deep_size_of() > cache_limit {
            sub_part.sort_unstable_by(cmp_f.clone());
            max_len = std::cmp::max(max_len, sub_part.len());
            sub_parts.push(sub_part);
            sub_part = Vec::new();
        }
    }
    if !sub_part.is_empty() {
        sub_part.sort_unstable_by(cmp_f.clone());
        max_len = std::cmp::max(max_len, sub_part.len());
        sub_parts.push(sub_part);
    }
    (sub_parts, max_len)
}

//the results are encrypted and stay outside enclave
pub fn filter_3_agg_1<K, V>(buckets_enc: &Vec<Vec<ItemE>>, outer_parallel: usize, part_id: usize, partitioner: &Box<dyn Partitioner>, seed: u64) -> (Vec<ItemE>, Vec<Vec<ItemE>>) 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
{
    let (parts, max_len) = read_sorted_bucket::<((K, V), u64)>(buckets_enc, outer_parallel);
    let part = obliv_global_filter_stage3_kv(parts, max_len);
    //group count
    let create_combiner = Box::new(|v: i64| v);
    let merge_value = Box::new(move |(buf, v)| { buf + v });
    let merge_combiners = Box::new(move |(b1, b2)| { b1 + b2 });
    let aggregator = Arc::new(Aggregator::new(create_combiner, merge_value, merge_combiners));

    let buckets_enc = obliv_agg_stage1(
        part.iter()
            .map(|(k, _)| ((k.clone(), 1i64), 1 << VALID_BIT))
            .collect::<Vec<_>>(),
        part_id,
        true,
        &aggregator,
        partitioner,
        seed,
        outer_parallel,
    );
    let part_enc = batch_encrypt(&part, true);
    (part_enc, buckets_enc)
}

//require each vec (ItemE) is sorted
pub struct SortHelper<T, F>
where
    T: Data,
    F: FnMut(&T, &T) -> Ordering + Clone, 
{
    data: Vec<Arc<Mutex<Vec<T>>>>,
    max_len: usize,
    max_value: T,
    num_padding: Arc<AtomicUsize>,
    ascending: bool,
    dist: Option<Vec<usize>>,
    dist_use_map: Option<HashMap<usize, AtomicBool>>,
    f: F,
}

impl<T, F> SortHelper<T, F>
where
    T: Data,
    F: FnMut(&T, &T) -> Ordering + Clone, 
{
    pub fn new(data: Vec<Vec<T>>, max_len: usize, max_value: T, ascending: bool, f: F) -> Self {
        let data = {
            data
                .into_iter()
                .map(|x| Arc::new(Mutex::new(x)))
                .collect::<Vec<_>>()
        };
        SortHelper { data, max_len, max_value, num_padding: Arc::new(AtomicUsize::new(0)), ascending, dist: None, dist_use_map: None, f}
    }

    //optimize for merging sorted arrays
    pub fn new_with(data: Vec<Vec<Vec<T>>>, max_len: usize, max_value: T, ascending: bool, f: F) -> Self {
        let dist = Some(vec![0].into_iter().chain(data.iter()
            .map(|p| p.len()))
            .scan(0usize, |acc, x| {
                *acc = *acc + x;
                Some(*acc)
            }).collect::<Vec<_>>());
        // true means the array is sorted originally, never touched in the following algorithm
        let dist_use_map = dist.as_ref().map(|dist| dist.iter().map(|k| (*k, AtomicBool::new(true))).collect::<HashMap<_, _>>());
        let data = {
            data.into_iter().map(|p| p.into_iter().map(|x|Arc::new(Mutex::new(x)))).flatten().collect::<Vec<_>>()
        };
        SortHelper { data, max_len, max_value, num_padding: Arc::new(AtomicUsize::new(0)), ascending, dist, dist_use_map, f}
    }

    pub fn sort(&self) {
        let n = self.data.len();
        self.bitonic_sort_arbitrary(0, n, self.ascending);
    }

    fn bitonic_sort_arbitrary(&self, lo: usize, n: usize, dir: bool) {
        if n > 1 {
            match &self.dist {
                Some(dist) => {
                    let r_idx = dist.binary_search(&(lo+n)).unwrap();
                    let l_idx = dist.binary_search(&lo).unwrap();
                    if r_idx - l_idx > 1 {
                        let m = dist[(l_idx + r_idx)/2] - lo;
                        self.bitonic_sort_arbitrary(lo, m, !dir);
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
        fn is_inconsistent_dir<T, F>(d: &Vec<T>, dir: bool, mut f: F) -> bool 
        where
            T: Data, 
            F: FnMut(&T, &T) -> Ordering + Clone
        {
            d
                .first()
                .zip(d.last())
                .map(move |(x, y)| f(x, y).is_lt() != dir)
                .unwrap_or(false)
        }
        
        if n > 1 {
            let max_len = self.max_len;
            let num_padding = self.num_padding.clone();

            let mut is_sorted = self.dist.as_ref().map_or(false, |dist| {
                dist.binary_search(&(lo + n)).map_or(false, |idx| 
                    dist[idx - 1] == lo 
                )
            });
            is_sorted = is_sorted && self.dist_use_map.as_ref().map_or(false, |dist_use_map| {
                dist_use_map.get(&(lo+n)).unwrap().fetch_and(false, atomic::Ordering::SeqCst)
            });
            if is_sorted {
                let mut first_d = None;
                let mut last_d = None;
                for i in lo..(lo + n) {
                    if let Some(x) = self.data[i].lock().unwrap().first() {
                        first_d = Some(x.clone());
                        break;
                    }
                }
                for i in (lo..(lo + n)).rev() {
                    if let Some(x) = self.data[i].lock().unwrap().last() {
                        last_d = Some(x.clone());
                        break;
                    }
                }

                let mut f = self.f.clone();
                let (ori_dir, should_reverse) = first_d.as_ref()
                    .zip(last_d.as_ref())
                    .map(|(x, y)| {
                        let ori_dir = f(x, y).is_lt();
                        (ori_dir, ori_dir != dir)
                    }).unwrap_or((true, false));

                //originally ascending
                if ori_dir {
                    let mut d_i: Vec<T> = Vec::new();
                    let mut cnt = lo;
                    for i in lo..(lo + n) {
                        d_i.append(&mut *self.data[i].lock().unwrap());
                        if d_i.len() >= max_len {
                            let mut r = d_i.split_off(max_len);
                            std::mem::swap(&mut r, &mut d_i);
                            if should_reverse {
                                r.reverse();
                            }
                            *self.data[cnt].lock().unwrap() = r;
                            cnt += 1;
                        }
                    }
                    let mut add_num_padding = 0;
                    while cnt < lo + n {
                        add_num_padding += max_len - d_i.len();
                        d_i.resize(max_len, self.max_value.clone());
                        if should_reverse {
                            d_i.reverse();
                        }
                        *self.data[cnt].lock().unwrap() = d_i;
                        d_i = Vec::new();
                        cnt += 1;
                    }
                    num_padding.fetch_add(add_num_padding, atomic::Ordering::SeqCst);
                } else {  //originally desending
                    let mut d_i: Vec<T> = vec![Default::default(); max_len];
                    let mut cur_len = 0;
                    let mut cnt = lo + n - 1;
                    for i in (lo..(lo + n)).rev() {
                        let v = &mut *self.data[i].lock().unwrap();
                        let v_len = v.len();
                        if v_len < max_len - cur_len {
                            (&mut d_i[max_len-cur_len-v_len..max_len-cur_len]).clone_from_slice(v);
                            cur_len += v_len;
                        } else {
                            let r = v.split_off(v_len - (max_len - cur_len));
                            (&mut d_i[..max_len-cur_len]).clone_from_slice(&r);
                            if should_reverse {
                                d_i.reverse();
                            }
                            *self.data[cnt].lock().unwrap() = d_i;
                            cnt = cnt.wrapping_sub(1);
                            cur_len = v.len();
                            d_i = vec![Default::default(); max_len];
                            (&mut d_i[max_len-cur_len..max_len]).clone_from_slice(v);
                        }
                    }
                    let mut add_num_padding = 0;
                    while cnt >= lo && cnt != usize::MAX { 
                        add_num_padding += max_len - cur_len;
                        for elem in &mut d_i[..max_len-cur_len] {
                            *elem = self.max_value.clone();
                        }
                        if should_reverse {
                            d_i.reverse();
                        }
                        *self.data[cnt].lock().unwrap() = d_i;
                        cnt = cnt.wrapping_sub(1);
                        cur_len = 0;
                        d_i = vec![Default::default(); max_len];
                    }
                    num_padding.fetch_add(add_num_padding, atomic::Ordering::SeqCst);
                }
                if should_reverse {
                    for i in lo..(lo + n / 2) {
                        let l = 2 * lo + n - 1 - i;
                        std::mem::swap(&mut *self.data[i].lock().unwrap(), &mut *self.data[l].lock().unwrap());
                    }
                }
            } else {
                let m = n.next_power_of_two() >> 1;
                for i in lo..(lo + n - m) {
                    let l = i + m;
                    //merge
                    let d_i: Vec<T> = std::mem::take(&mut *self.data[i].lock().unwrap());
                    let d_l: Vec<T> = std::mem::take(&mut *self.data[l].lock().unwrap());
    
                    let real_len = d_i.len() + d_l.len();
                    let dummy_len = max_len * 2 - real_len;
                    if real_len < max_len * 2 {
                        num_padding.fetch_add(dummy_len, atomic::Ordering::SeqCst);
                    }
    
                    let mut new_d_i = Vec::with_capacity(max_len);
                    let mut new_d_l = Vec::with_capacity(max_len);
                    //desending, append dummy elements first
                    if !dir {
                        new_d_i.append(&mut vec![self.max_value.clone(); std::cmp::min(dummy_len, max_len)]);
                        new_d_l.append(&mut vec![self.max_value.clone(); dummy_len.saturating_sub(max_len)]);
                    }
    
                    let mut iter_i = if is_inconsistent_dir(&d_i, dir, self.f.clone()) {
                        Box::new(d_i.into_iter().rev()) as Box<dyn Iterator<Item = T>>
                    } else {
                        Box::new(d_i.into_iter()) as Box<dyn Iterator<Item = T>>
                    };
    
                    let mut iter_l = if is_inconsistent_dir(&d_l, dir, self.f.clone()) {
                        Box::new(d_l.into_iter().rev()) as Box<dyn Iterator<Item = T>>
                    } else {
                        Box::new(d_l.into_iter()) as Box<dyn Iterator<Item = T>>
                    };
    
                    let mut cur_i = iter_i.next();
                    let mut cur_l = iter_l.next();
    
                    let mut f = self.f.clone();
                    while cur_i.is_some() || cur_l.is_some() {
                        let should_puti = cur_l.is_none()
                            || cur_i.is_some()
                                &&  f(cur_i.as_ref().unwrap(), cur_l.as_ref().unwrap()).is_lt() == dir;
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
                        new_d_i.resize(max_len, self.max_value.clone());
                        new_d_l.resize(max_len, self.max_value.clone());
                    }

                    *self.data[i].lock().unwrap() = new_d_i;
                    *self.data[l].lock().unwrap() = new_d_l;
                }
    
                self.bitonic_merge_arbitrary(lo, m, dir);
                self.bitonic_merge_arbitrary(lo + m, n - m, dir);
            }
        }
    }

    pub fn take(&mut self) -> (Vec<T>, usize) {
        if self.data.is_empty() {
            return (Vec::new(), 0);
        }
        let num_padding = self.num_padding.load(atomic::Ordering::SeqCst);
        let num_real_elem = self.data.len() * self.max_len - num_padding;
        let num_real_sub_part = self.data.len() - num_padding / self.max_len;
        let num_padding_remaining = num_real_sub_part * self.max_len - num_real_elem;

        let mut data = std::mem::take(&mut self.data);
        //remove dummy elements
        data.truncate(num_real_sub_part);
        if let Some(last) = data.last_mut() {
            let mut last = last.lock().unwrap();
            assert!(last.get(self.max_len - num_padding_remaining).map_or(true, |x| (self.f)(x, &self.max_value).is_eq()));
            last.truncate(self.max_len - num_padding_remaining);
        }
        let res = data.into_iter().flat_map(|x| Arc::try_unwrap(x).unwrap().into_inner().unwrap()).collect::<Vec<_>>();
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
    /// 20-24 for shuffle_op, 20-27 for join_op , 12/13/14 for global filter
    pub fn dep_type(&self) -> u8 {
        self.is_shuffle
    }

}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Input {
    pub data: usize,
    pub marks: usize,
    parallel_num: usize,
}

impl Input {
    pub fn new<T: Data, A: Data>(data: &T,
        marks: &A, 
        parallel_num: usize,
    ) -> Self {
        let data = data as *const T as usize;
        let marks = marks as *const A as usize;
        Input {
            data,
            marks,
            parallel_num,
        }
    }

    pub fn get_enc_data<T>(&self) -> &T {
        unsafe { (self.data as *const T).as_ref() }.unwrap()
    }

    pub fn get_enc_marks<A>(&self) -> &A {
        unsafe { (self.marks as *const A).as_ref() }.unwrap()
    }

    pub fn get_parallel(&self) -> usize {
        self.parallel_num
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Text<T, TE>
{
    data: Option<T>,
    data_enc: Option<TE>,
    id: u64,
}

impl<T, TE> Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    #[track_caller]
    pub fn new(data: Option<T>, data_enc: Option<TE>) -> Self {
        let id = get_text_loc_id();
        Text {
            data,
            data_enc,
            id,
        }
    }

    pub fn get_ct(&self) -> TE {
        self.data_enc.clone().unwrap()
    }

    pub fn get_ct_ref(&self) -> &TE {
        self.data_enc.as_ref().unwrap()
    }

    pub fn get_ct_mut(&mut self) -> &mut TE {
        self.data_enc.as_mut().unwrap()
    }

    pub fn update_from_tail_info(&mut self, tail_info: &TailCompInfo) {
        let text = bincode::deserialize(&tail_info.get(self.id).unwrap()).unwrap();
        *self = text;
    }
}

impl<T> Text<T, Vec<u8>>
where
    T: Data,
{
    #[track_caller]
    pub fn rec(tail_info: &TailCompInfo) -> Self {
        let id = get_text_loc_id();
        let mut text: Text<T, Vec<u8>> = match tail_info.get(id) {
            Some(bytes) => {
                bincode::deserialize(&bytes).unwrap()
            },
            None => {
                Default::default()
            },
        };
        if text.data.is_none() && text.data_enc.is_some() {
            text.data = Some(ser_decrypt(text.data_enc.as_ref().unwrap()));
            text.data_enc = None;
        }
        text
    }

    //currently it is only used for the case
    //where the captured variables are encrypted
    pub fn get_pt(&self) -> T {
        if self.data.is_some() {
            self.data.clone().unwrap()
        } else if self.data_enc.is_some() {
            ser_decrypt(self.data_enc.as_ref().unwrap())
        } else {
            Default::default()
        }
    }
}

impl<T> Text<Vec<T>, Vec<Vec<u8>>>
where
    T: Data,
{
    #[track_caller]
    pub fn rec(tail_info: &TailCompInfo) -> Self {
        let id = get_text_loc_id();
        let mut text: Text<Vec<T>, Vec<Vec<u8>>> = match tail_info.get(id) {
            Some(bytes) => {
                bincode::deserialize(&bytes).unwrap()
            },
            None => {
                Default::default()
            },
        };
        if text.data.is_none() && text.data_enc.is_some() {
            text.data = Some(text.data_enc.as_ref()
                .unwrap()
                .iter()
                .map(|x| ser_decrypt::<Vec<T>>(x).into_iter())
                .flatten()
                .collect::<Vec<_>>()
            );
            text.data_enc = None;
        }
        text
    }

    //currently it is only used for the case
    //where the captured variables are encrypted
    pub fn get_pt(&self) -> Vec<T> {
        if self.data.is_some() {
            self.data.clone().unwrap()
        } else if self.data_enc.is_some() {
            self.data_enc
                .as_ref()
                .unwrap()
                .iter()
                .map(|x| ser_decrypt::<Vec<T>>(x).into_iter())
                .flatten()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        }
    }
}

impl<T, TE> Deref for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.data.as_ref().unwrap()
    }
}

impl<T, TE> DerefMut for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.as_mut().unwrap()
    }
}

#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct TailCompInfo {
    m: HashMap<u64, Vec<u8>>,
}

impl TailCompInfo {
    pub fn new() -> Self {
        TailCompInfo { m: HashMap::new() }
    }

    pub fn insert<T, TE>(&mut self, text: &Text<T, TE>)
    where
        T: Data,
        TE: Data,
    {
        self.m.insert(text.id, bincode::serialize(text).unwrap());
    }

    pub fn remove(&mut self, id: u64) -> Vec<u8> {
        self.m.remove(&id).unwrap()
    }

    pub fn get(&self, id: u64) -> Option<Vec<u8>> {
        match self.m.get(&id) {
            Some(ser) => Some(ser.clone()),
            None => None,
        }
    }

    pub fn clear(&mut self) {
        self.m.clear();
    }
}

#[derive(Clone)]
pub struct OpCache {
    //temperarily save the point of data, which is ready to sent out
    // <(cached_rdd_id, part_id), (data ptr, marks ptr)>
    out_map: Arc<RwLock<HashMap<(usize, usize), (usize, usize)>>>,
}

impl OpCache{
    pub fn new() -> Self {
        OpCache {
            out_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn send(&self, key: (usize, usize)) {
        if let Some((ct_ptr, ct_mk_ptr)) = self.out_map.write().unwrap().remove(&key) {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, key.0, key.1, ct_ptr, ct_mk_ptr); }
            //TODO: Handle the case res != 0
        }
    }

    pub fn clear(&self) {
        let out_map = std::mem::take(&mut *self.out_map.write().unwrap());
        //normally the map is empty, and it should not enter the following loop
        for ((rdd_id, part_id), (ct_ptr, ct_mk_ptr)) in out_map.into_iter() {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, rdd_id, part_id, ct_ptr, ct_mk_ptr); }
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

#[derive(Clone, Debug)]
pub struct NextOpId {
    tid: u64,
    pub stage_id: usize,
    rdd_ids: Vec<usize>,
    op_ids: Vec<OpId>,
    part_ids: Vec<usize>,
    cur_idx: usize,
    cache_meta: CacheMeta,
    captured_vars: HashMap<usize, Vec<Vec<u8>>>,
    is_shuffle: bool,
    pub para_range: Option<(usize, usize)>,
    //if the decryption step and the narrow processing parallelizable 
    pub is_step_para: (bool, bool, bool),
    //used to decide is_step_para
    pub sample_len: usize,
    //used to decide whether the global filter can be omitted
    //record if (group by/join/filter appears, aggregate/fold/reduce/count don't appear)
    pub should_filter: (bool, bool),
}

impl NextOpId {
    pub fn new(tid: u64,
        stage_id: usize,
        rdd_ids: Vec<usize>, 
        op_ids: Vec<OpId>, 
        part_ids: Vec<usize>, 
        cache_meta: CacheMeta, 
        captured_vars: HashMap<usize, Vec<Vec<u8>>>, 
        dep_info: &DepInfo,
    ) -> Self {
        let is_shuffle = dep_info.is_shuffle == 1;
        NextOpId {
            tid,
            stage_id,
            rdd_ids,
            op_ids,
            part_ids,
            cur_idx: 0,
            cache_meta,
            captured_vars,
            is_shuffle,
            para_range: None,
            is_step_para: (true, true, true),
            sample_len: 0,
            should_filter: (false, true),
        }
    }

    pub fn get_cur_rdd_id(&self) -> usize {
        self.rdd_ids[self.cur_idx]
    }

    fn get_cur_op_id(&self) -> OpId {
        self.op_ids[self.cur_idx]
    }
    
    pub fn get_part_id(&self) -> usize {
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
    pub fn make_op<T>(self: &Arc<Self>, num_splits: usize) -> SerArc<dyn Op<Item = T>> 
    where
        T: Data,
    {
        let new_op = SerArc::new(ParallelCollection::new(self.clone(), num_splits));
        if !self.get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    /// Load from a distributed source and turns it into a parallel collection.
    #[track_caller]
    pub fn read_source<C, I: Data, O: Data>(
        self: &Arc<Self>,
        config: C,
        func: Option< Box<dyn Func(I) -> O >>,
        sec_func: Option< Box<dyn Func(I) -> Vec<ItemE> >>,
    ) -> SerArc<dyn Op<Item = O>>
    where
        C: ReaderConfiguration<I>,
    {
        config.make_reader(self.clone(), func, sec_func)
    }

    #[track_caller]
    pub fn union<T: Data>(rdds: &[Arc<dyn Op<Item = T>>]) -> impl Op<Item = T> {
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
    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo);
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
    fn iterator_start(&self, call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8);
    fn randomize_in_place(&self, input: *mut u8, seed: Option<u64>, num: u64) -> *mut u8 {
        unreachable!()
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        unreachable!()
    }
    fn etake(&self, input: *mut u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        unreachable!()
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

impl<I: Op + ?Sized> OpBase for SerArc<I> {
    //need to avoid freeing the encrypted data for subsequent "clone_enc_data_out"
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).build_enc_data_sketch(p_buf, p_data_enc, dep_info);
    }
    //need to free the encrypted data. But how to deal with task failure?
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).clone_enc_data_out(p_out, p_data_enc, dep_info);
    }
    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        (**self).call_free_res_enc(data, marks, is_enc, dep_info);
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
    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        (**self).get_op_base().iterator_start(call_seq, input, dep_info)
    }
    fn randomize_in_place(&self, input: *mut u8, seed: Option<u64>, num: u64) -> *mut u8 {
        (**self).randomize_in_place(input, seed, num)
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        (**self).set_sampler(with_replacement, fraction)
    }
    fn etake(&self, input: *mut u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        (**self).etake(input, should_take, have_take)
    }
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        (**self).clone().__to_arc_op(id)
    }
}

impl<I: Op + ?Sized> Op for SerArc<I> {
    type Item = I::Item;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        (**self).get_op()
    } 
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        (**self).get_op_base()
    }
    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<Self::Item>>, Vec<Vec<bool>>)>>> {
        (**self).get_cache_space()
    }
    fn compute_start(&self, call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        (**self).compute_start(call_seq, input, dep_info)
    }
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        (**self).compute(call_seq, input)
    }
    fn cache(&self, data: Vec<Self::Item>) {
        (**self).cache(data);
    }
}


pub trait Op: OpBase + 'static {
    type Item: Data;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_op_base(&self) -> Arc<dyn OpBase>;
    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<Self::Item>>, Vec<Vec<bool>>)>>> {
        unreachable!()
    }
    fn compute_start(&self, call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        match dep_info.dep_type() {
            0 => { 
                self.narrow(call_seq, input, true)
            },
            1 | 11 => {
                self.shuffle(call_seq, input, dep_info)
            },
            12 | 13 | 14 => {
                self.remove_dummy(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }
    //the default is for narrow begin
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let marks_ptr = input.marks;

        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache || (call_seq.get_cur_rdd_id() == call_seq.cache_meta.cached_rdd_id && data_ptr == 0) {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
        }
        
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let marks_enc = input.get_enc_marks::<Vec<ItemE>>();
        let res_iter = self.parallel_control(call_seq, data_enc, marks_enc);
        
        if need_cache {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }
    fn cache(&self, data: Vec<Self::Item>) {
        ()
    }
    fn cache_from_outside(&self, key: (usize, usize)) -> Option<(&'static Vec<ItemE>, &'static Vec<ItemE>)> {
        let mut ct_ptr: usize = 0;
        let mut ct_mk_ptr: usize = 0;
        let sgx_status = unsafe { 
            ocall_cache_from_outside(&mut ct_ptr, key.0, key.1, &mut ct_mk_ptr)
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] OCALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        if ct_ptr == 0 {
            return None;
        }
        /*
        let ct_ = unsafe {
            Box::from_raw(ptr as *mut u8 as *mut Vec<ItemE>)
        };
        let ct = ct_.clone();
        forget(ct_);
        batch_decrypt(*ct)
        */
        Some(unsafe {
            ((ct_ptr as *const u8 as *const Vec<ItemE>).as_ref().unwrap(),
            (ct_mk_ptr as *const u8 as *const Vec<ItemE>).as_ref().unwrap())
        })
    }

    fn cache_to_outside(&self, key: (usize, usize), value: &Vec<Self::Item>, marks: &Vec<bool>) -> Option<PThread> {
        //let handle = unsafe {
        //    PThread::new(Box::new(move || {
        let (ct, ct_mk) = batch_encrypt_and_align_marks(value, marks);
        //println!("finish encryption, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        let mut out_map = CACHE.out_map.write().unwrap();
        let ct_pair = match out_map.remove(&key) {
            Some((ct_ptr, ct_mk_ptr)) => {
                crate::ALLOCATOR.set_switch(true);
                let mut acc_ct = *unsafe { Box::from_raw(ct_ptr as *mut Vec<ItemE>) };
                let mut acc_ct_mk = *unsafe { Box::from_raw(ct_mk_ptr as *mut Vec<ItemE>) };
                combine_enc(&mut acc_ct, ct);
                combine_enc(&mut acc_ct_mk, ct_mk);
                crate::ALLOCATOR.set_switch(false);
                (to_ptr(acc_ct), to_ptr(acc_ct_mk)) 
            },
            None => (to_ptr(ct), to_ptr(ct_mk)), 
        };
        out_map.insert(key, (ct_pair.0 as usize, ct_pair.1 as usize));
        //println!("finish copy out, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        //    }))
        //}.unwrap();
        //Some(handle)
        None
    }

    fn get_and_remove_cached_data(&self, call_seq: &mut NextOpId) -> ResIter<Self::Item> {
        let key = call_seq.get_cached_doublet();
        //cache inside enclave
        let have_cache_inside = {
            let cache_space = self.get_cache_space();
            let l = cache_space.lock().unwrap();
            l.contains_key(&key)
        };
        
        if have_cache_inside {
            //another version of parallel_control, there are no decryption step
            match std::mem::take(&mut call_seq.para_range) {
                Some((b, e)) => {
                    let r = e.saturating_sub(b);
                    if r == 0 {
                        Box::new(vec![].into_iter()) 
                    } else {
                        let cache_space = self.get_cache_space();
                        let mut l = cache_space.lock().unwrap();
                        let (data, marks, is_empty) = {
                            let v = l.get_mut(&key).unwrap();
                            let data = v.0.split_off(v.0.len() - r);
                            let marks = v.1.split_off(v.1.len() - r);
                            (data, marks, v.0.is_empty())
                        };
                        if is_empty {
                            println!("remove cache key");
                            l.remove(&key).unwrap();
                        }
                        Box::new(data.into_iter().zip(marks.into_iter()).map(|(bl, blmarks)| (Box::new(bl.into_iter()) as Box<dyn Iterator<Item = _>>, blmarks)))
                    }
                },
                None => {
                    let (sample_data, sample_marks) = {
                        let cache_space = self.get_cache_space();
                        let mut cache_space = cache_space.lock().unwrap();
                        let entry = cache_space.get_mut(&key).unwrap();
                        call_seq.para_range = Some((0, entry.0.len().saturating_sub(1)));
                        (entry.0.pop().unwrap_or(Vec::new()), entry.1.pop().unwrap_or(Vec::new()))
                    };
                    call_seq.sample_len = sample_data.len();
                    //profile begin
                    crate::ALLOCATOR.reset_alloc_cnt();
                    Box::new(vec![(sample_data, sample_marks)].into_iter().map(move |(bl, blmarks)| {
                        (Box::new(bl.into_iter()) as Box<dyn Iterator<Item = _>>, blmarks)
                    }))
                }
            }
        } else {
            let (data_enc, marks_enc) = self.cache_from_outside(key).unwrap();
            self.parallel_control(call_seq, data_enc, marks_enc)
        }
    }

    fn set_cached_data(&self, call_seq: &NextOpId, res_iter: ResIter<Self::Item>, is_caching_final_rdd: bool) -> ResIter<Self::Item> {
        let ope = self.get_op();
        let op_id = self.get_op_id();
        let key = call_seq.get_caching_doublet();
        let cache_space = self.get_cache_space();

        Box::new(res_iter.map(move |(bl_iter, blmarks)| {
            //cache outside enclave
            if is_caching_final_rdd {
                (bl_iter, blmarks)
            } else {
                let bl = bl_iter.collect::<Vec<_>>();
                //cache inside enclave
                if ENABLE_CACHE_INSIDE {
                    let mut cache_space = cache_space.lock().unwrap();
                    let (data, marks) = cache_space.entry(key).or_insert((Vec::new(), Vec::new()));
                    data.push(bl.clone());
                    marks.push(blmarks.clone());
                } 
                //it should be always cached outside for inter-machine communcation
                let _handle = ope.cache_to_outside(key, &bl, &blmarks);
                (Box::new(bl.into_iter()) as Box<dyn Iterator<Item = _>>, blmarks)
            }
        }))
    }

    fn step0_of_clone(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match dep_info.dep_type() {
            0 | 2 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            }, 
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.send_sketch(&mut buf, p_data_enc);
            },
            3 | 4 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
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
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }, 
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.send_enc_data(p_out, p_data_enc);
            },
            3 | 4 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }
            _ => panic!("invalid is_shuffle"),
        }  
    }

    fn free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool) {
        if is_enc {
            crate::ALLOCATOR.set_switch(true);
            let data = unsafe { Box::from_raw(data as *mut Vec<ItemE>) };
            drop(data);
            if 0 != marks as usize {
                let marks = unsafe { Box::from_raw(marks as *mut Vec<ItemE>) };
                drop(marks);
            }
            crate::ALLOCATOR.set_switch(false);
        } else {
            let _data = unsafe { Box::from_raw(data as *mut Vec<Self::Item>) };
            if 0 != marks as usize {
                let _marks = unsafe { Box::from_raw(marks as *mut Vec<bool>) };
            }
        }
    }

    //note that data_enc is outside enclave
    fn parallel_control(&self, call_seq: &mut NextOpId, data_enc: &Vec<ItemE>, marks_enc: &Vec<ItemE>) -> ResIter<Self::Item> {
        assert!(data_enc.len() == marks_enc.len() || marks_enc.is_empty());
        match std::mem::take(&mut call_seq.para_range) {
            Some((b, e)) => {
                let mut data = if let Some(data_enc) = data_enc.get(b..e) {
                    data_enc.iter().map(|x| ser_decrypt::<Vec<Self::Item>>(&x.clone())).collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let mut marks = if marks_enc.is_empty() {
                    vec![Vec::new(); e.saturating_sub(b)]
                } else {
                    marks_enc[b..e].iter().map(|x| ser_decrypt::<Vec<bool>>(&x.clone())).collect::<Vec<_>>()
                };
                if call_seq.is_step_para.0 ^ call_seq.is_step_para.1 {
                    
                    let key = (call_seq.get_cur_rdd_id(), call_seq.get_part_id());
                    //originally it cannot happen that call_seq.get_caching_doublet() == call_seq.get_cached_doublet()
                    //however, in parallel processing, since we manually set cached key for special use, the situation can occur 
                    if key == call_seq.get_caching_doublet() {
                        for (bl, blmarks) in data.iter().zip(marks.iter()) {
                            let _handle = self.cache_to_outside(key, bl, blmarks);
                        }
                    }
                    //cache the data inside enclave for parallel processing
                    let cache_space = self.get_cache_space();
                    let mut cache_space = cache_space.lock().unwrap();
                    let entry = cache_space.entry(key).or_insert((Vec::new(), Vec::new()));
                    entry.0.append(&mut data);
                    entry.1.append(&mut marks);
                    Box::new(Vec::new().into_iter())
                } else {
                    Box::new(data.into_iter().zip(marks.into_iter()).map(|(bl, blmarks)| (Box::new(bl.into_iter()) as Box<dyn Iterator<Item = _>>, blmarks)))
                }
            },
            None => {
                //for profile
                crate::ALLOCATOR.reset_alloc_cnt();
                let data = if data_enc.is_empty() {
                    Vec::new()
                } else {
                    ser_decrypt::<Vec<Self::Item>>(&data_enc[0].clone())
                };
                let marks = if marks_enc.is_empty() {
                    Vec::new()
                } else {
                    ser_decrypt::<Vec<bool>>(&marks_enc[0].clone())
                };

                let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
                call_seq.sample_len = data.len();
                let alloc_cnt_ratio = alloc_cnt as f64/(call_seq.sample_len as f64);
                println!("for decryption, alloc_cnt per len = {:?}", alloc_cnt_ratio);
                call_seq.is_step_para.0 = alloc_cnt_ratio < PARA_THRESHOLD;
                call_seq.para_range = Some((1, data_enc.len()));

                //profile begin
                crate::ALLOCATOR.reset_alloc_cnt();
                Box::new(vec![(data, marks)].into_iter().map(move |(bl, blmarks)| {
                    (Box::new(bl.into_iter()) as Box<dyn Iterator<Item = _>>, blmarks)
                }))
            }
        }
    }

    fn spawn_enc_thread(&self, should_filter: bool, mut res: Vec<(Vec<Self::Item>, Vec<bool>)>, handlers: &mut Vec<JoinHandle<(Vec<ItemE>, Vec<ItemE>)>>) {
        let mut remaining = res.len();
        let r = remaining.saturating_sub(1) / MAX_THREAD + 1;
        for _ in 0..MAX_THREAD {
            let res_local = res.split_off(res.len() - std::cmp::min(r, remaining));
            remaining = remaining.saturating_sub(r);
            let handler = thread::Builder::new()
                .spawn(move || {
                    let mut acc_data = create_enc();
                    let mut acc_marks = create_enc();
                    for (bl, blmarks) in res_local {
                        if should_filter {
                            let bl_enc = align_marks_and_batch_encrypt(bl, blmarks);
                            combine_enc(&mut acc_data, bl_enc);
                        } else {
                            let (bl_enc, blmarks_enc) = batch_encrypt_and_align_marks(&bl, &blmarks);
                            combine_enc(&mut acc_data, bl_enc);
                            combine_enc(&mut acc_marks, blmarks_enc);
                        }
                    }
                    (acc_data, acc_marks)
                }).unwrap();
            handlers.push(handler);
        }
    }

    fn spawn_dec_nar_thread(&self, call_seq: &NextOpId, input: Input, handlers: &mut Vec<JoinHandle<Vec<(Vec<Self::Item>, Vec<bool>)>>>, only_dec: bool) {
        let (s, len) = call_seq.para_range.as_ref().unwrap();
        let r = (len.saturating_sub(*s)).saturating_sub(1) / MAX_THREAD + 1;
        let mut b = *s;
        let mut e = std::cmp::min(b + r, *len);
        for _ in 0..MAX_THREAD {
            let op = self.get_op();
            let mut call_seq = call_seq.clone();
            call_seq.para_range = Some((b, e));
            b = e;
            e = std::cmp::min(b + r, *len);
            let handler = thread::Builder::new()
                .spawn(move || {
                    let res_local = op.compute(&mut call_seq, input).map(|(bl_iter, blmarks)| (bl_iter.collect::<Vec<_>>(), blmarks)).collect::<Vec<_>>();
                    if only_dec {
                        assert!(res_local.into_iter().map(|(x, _)| x).flatten().collect::<Vec<_>>().is_empty());
                        Vec::new()
                    } else {
                        res_local
                    }
                }).unwrap();
            handlers.push(handler);
        }
    }

    fn spawn_dec_nar_enc_thread(&self, should_filter: bool, call_seq: &NextOpId, input: Input, handlers: &mut Vec<JoinHandle<(Vec<ItemE>, Vec<ItemE>)>>, only_dec: bool) {
        let (s, len) = call_seq.para_range.as_ref().unwrap();
        let r = (len.saturating_sub(*s)).saturating_sub(1) / MAX_THREAD + 1;
        let mut b = *s;
        let mut e = std::cmp::min(b + r, *len);
        for _ in 0..MAX_THREAD {
            let op = self.get_op();
            let mut call_seq = call_seq.clone();
            call_seq.para_range = Some((b, e));
            b = e;
            e = std::cmp::min(b + r, *len);
            let handler = thread::Builder::new()
                .spawn(move || {
                    let res_local = op.compute(&mut call_seq, input).map(|(bl_iter, blmarks)| (bl_iter.collect::<Vec<_>>(), blmarks)).collect::<Vec<_>>();
                    if only_dec {
                        assert!(res_local.into_iter().map(|(x, _)| x).flatten().collect::<Vec<_>>().is_empty());
                        (Vec::new(), Vec::new())
                    } else {
                        let mut acc_data = create_enc();
                        let mut acc_marks = create_enc();
                        for (bl, blmarks) in res_local {
                            if should_filter {
                                let bl_enc = align_marks_and_batch_encrypt(bl, blmarks);
                                combine_enc(&mut acc_data, bl_enc);
                            } else {
                                let (bl_enc, blmarks_enc) = batch_encrypt_and_align_marks(&bl, &blmarks); 
                                combine_enc(&mut acc_data, bl_enc);
                                combine_enc(&mut acc_marks, blmarks_enc);
                            }
                        }
                        (acc_data, acc_marks)
                    }
                }).unwrap();
            handlers.push(handler);
        }
    }

    fn narrow(&self, mut call_seq: NextOpId, mut input: Input, need_enc: bool) -> (*mut u8, *mut u8) {
        let mut acc_data = create_enc();
        let mut acc_marks = create_enc();
        //run the first block for profiling
        let mut res = Vec::new();
        let should_filter = {
            let mut call_seq_sample = call_seq.clone();
            assert!(call_seq_sample.para_range.is_none());
            let (sample_data_iter, sample_marks) = self.compute(&mut call_seq_sample, input).collect::<Vec<_>>().remove(0);
            let sample_data = sample_data_iter.collect::<Vec<_>>();
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = alloc_cnt as f64/(call_seq_sample.sample_len as f64);
            println!("for narrow processing, alloc_cnt per len = {:?}", alloc_cnt_ratio);
            call_seq.para_range = call_seq_sample.para_range;
            call_seq.is_step_para.0 = call_seq_sample.is_step_para.0;
            call_seq.is_step_para.1 = alloc_cnt_ratio < PARA_THRESHOLD;
            call_seq.should_filter = call_seq_sample.should_filter;
            let should_filter = call_seq.should_filter.0 && call_seq.should_filter.1;
            //narrow specific
            if need_enc && should_filter {
                println!("should filter");
                let bl_enc = align_marks_and_batch_encrypt(sample_data, sample_marks);
                combine_enc(&mut acc_data, bl_enc);
            } else if need_enc {
                println!("do not need filter");
                let (bl_enc, blmarks_enc) = batch_encrypt_and_align_marks(&sample_data, &sample_marks);
                combine_enc(&mut acc_data, bl_enc);
                combine_enc(&mut acc_marks, blmarks_enc);
            } else {
                res.push((sample_data, sample_marks));
            }
            should_filter
        };

        let mut handlers_pt =  Vec::with_capacity(MAX_THREAD);
        let mut handlers_ct = Vec::with_capacity(MAX_THREAD);
        if !call_seq.is_step_para.0 && !call_seq.is_step_para.1 {
            let mut res_bl = self.compute(&mut call_seq, input).map(|(bl_iter, blmarks)| (bl_iter.collect::<Vec<_>>(), blmarks)).collect::<Vec<_>>();
            res.append(&mut res_bl);
            if need_enc {
                self.spawn_enc_thread(should_filter, res, &mut handlers_ct);
                res = Vec::new();
            }
        } else if call_seq.is_step_para.0 ^ call_seq.is_step_para.1 {
            //for cache inside, range begin = 0, and for cache outside or no cache, range begin = 1;
            let (s, len) = call_seq.para_range.as_ref().unwrap();
            if *s == 1 {
                //decryption needed
                if !call_seq.is_step_para.0 {
                    let mut call_seq = call_seq.clone();
                    assert!(self.compute(&mut call_seq, input).map(|(x, _)| x).flatten().collect::<Vec<_>>().is_empty());
                } else {
                    if need_enc {
                        self.spawn_dec_nar_enc_thread(should_filter, &call_seq, input, &mut handlers_ct, true); 
                    } else {
                        self.spawn_dec_nar_thread(&call_seq, input, &mut handlers_pt, true); 
                    }
                }
                //set cached key
                call_seq.cache_meta.cached_rdd_id = *call_seq.rdd_ids.last().unwrap();
                call_seq.cache_meta.cached_part_id = *call_seq.part_ids.last().unwrap();
                input.data = 0;
            } 
            {
                //narrow
                if !call_seq.is_step_para.1 {
                    for handler in handlers_ct {
                        assert!(handler.join().unwrap().0.is_empty());
                    }
                    for handler in handlers_pt {
                        handler.join().unwrap();
                    }
                    handlers_ct = Vec::with_capacity(MAX_THREAD);
                    handlers_pt = Vec::with_capacity(MAX_THREAD);
                    let mut call_seq = call_seq.clone();
                    res.append(&mut self.compute(&mut call_seq, input).map(|(bl_iter, blmarks)| (bl_iter.collect::<Vec<_>>(), blmarks)).collect::<Vec<_>>());
                    if need_enc {
                        self.spawn_enc_thread(should_filter, res, &mut handlers_ct);
                        res = Vec::new();
                    }
                } else {
                    if need_enc {
                        self.spawn_dec_nar_enc_thread(should_filter, &call_seq, input, &mut handlers_ct, false);
                    } else {
                        self.spawn_dec_nar_thread(&call_seq, input, &mut handlers_pt, false);
                    }
                }
            }
        } else {
            if need_enc {
                self.spawn_dec_nar_enc_thread(should_filter, &call_seq, input, &mut handlers_ct, false);
            } else {
                self.spawn_dec_nar_thread(&call_seq, input, &mut handlers_pt, false);
            }
        }
        for handler in handlers_ct {
            let (bl_enc, blmarks_enc) = handler.join().unwrap();
            combine_enc(&mut acc_data, bl_enc);
            combine_enc(&mut acc_marks, blmarks_enc);
        }
        for handler in handlers_pt {
            res.append(&mut handler.join().unwrap());
        }
        //cache to outside
        let key = call_seq.get_caching_doublet();
        CACHE.send(key);
        if need_enc && should_filter {
            assert!(acc_marks.is_empty());
            crate::ALLOCATOR.set_switch(true);
            drop(acc_marks);
            crate::ALLOCATOR.set_switch(false);
            if acc_data.is_empty() {
                merge_enc(&mut acc_data, &Vec::new())  //fix the synchronization issue, otherwise empty partition will skip the global filter
            }
            (to_ptr(acc_data), 0usize as *mut u8)
        } else if need_enc {
            (to_ptr(acc_data), to_ptr(acc_marks))
        } else {
            assert!(acc_data.is_empty() && acc_marks.is_empty());
            crate::ALLOCATOR.set_switch(true);
            drop(acc_data);
            drop(acc_marks);
            crate::ALLOCATOR.set_switch(false);
            (Box::into_raw(Box::new(res)) as *mut u8, 0usize as *mut u8)
        }
    } 

    fn shuffle(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
        let tid = call_seq.tid;
        let now = Instant::now();

        let result_ptr = if dep_info.dep_type() == 1 {
            let opb = self.get_op_base();
            shuf_dep.do_shuffle_task(tid, opb, call_seq, input)
        } else {
            shuf_dep.do_shuffle_task_rem(tid, call_seq, input)
        };

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("tid: {:?}, shuffle write: {:?}s", tid, dur);
        (result_ptr, 0usize as *mut u8)
    }

    fn remove_dummy(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        //since the invalid ones will be filtered out, and they are sorted to the end
        //we do no not need to use DUMMY_BIT, directly use the VALID_BIT=0
        let num_splits = self.number_of_splits();
        let part_id = call_seq.get_part_id();

        match dep_info.dep_type() {
            12 => {
                let data_enc_len = input.get_enc_data::<Vec<ItemE>>().len();
                let data_iter = Box::new((0..data_enc_len).map(move |i| input.get_enc_data::<Vec<ItemE>>()[i].clone()).map(|bl_enc| {
                    if bl_enc.is_empty() {
                        Vec::new()
                    } else {
                        ser_decrypt::<Vec<(Self::Item, bool)>>(&bl_enc).into_iter()
                        .map(|(t, m)| (t, (m as u64) << VALID_BIT))
                        .collect::<Vec<_>>()
                    }
                }));
                let (data, num_invalids) = obliv_global_filter_stage1(data_iter);
                let data_ptr = Box::into_raw(Box::new(data)) as *mut u8 as usize;
                let mut info = create_enc();
                merge_enc(&mut info, &ser_encrypt(&data_ptr));
                merge_enc(&mut info, &ser_encrypt(&num_invalids));
                (to_ptr(info), 0 as *mut u8)
            },
            13 => {
                let (data_ptr, num_invalids) = input.get_enc_data::<(ItemE, Vec<ItemE>)>();
                let data_ptr: usize = ser_decrypt(&data_ptr.clone());
                let nums_invalids = num_invalids.iter().map(|x| ser_decrypt::<usize>(&x.clone())).collect::<Vec<_>>();
                let data = *unsafe {
                    Box::from_raw(data_ptr as *mut u8 as *mut Vec<Vec<(Self::Item, u64)>>)
                };
                let buckets = obliv_global_filter_stage2_t(data, part_id, num_splits, nums_invalids, input.get_parallel());
                let mut buckets_enc = create_enc();
                for bucket in buckets {
                    merge_enc(&mut buckets_enc, &batch_encrypt(&bucket, false));
                }
                (to_ptr(buckets_enc), 0 as *mut u8)
            },
            14 => {
                let (parts, max_len) = read_sorted_bucket::<(Self::Item, u64)>(input.get_enc_data::<Vec<Vec<ItemE>>>(), input.get_parallel());
                let part = obliv_global_filter_stage3_t(parts, max_len);
                let part_enc = batch_encrypt(&part, true);
                (to_ptr(part_enc), 0 as *mut u8)
            }
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn randomize_in_place_(&self, input: *mut u8, seed: Option<u64>, num: u64) -> *mut u8 {
        let sample_enc = unsafe { (input as *const Vec<ItemE>).as_ref() }.unwrap();
        let mut sample: Vec<Self::Item> = batch_decrypt(sample_enc, true);
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };
        utils::randomize_in_place(&mut sample, &mut rng);
        sample = sample.into_iter().take(num as usize).collect();
        let sample_enc = batch_encrypt(&sample, true);
        to_ptr(sample_enc)
    }

    fn take_(&self, input: *mut u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        let data_enc = unsafe { (input as *const Vec<ItemE>).as_ref() }.unwrap();
        let mut data: Vec<Self::Item> = batch_decrypt(data_enc, true);
        data = data.into_iter().take(should_take).collect();
        *have_take = data.len();
        let data_enc = batch_encrypt(&data, true);
        to_ptr(data_enc)
    }

    /// Return a new RDD containing only the elements that satisfy a predicate.
    #[track_caller]
    fn filter<F>(&self, predicate: F) -> SerArc<dyn Op<Item = Self::Item>>
    where
        F: SerFunc(&Self::Item) -> bool + Copy,
        Self: Sized,
    {
        let new_op = SerArc::new(Filter::new(self.get_op(), predicate));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn map<U, F>(&self, f: F) -> SerArc<dyn Op<Item = U>>
    where
        Self: Sized,
        U: Data,
        F: SerFunc(Self::Item) -> U,
    {
        let new_op = SerArc::new(Mapper::new(self.get_op(), f));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn flat_map<U, F>(&self, f: F) -> SerArc<dyn Op<Item = U>>
    where
        Self: Sized,
        U: Data,
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
    {
        let new_op = SerArc::new(FlatMapper::new(self.get_op(), f));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn reduce<F>(&self, f: F) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
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
        let new_op = SerArc::new(Reduced::new(self.get_op(), reduce_partition));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::<Self::Item, ItemE>::new(None, None))
    }

    //secure_*** are used to receive
    #[track_caller]
    fn secure_reduce<F>(&self, f: F, tail_info: &TailCompInfo) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        Ok(Text::<Self::Item, ItemE>::rec(tail_info))
    }

    #[track_caller]
    fn fold<F>(&self, init: Self::Item, f: F) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                iter.fold(zero.clone(), &cf)
        });
        let new_op = SerArc::new(Fold::new(self.get_op(), reduce_partition));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::<Self::Item, ItemE>::new(None, None))
    }

    #[track_caller]
    fn secure_fold<F>(&self, init: Self::Item, f: F, tail_info: &TailCompInfo) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        Ok(Text::<Self::Item, ItemE>::rec(tail_info))
    }

    #[track_caller]
    fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<Text<U, ItemE>>
    where
        Self: Sized,
        U: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn)
        );
        let zero = init.clone();
        let combine = Fn!(
            move |iter: Box<dyn Iterator<Item = U>>| iter.fold(zero.clone(), &comb_fn)
        );
        let new_op = SerArc::new(Aggregated::new(self.get_op(), reduce_partition, combine));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::<U, ItemE>::new(None, None))
    }

    #[track_caller]
    fn secure_aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF, tail_info: &TailCompInfo) -> Result<Text<U, ItemE>>
    where
        Self: Sized,
        U: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        Ok(Text::<U, ItemE>::rec(tail_info))
    }

    #[track_caller]
    fn collect(&self) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::new(None, None))
    }

    #[track_caller]
    fn secure_collect(&self, tail_info: &TailCompInfo) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::rec(tail_info))
    }
    
    #[track_caller]
    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let new_op = SerArc::new(Count::new(self.get_op()));
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
    ) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
    {
        let mapped = self.map(Box::new(Fn!(|x| (Some(x), None)))
            as Box<
                dyn Func(Self::Item) -> (Option<Self::Item>, Option<Self::Item>),
            >);
        self.get_context().add_num(1);
        let reduced_by_key = mapped.reduce_by_key(Box::new(Fn!(|(_x, y)| y)),
            num_partitions);
        self.get_context().add_num(1);
        reduced_by_key.map(Box::new(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, _y) = x;
            x.unwrap()
        })))
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct(&self) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    #[track_caller]
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        let new_op = SerArc::new(PartitionwiseSampled::new(self.get_op(), sampler, true));
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
    ) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
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
    ) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
    where
        Self: Sized,
    {
        let op = self.sample(with_replacement, 0 as f64); //padding
        let r = op.secure_collect(tail_info);
        r
    }

    #[track_caller]
    fn take(&self, num: usize) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::new(None, None))
    }

    #[track_caller]
    fn secure_take(&self, num: usize, tail_info: &TailCompInfo) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::rec(tail_info))
    }

    #[track_caller]
    fn union(
        &self,
        other: Arc<dyn Op<Item = Self::Item>>,
    ) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Clone,
    {
        let new_op = SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn Op<Item = Self::Item>>,
            other,
        ]));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn zip<S>(
        &self,
        second: Arc<dyn Op<Item = S>>,
    ) -> SerArc<dyn Op<Item = (Self::Item, S)>>
    where
        Self: Clone,
        S: Data,
    {
        let new_op = SerArc::new(Zipped::new(
            Arc::new(self.clone()) as Arc<dyn Op<Item = Self::Item>>,
            second,
        ));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn key_by<T, F>(&self, func: F) -> SerArc<dyn Op<Item = (T, Self::Item)>>
    where
        Self: Sized,
        T: Data,
        F: SerFunc(&Self::Item) -> T,
    {
        self.map(Fn!(move |k: Self::Item| -> (T, Self::Item) {
            let t = (func)(&k);
            (t, k)
        }))
    }

}