use core::marker::PhantomData;
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

use crate::{CACHE, Fn, OP_MAP, CNT_PER_PARTITION};
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

pub type ItemE = Vec<u8>;
type ResIter<T> = Box<dyn Iterator<Item = Box<dyn Iterator<Item = T>>>>;

pub const MAX_ENC_BL: usize = 1024;
pub const CACHE_LIMIT: usize = 4_000_000;
pub const ENABLE_CACHE_INSIDE: bool = false;
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
pub fn column_sort_step_2<T>(tid: u64, sub_parts: Vec<ItemE>, max_len: usize, num_output_splits: usize) -> Vec<Vec<ItemE>>
where
    T: Data
{
    //TODO: can be optimized to avoid unneccessary copy, as column_sort_step_4 shows
    crate::ALLOCATOR.set_switch(true);
    let mut buckets_enc = vec![Vec::new(); num_output_splits];
    crate::ALLOCATOR.set_switch(false);

    let mut i = 0;
    for sub_part in sub_parts.iter() {
        let mut buckets = vec![Vec::with_capacity(max_len.saturating_sub(1) / num_output_splits + 1); num_output_splits];
        let sub_part: Vec<T> = ser_decrypt(&sub_part.clone());
        for kc in sub_part {
            buckets[i % num_output_splits].push(kc);
            i += 1;
        }

        for (j, bucket_enc) in buckets.into_iter().map(|x| ser_encrypt(&x)).enumerate() {
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
pub fn column_sort_step_4_6_8<T>(tid: u64, sub_parts: Vec<ItemE>, cnt_per_partition: usize, max_len: usize, n: usize, num_output_splits: usize) -> Vec<Vec<ItemE>>
where
    T: Data
{
    let n_last = n - (sub_parts.len() - 1) * max_len;
    let chunk_size = cnt_per_partition / num_output_splits;
    let mut next_i = 0;
    let mut n_acc = 0;
    let last_j = sub_parts.len() - 1;
    let mut r: Vec<T> = Vec::new();

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
                sub_part.append(&mut ser_decrypt(&sub_part_enc.clone()));
                r = sub_part.split_off(chunk_size - n_acc);
                let new_sub_part_enc = ser_encrypt(&sub_part);
                merge_enc(&mut buckets_enc[next_i], &new_sub_part_enc);
                crate::ALLOCATOR.set_switch(true);
                drop(sub_part_enc);
                crate::ALLOCATOR.set_switch(false);
            } else {
                if r.len() > 0 {
                    let r_enc = ser_encrypt(&r);
                    r = Vec::new();
                    merge_enc(&mut buckets_enc[next_i], &r_enc);
                }
                crate::ALLOCATOR.set_switch(true);
                buckets_enc[next_i].push(sub_part_enc);
                crate::ALLOCATOR.set_switch(false);
            }
            next_i += 1;
            n_acc = r.len();
        } else {
            if r.len() > 0 {
                let r_enc = ser_encrypt(&r);
                r = Vec::new();
                merge_enc(&mut buckets_enc[next_i], &r_enc);
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
        let r_enc = ser_encrypt(&r);
        crate::ALLOCATOR.set_switch(true);
        buckets_enc[next_i].push(r_enc.clone());
        crate::ALLOCATOR.set_switch(false);
    }
    
    crate::ALLOCATOR.set_switch(true);
    drop(iter_cur);
    drop(iter);
    crate::ALLOCATOR.set_switch(false);
    buckets_enc
}

pub fn combined_column_sort_step_2<K, V>(tid: u64, input: Input, op_id: OpId, part_id: usize, num_output_splits: usize) -> *mut u8
where
    K: Data + Ord,
    V: Data
{
    let data_enc = input.get_enc_data::<Vec<ItemE>>();
    let mut sub_parts = create_enc();
    let mut max_len = 0;
    for sub_part in data_enc {
        let mut sub_part: Vec<(Option<K>, V)> = ser_decrypt(&sub_part.clone());
        if sub_part.is_empty() {
            continue;
        }
        max_len = std::cmp::max(max_len, sub_part.len());
        sub_part.sort_unstable_by(cmp_f);
        let sub_part_enc = ser_encrypt(&sub_part);
        merge_enc(&mut sub_parts, &sub_part_enc);
    }
    if max_len == 0 {
        assert!(CNT_PER_PARTITION.lock().unwrap().insert((op_id, part_id), 0).is_none());
        //although the type is incorrect, it is ok because it is empty.
        crate::ALLOCATOR.set_switch(true);
        drop(sub_parts);
        let buckets_enc: Vec<Vec<ItemE>> = Vec::new();
        crate::ALLOCATOR.set_switch(false);
        to_ptr(buckets_enc)
    } else {
        let mut sort_helper = SortHelper::<K, V>::new(sub_parts, max_len, true);
        sort_helper.sort();
        let (sub_parts, num_real_elem) = sort_helper.take();
        assert!(CNT_PER_PARTITION.lock().unwrap().insert((op_id, part_id), num_real_elem).is_none());
        let buckets_enc = column_sort_step_2::<(Option<K>, V)>(tid, sub_parts, max_len, num_output_splits);
        to_ptr(buckets_enc)
    }
}

pub fn combined_column_sort_step_4_6_8<K, V>(tid: u64, input: Input, dep_info: &DepInfo, op_id: OpId, part_id: usize, num_output_splits: usize) -> *mut u8
where
    K: Data + Ord,
    V: Data
{
    let data_enc_ref = input.get_enc_data::<Vec<Vec<ItemE>>>();
    let mut max_len = 0;
    let mut data_enc = create_enc();
    //TODO: re encrypt the sub part because in step 2, the size of sub part is shrinked by num_of_splits
    for part in data_enc_ref {
        let mut part_enc = create_enc();
        let mut sub_part: Vec<(Option<K>, V)> = vec![];
        for sub_part_enc in part.iter() {
            //we can derive max_len from the first sub partition in each partition
            sub_part.append(&mut ser_decrypt(&sub_part_enc.clone()));
            let cur_memory = crate::ALLOCATOR.get_max_memory_usage().0;
            if cur_memory > CACHE_LIMIT/input.get_parallel() {
                max_len = std::cmp::max(max_len, sub_part.len());
                let sub_part_enc = ser_encrypt(&sub_part);
                merge_enc(&mut part_enc, &sub_part_enc);
                sub_part = Vec::new();
                crate::ALLOCATOR.reset_max_memory_usage();
            }
        }
        if !sub_part.is_empty() {
            max_len = std::cmp::max(max_len, sub_part.len());
            let sub_part_enc = ser_encrypt(&sub_part);
            merge_enc(&mut part_enc, &sub_part_enc);
        }
        if part_enc.is_empty() {
            crate::ALLOCATOR.set_switch(true);
            drop(part_enc);
            crate::ALLOCATOR.set_switch(false);
        } else {
            crate::ALLOCATOR.set_switch(true);
            data_enc.push(part_enc);
            crate::ALLOCATOR.set_switch(false);
        }
    }
    if max_len == 0 {
        crate::ALLOCATOR.set_switch(true);
        drop(data_enc);
        crate::ALLOCATOR.set_switch(false);
        if dep_info.dep_type() == 23 || dep_info.dep_type() == 28 {
            CNT_PER_PARTITION.lock().unwrap().remove(&(op_id, part_id)).unwrap();
        }
        res_enc_to_ptr(Vec::<Vec<ItemE>>::new())
    } else {
        let mut sort_helper = SortHelper::<K, V>::new_with(data_enc, max_len, true);
        sort_helper.sort();
        let (sub_parts, num_real_elem) = sort_helper.take();
        let cnt_per_partition = *CNT_PER_PARTITION.lock().unwrap().get(&(op_id, part_id)).unwrap();
        let buckets_enc = match dep_info.dep_type() {
            21 | 26 => column_sort_step_4_6_8::<(Option<K>, V)>(tid, sub_parts, cnt_per_partition, max_len, num_real_elem, num_output_splits),
            22 | 27 => column_sort_step_4_6_8::<(Option<K>, V)>(tid, sub_parts, cnt_per_partition, max_len, num_real_elem, 2),
            23 | 28 => {
                CNT_PER_PARTITION.lock().unwrap().remove(&(op_id, part_id)).unwrap();
                column_sort_step_4_6_8::<(Option<K>, V)>(tid, sub_parts, cnt_per_partition, max_len, num_real_elem, 2)
            },
            _ => unreachable!(),
        };
        to_ptr(buckets_enc)
    }
}

pub fn cmp_f<K, V>(a: &(Option<K>, V), b: &(Option<K>, V)) -> Ordering 
where 
    K: Data + Ord,
    V: Data
{
    //TODO: need to be a doubly-oblivious implementation
    if a.0.is_some() && b.0.is_some() {
        a.0.cmp(&b.0)
    } else {
        a.0.cmp(&b.0).reverse()
    }
}

//require each vec (ItemE) is sorted
pub struct SortHelper<K, V>
where
    K: Data + Ord,
    V: Data,
{
    data: Vec<Arc<Mutex<ItemE>>>,
    max_len: usize,
    num_padding: Arc<AtomicUsize>,
    ascending: bool,
    dist: Option<Vec<usize>>,
    dist_use_map: Option<HashMap<usize, AtomicBool>>,
    _marker_k: PhantomData<K>,
    _marker_v: PhantomData<V>,
}

impl<K, V> SortHelper<K, V>
where
    K: Data + Ord,
    V: Data,
{
    pub fn new(data: Vec<ItemE>, max_len: usize, ascending: bool) -> Self {
        crate::ALLOCATOR.set_switch(true);
        let data = {
            data
                .into_iter()
                .map(|x| Arc::new(Mutex::new(x)))
                .collect::<Vec<_>>()
        };
        crate::ALLOCATOR.set_switch(false);
        //note that data is outside enclave
        SortHelper { data, max_len, num_padding: Arc::new(AtomicUsize::new(0)), ascending, dist: None, dist_use_map: None, _marker_k: PhantomData, _marker_v: PhantomData}
    }

    //optimize for merging sorted arrays
    pub fn new_with(data: Vec<Vec<ItemE>>, max_len: usize, ascending: bool) -> Self {
        let dist = Some(vec![0].into_iter().chain(data.iter()
            .map(|p| p.len()))
            .scan(0usize, |acc, x| {
                *acc = *acc + x;
                Some(*acc)
            }).collect::<Vec<_>>());
        // true means the array is sorted originally, never touched in the following algorithm
        let dist_use_map = dist.as_ref().map(|dist| dist.iter().map(|k| (*k, AtomicBool::new(true))).collect::<HashMap<_, _>>());
        crate::ALLOCATOR.set_switch(true);
        let data = {
            data.into_iter().map(|p| p.into_iter().map(|x|Arc::new(Mutex::new(x)))).flatten().collect::<Vec<_>>()
        };
        crate::ALLOCATOR.set_switch(false);
        //note that data is outside enclave
        SortHelper { data, max_len, num_padding: Arc::new(AtomicUsize::new(0)), ascending, dist, dist_use_map, _marker_k: PhantomData, _marker_v: PhantomData}
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
        fn is_inconsistent_dir<K: Data + Ord, V: Data>(d: &Vec<(Option<K>, V)>, dir: bool) -> bool {
            d
                .first()
                .zip(d.last())
                .map(|(x, y)| cmp_f(x, y).is_lt() != dir)
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
                    let d: Vec<(Option<K>, V)> = ser_decrypt(&self.get_data(i));
                    if let Some(x) = d.first() {
                        first_d = Some(x.clone());
                        break;
                    }
                }
                for i in (lo..(lo + n)).rev() {
                    let d: Vec<(Option<K>, V)> = ser_decrypt(&self.get_data(i));
                    if let Some(x) = d.last() {
                        last_d = Some(x.clone());
                        break;
                    }
                }

                let (ori_dir, should_reverse) = first_d.as_ref()
                    .zip(last_d.as_ref())
                    .map(|(x, y)| {
                        let ori_dir = cmp_f(x, y).is_lt();
                        (ori_dir, ori_dir != dir)
                    }).unwrap_or((true, false));

                //originally ascending
                if ori_dir {
                    let mut d_i: Vec<(Option<K>, V)> = Vec::new();
                    let mut cnt = lo;
                    for i in lo..(lo + n) {
                        d_i.append(&mut ser_decrypt(&self.get_data(i)));
                        if d_i.len() >= max_len {
                            let mut r = d_i.split_off(max_len);
                            std::mem::swap(&mut r, &mut d_i);
                            if should_reverse {
                                r.reverse();
                            }
                            let new_d_i = ser_encrypt(&r);
                            self.set_data(cnt, &new_d_i);
                            cnt += 1;
                        }
                    }
                    let mut add_num_padding = 0;
                    while cnt < lo + n {
                        add_num_padding += max_len - d_i.len();
                        d_i.resize(max_len, (None, Default::default()));
                        if should_reverse {
                            d_i.reverse();
                        }
                        let new_d_i = ser_encrypt(&d_i);
                        self.set_data(cnt, &new_d_i);
                        d_i = Vec::new();
                        cnt += 1;
                    }
                    num_padding.fetch_add(add_num_padding, atomic::Ordering::SeqCst);
                } else {  //originally desending
                    let mut d_i: Vec<(Option<K>, V)> = vec![Default::default(); max_len];
                    let mut cur_len = 0;
                    let mut cnt = lo + n - 1;
                    for i in (lo..(lo + n)).rev() {
                        let mut v: Vec<(Option<K>, V)> = ser_decrypt(&self.get_data(i));
                        let v_len = v.len();
                        if v_len < max_len - cur_len {
                            (&mut d_i[max_len-cur_len-v_len..max_len-cur_len]).clone_from_slice(&v);
                            cur_len += v_len;
                        } else {
                            let r = v.split_off(v_len - (max_len - cur_len));
                            (&mut d_i[..max_len-cur_len]).clone_from_slice(&r);
                            if should_reverse {
                                d_i.reverse();
                            }
                            let new_d_i = ser_encrypt(&d_i);
                            self.set_data(cnt, &new_d_i);
                            cnt = cnt.wrapping_sub(1);
                            cur_len = v.len();
                            (&mut d_i[max_len-cur_len..max_len]).clone_from_slice(&v);
                        }
                    }
                    let mut add_num_padding = 0;
                    while cnt >= lo && cnt != usize::MAX { 
                        add_num_padding += max_len - cur_len;
                        for elem in &mut d_i[..max_len-cur_len] {
                            elem.0 = None;
                            elem.1 = Default::default();
                        }
                        if should_reverse {
                            d_i.reverse();
                        }
                        let new_d_i = ser_encrypt(&d_i);
                        self.set_data(cnt, &new_d_i);
                        cnt = cnt.wrapping_sub(1);
                        cur_len = 0;
                    }
                    num_padding.fetch_add(add_num_padding, atomic::Ordering::SeqCst);
                }
                if should_reverse {
                    for i in lo..(lo + n / 2) {
                        let l = 2 * lo + n - 1 - i;
                        crate::ALLOCATOR.set_switch(true);
                        {
                            let mut data_i = self.data[i].lock().unwrap();
                            let mut data_l = self.data[l].lock().unwrap();
                            std::mem::swap(&mut *data_i, &mut *data_l);
                        }
                        crate::ALLOCATOR.set_switch(false);
                    }
                }
            } else {
                let m = n.next_power_of_two() >> 1;
                for i in lo..(lo + n - m) {
                    let l = i + m;
                    //merge
                    let d_i: Vec<(Option<K>, V)> = ser_decrypt(&self.get_data(i));
                    let d_l: Vec<(Option<K>, V)> = ser_decrypt(&self.get_data(l));
    
                    let real_len = d_i.len() + d_l.len();
                    let dummy_len = max_len * 2 - real_len;
                    if real_len < max_len * 2 {
                        num_padding.fetch_add(dummy_len, atomic::Ordering::SeqCst);
                    }
    
                    let mut new_d_i = Vec::with_capacity(max_len);
                    let mut new_d_l = Vec::with_capacity(max_len);
                    //desending, append dummy elements first
                    if !dir {
                        new_d_i.append(&mut vec![(None, Default::default()); std::cmp::min(dummy_len, max_len)]);
                        new_d_l.append(&mut vec![(None, Default::default()); dummy_len.saturating_sub(max_len)]);
                    }
    
                    let mut iter_i = if is_inconsistent_dir(&d_i, dir) {
                        Box::new(d_i.into_iter().rev()) as Box<dyn Iterator<Item = (Option<K>, V)>>
                    } else {
                        Box::new(d_i.into_iter()) as Box<dyn Iterator<Item = (Option<K>, V)>>
                    };
    
                    let mut iter_l = if is_inconsistent_dir(&d_l, dir) {
                        Box::new(d_l.into_iter().rev()) as Box<dyn Iterator<Item = (Option<K>, V)>>
                    } else {
                        Box::new(d_l.into_iter()) as Box<dyn Iterator<Item = (Option<K>, V)>>
                    };
    
                    let mut cur_i = iter_i.next();
                    let mut cur_l = iter_l.next();
    
                    while cur_i.is_some() || cur_l.is_some() {
                        let should_puti = cur_l.is_none()
                            || cur_i.is_some()
                                &&  cmp_f(cur_i.as_ref().unwrap(), cur_l.as_ref().unwrap()).is_lt() == dir;
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
                        new_d_i.resize(max_len, (None, Default::default()));
                        new_d_l.resize(max_len, (None, Default::default()));
                    }
    
                    let new_d_i = ser_encrypt(&new_d_i);
                    let new_d_l = ser_encrypt(&new_d_l);
                    self.set_data(i, &new_d_i);
                    self.set_data(l, &new_d_l);
                }
    
                self.bitonic_merge_arbitrary(lo, m, dir);
                self.bitonic_merge_arbitrary(lo + m, n - m, dir);
            }
        }
    }

    pub fn get_data(&self, i: usize) -> ItemE {
        crate::ALLOCATOR.set_switch(true);
        let l = self.data[i].lock().unwrap();
        let tmp = &*l;
        crate::ALLOCATOR.set_switch(false);
        let r = tmp.clone();
        crate::ALLOCATOR.set_switch(true);
        drop(l);
        crate::ALLOCATOR.set_switch(false);
        r
    }

    pub fn set_data(&self, i: usize, v: &ItemE) {
        crate::ALLOCATOR.set_switch(true);
        *self.data[i].lock().unwrap() = v.clone();
        crate::ALLOCATOR.set_switch(false);
    } 

    pub fn take(&mut self) -> (Vec<ItemE>, usize) {
        let num_padding = self.num_padding.load(atomic::Ordering::SeqCst);
        let num_real_elem = self.data.len() * self.max_len - num_padding;
        let num_real_sub_part = self.data.len() - num_padding / self.max_len;
        let num_padding_remaining = num_real_sub_part * self.max_len - num_real_elem;

        // if core dump, check this
        crate::ALLOCATOR.set_switch(true);
        let mut data = std::mem::take(&mut self.data);
        //remove dummy elements
        let dummy_sub_parts = data.split_off(num_real_sub_part);
        drop(dummy_sub_parts);
        let mut res = Vec::with_capacity(data.len());
        for x in data.into_iter() {
            let x = Arc::try_unwrap(x).unwrap().into_inner().unwrap();
            res.push(x);
        }
        crate::ALLOCATOR.set_switch(false);

        let last = res.last_mut();
        if last.is_some() {
            let last = last.unwrap();
            let mut sub_part: Vec<(Option<K>, V)> = ser_decrypt(&last.clone());
            // assert!(sub_part[self.max_len - num_padding_remaining - 1].0.is_some()
            //     && sub_part.get(self.max_len - num_padding_remaining).map_or(true, |x| x.0.is_none()));
            assert!(sub_part.get(self.max_len - num_padding_remaining).map_or(true, |x| x.0.is_none()));
            sub_part.truncate(self.max_len - num_padding_remaining);
            let sub_part_enc = ser_encrypt(&sub_part);
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
    /// 20-23, 25-28 for first/second column sort, 24 for aggregate again
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
    //<(cached_rdd_id, part_id), data, op id>, data can not be Any object, required by lazy_static
    in_map: Arc<RwLock<HashMap<(usize, usize), (usize, OpId)>>>,
    //temperarily save the point of data, which is ready to sent out
    // <(cached_rdd_id, part_id), data ptr>
    out_map: Arc<RwLock<HashMap<(usize, usize), usize>>>,
}

impl OpCache{
    pub fn new() -> Self {
        OpCache {
            in_map: Arc::new(RwLock::new(HashMap::new())),
            out_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn contains(&self, key: (usize, usize)) -> bool {
        self.in_map.read().unwrap().contains_key(&key)
    }

    pub fn insert(&self, key: (usize, usize), data: usize, op_id: OpId) -> Option<(usize, OpId)> {
        self.in_map.write().unwrap().insert(key, (data, op_id))
    }

    //Note: should be reconstructed with Box::from_raw()
    pub fn remove(&self, key: (usize, usize)) -> Option<(usize, OpId)> {
        self.in_map.write().unwrap().remove(&key)
    }

    pub fn send(&self, key: (usize, usize)) {
        if let Some(ct_ptr) = self.out_map.write().unwrap().remove(&key) {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, key.0, key.1, ct_ptr); }
            //TODO: Handle the case res != 0
        }
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
        let out_map = std::mem::take(&mut *self.out_map.write().unwrap());
        //normally the map is empty, and it should not enter the following loop
        for ((rdd_id, part_id), data_ptr) in out_map.into_iter() {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, rdd_id, part_id, data_ptr); }
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


pub trait Op: OpBase + 'static {
    type Item: Data;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_op_base(&self) -> Arc<dyn OpBase>;
    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item>;
    fn cache(&self, data: Vec<Self::Item>) {
        ()
    }
    fn cache_from_outside(&self, key: (usize, usize)) -> Option<&'static Vec<ItemE>> {
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
            Box::from_raw(ptr as *mut u8 as *mut Vec<ItemE>)
        };
        let ct = ct_.clone();
        forget(ct_);
        batch_decrypt(*ct)
        */
        Some(unsafe {
            (ptr as *const u8 as *const Vec<ItemE>).as_ref()
        }.unwrap())
    }

    fn cache_to_outside(&self, key: (usize, usize), value: &Vec<Self::Item>) -> Option<PThread> {
        //let handle = unsafe {
        //    PThread::new(Box::new(move || {
        let ct = batch_encrypt(value, true);
        //println!("finish encryption, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        let mut out_map = CACHE.out_map.write().unwrap();
        let acc = match out_map.remove(&key) {
            Some(ptr) => {
                crate::ALLOCATOR.set_switch(true);
                let mut acc = *unsafe { Box::from_raw(ptr as *mut Vec<ItemE>) };
                combine_enc(&mut acc, ct);
                crate::ALLOCATOR.set_switch(false);
                to_ptr(acc)
            },
            None => to_ptr(ct), 
        } as usize;
        out_map.insert(key, acc);
        //println!("finish copy out, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        //    }))
        //}.unwrap();
        //Some(handle)
        None
    }

    fn get_and_remove_cached_data(&self, key: (usize, usize)) -> ResIter<Self::Item> {
        //cache inside enclave
        //TODO: parallelize
        if let Some(val) = CACHE.remove(key) {
            let v = unsafe { Box::from_raw(val.0 as *mut u8 as *mut Vec<Vec<Self::Item>>) };
            Box::new(v.into_iter().map(|x| Box::new(x.into_iter()) as Box<dyn Iterator<Item = _>>)) as ResIter<_>
        } else {
            let ct = self.cache_from_outside(key).unwrap();
            let len = ct.len();
            //cache outside enclave
            Box::new((0..len).map(move |i| {
                //println!("get cached data outside enclave");
                Box::new(ser_decrypt::<Vec<Self::Item>>(&ct[i].clone()).into_iter()) as Box<dyn Iterator<Item = _>>
            })) as ResIter<_>
        }
    }

    fn set_cached_data(&self, call_seq: &NextOpId, res_iter: ResIter<Self::Item>, is_caching_final_rdd: bool) -> ResIter<Self::Item> {
        let ope = self.get_op();
        let op_id = self.get_op_id();
        let key = call_seq.get_caching_doublet();

        Box::new(res_iter.map(move |iter| {
            //cache outside enclave
            if is_caching_final_rdd {
                iter
            } else {
                let res = iter.collect::<Vec<_>>();
                //cache inside enclave
                if ENABLE_CACHE_INSIDE {
                    let mut data = CACHE.remove(key).map_or(vec![], |x| *unsafe{Box::from_raw(x.0 as *mut u8 as *mut Vec<Vec<Self::Item>>)});
                    data.push(res.clone());
                    CACHE.insert(key, Box::into_raw(Box::new(data)) as *mut u8 as usize, op_id);
                } 
                //it should be always cached outside for inter-machine communcation
                let _handle = ope.cache_to_outside(key, &res);
                Box::new(res.into_iter()) as Box<dyn Iterator<Item = _>>
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

    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool) {
        if is_enc {
            crate::ALLOCATOR.set_switch(true);
            let res = unsafe { Box::from_raw(res_ptr as *mut Vec<ItemE>) };
            drop(res);
            crate::ALLOCATOR.set_switch(false);
        } else {
            let _res = unsafe { Box::from_raw(res_ptr as *mut Vec<Self::Item>) };
        }
    }

    fn narrow(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        println!("regular narrow");
        let result_iter = self.compute(call_seq, input);
        //acc stays outside enclave
        let mut acc = create_enc();
        for result in result_iter {
            let block_enc = batch_encrypt(&result.collect::<Vec<_>>(), true);
            combine_enc(&mut acc, block_enc);
        }
        //cache to outside
        let key = call_seq.get_caching_doublet();
        CACHE.send(key);
        to_ptr(acc)
    } 

    fn shuffle(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
        let tid = call_seq.tid;
        let now = Instant::now();

        let result_iter = Box::new(self.compute(call_seq, input)
            .map(|x| Box::new(x.collect::<Vec<_>>()) as Box<dyn Any>)) as Box<dyn Iterator<Item = Box<dyn Any>>>;
        let result_ptr = shuf_dep.do_shuffle_task(tid, result_iter, input.get_parallel());

        //cache to outside
        let key = call_seq.get_caching_doublet();
        CACHE.send(key);

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("tid: {:?}, shuffle write: {:?}s, cur mem: {:?}B", call_seq.tid, dur, crate::ALLOCATOR.get_memory_usage());
        //let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
        result_ptr
    }

    fn randomize_in_place_(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        let sample_enc = unsafe{ (input as *const Vec<ItemE>).as_ref() }.unwrap(); 
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

    fn take_(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        let data_enc = unsafe{ (input as *const Vec<ItemE>).as_ref() }.unwrap(); 
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
        F: Fn(&Self::Item) -> bool + Send + Sync + Clone + Copy + 'static,
        Self: Sized,
    {
        let filter_fn = Fn!(move |_index: usize, 
                                  items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> {
            Box::new(items.filter(predicate))
        });
        let new_op = SerArc::new(MapPartitions::new(self.get_op(), filter_fn));
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