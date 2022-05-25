use std::any::Any;
use std::boxed::Box;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, SgxRwLock as RwLock, atomic::{self, AtomicBool}};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func};
use crate::op::{res_enc_to_ptr, to_ptr, MAX_ENC_BL, MERGE_FACTOR, CACHE_LIMIT, Input, OpId};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use downcast_rs::DowncastSync;
use itertools::Itertools;

#[derive(Clone)]
pub enum Dependency {
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

impl Dependency {
    pub fn get_parent(&self) -> OpId {
        match self {
            Dependency::NarrowDependency(nar) => nar.get_parent(),
            Dependency::ShuffleDependency(shuf) => shuf.get_parent(),
        }
    }

    pub fn get_child(&self) -> OpId {
        match self {
            Dependency::NarrowDependency(nar) => nar.get_child(),
            Dependency::ShuffleDependency(shuf) => shuf.get_child(),
        }
    }
}

impl<K, V, C, KE, CE> From<ShuffleDependency<K, V, C, KE, CE>> for Dependency 
where
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    fn from(shuf_dep: ShuffleDependency<K, V, C, KE, CE>) -> Self {
        Dependency::ShuffleDependency(Arc::new(shuf_dep) as Arc<dyn ShuffleDependencyTrait>)
    }
}

pub trait NarrowDependencyTrait: DowncastSync + Send + Sync {
    fn get_parent(&self) -> OpId;

    fn get_child(&self) -> OpId;
}
impl_downcast!(sync NarrowDependencyTrait);

#[derive(Clone)]
pub struct OneToOneDependency {
    parent: OpId,
    child: OpId, 
}

impl OneToOneDependency {
    pub fn new(parent: OpId, child: OpId) -> Self {
        OneToOneDependency{ parent, child }
    }
}

impl NarrowDependencyTrait for OneToOneDependency {
    fn get_parent(&self) -> OpId {
        self.parent
    }

    fn get_child(&self) -> OpId {
        self.child
    }

}

#[derive(Clone)]
pub struct RangeDependency {
    in_start: usize,
    out_start: usize,
    length: usize,
    parent: OpId,
    child: OpId,
}

impl RangeDependency {
    pub fn new(in_start: usize, out_start: usize, length: usize, parent: OpId, child: OpId) -> Self {
        RangeDependency { in_start, out_start, length, parent, child}
    }
}

impl NarrowDependencyTrait for RangeDependency {
    fn get_parent(&self) -> OpId {
        self.parent
    }

    fn get_child(&self) -> OpId {
        self.child
    }
}

pub trait ShuffleDependencyTrait: DowncastSync + Send + Sync  { 
    fn change_partitioner(&self, reduce_num: usize);
    fn create_buckets(&self, tid: u64) -> *mut u8;
    fn do_shuffle_task(&self, tid: u64, iter: Box<dyn Any>, buckets: *mut u8) -> *mut u8;
    fn finish_buckets(&self, tid: u64, buckets: *mut u8, result_ptr: Option<*mut u8>) -> Option<*mut u8>;
    fn free_buckets(&self, tid: u64, buckets: *mut u8);
    fn pre_merge(&self, tid: u64, input: Input) -> usize;
    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8);
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8);
    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool);
    fn get_parent(&self) -> OpId;
    fn get_child(&self) -> OpId;
    fn get_identifier(&self) -> usize;
    fn set_parent_and_child(&self, parent_op_id: OpId, child_op_id: OpId) -> Arc<dyn ShuffleDependencyTrait>;
}

impl_downcast!(sync ShuffleDependencyTrait);

pub struct ShuffleDependency<K, V, C, KE, CE> 
where
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    pub is_cogroup: bool,
    pub aggregator: Arc<Aggregator<K, V, C>>,
    pub partitioner: RwLock<Box<dyn Partitioner>>,
    pub split_num_unchanged: Arc<AtomicBool>,
    pub identifier: usize,
    pub parent: OpId,
    pub child: OpId,
    pub fe: Box<dyn Func(Vec<(K, C)>) -> (KE, CE)>,
    pub fd: Box<dyn Func((KE, CE)) -> Vec<(K, C)>>,
}

impl<K, V, C, KE, CE> ShuffleDependency<K, V, C, KE, CE> 
where 
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    pub fn new(
        is_cogroup: bool,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
        identifier: usize,
        parent: OpId,
        child: OpId,
        fe: Box<dyn Func(Vec<(K, C)>) -> (KE, CE)>,
        fd: Box<dyn Func((KE, CE)) -> Vec<(K, C)>>,
    ) -> Self {
        ShuffleDependency {
            is_cogroup,
            aggregator,
            partitioner: RwLock::new(partitioner),
            split_num_unchanged: Arc::new(AtomicBool::new(true)),
            identifier,
            parent,
            child,
            fe,
            fd,
        }
    }

    pub fn encrypt_buckets(&self, buckets: Vec<Vec<(K, C)>>) -> Vec<Vec<(KE, CE)>> {
        let result = buckets.into_iter()
            .map(|mut bucket| {
                //batch encrypt
                let mut len = bucket.len();
                let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL+1);
                if len >= 1 {    
                    len -= 1;
                    let input = vec![bucket.remove(0)];
                    data_enc.push((self.fe)(input));
                }
                //TODO: need to adjust the block size
                while len >= MAX_ENC_BL {    
                    len -= MAX_ENC_BL;
                    let remain = bucket.split_off(MAX_ENC_BL);
                    let input = bucket;
                    bucket = remain;
                    data_enc.push((self.fe)(input));
                }
                if len != 0 {
                    data_enc.push((self.fe)(bucket));
                }
                data_enc
            })
            .collect::<Vec<_>>();  //BTreeMap to Vec
        result
    }

    pub fn batch_decrypt(&self, data_enc: Vec<(KE, CE)>) -> Vec<(K, C)> {
        let mut data = Vec::new();
        for block in data_enc {
            let mut pt = (self.fd)(block);
            data.append(&mut pt); //need to check security
        }
        data
    }

    pub fn pre_merge_core(&self, parallel_num: usize, buckets_enc: &[Vec<(KE, CE)>], lower: &mut Vec<usize>, upper: &mut Vec<usize>, upper_bound: &Vec<usize>, mut combiners: Vec<(K, C)>, sorted_max_key: &mut BTreeMap<(K, usize), usize>) -> (Vec<(KE, CE)>, Vec<(K, C)>)  {
        let aggregator = self.aggregator.clone();
        let mut block = Vec::new();
        if sorted_max_key.is_empty() {
            block = buckets_enc.iter()
                .enumerate()
                .map(|(idx, sub_part)| {
                    let l = &mut lower[idx];
                    let u = &mut upper[idx];
                    let ub = upper_bound[idx];
                    match *l < ub {
                        true => {
                            let data_enc = sub_part[*l..*u].to_vec();
                            *l += 1;
                            *u += 1;
                            self.batch_decrypt(data_enc)
                        },
                        false => Vec::new(),
                    }
                }).collect::<Vec<_>>();
            *sorted_max_key = block.iter()
                .enumerate()
                .filter(|(idx, sub_part)| sub_part.last().is_some())
                .map(|(idx, sub_part)| ((sub_part.last().unwrap().0.clone(), idx), idx))
                .collect::<BTreeMap<_, _>>();
        } else {
            block.resize(lower.len(), Vec::new());
        }

        let mut cur_memory = crate::ALLOCATOR.get_memory_usage().1;
        while cur_memory < CACHE_LIMIT/parallel_num {
            let entry = match sorted_max_key.first_entry() {
                Some(entry) => entry,
                None => break,
            };
            let idx = *entry.get();
            entry.remove_entry();
            if lower[idx] >= upper_bound[idx] {
                continue;
            }
            let mut inc_block = self.batch_decrypt(buckets_enc[idx][lower[idx]..upper[idx]].to_vec());
            block[idx].append(&mut inc_block); 
            sorted_max_key.insert((block[idx].last().unwrap().0.clone(), idx), idx);
            lower[idx] += 1;
            upper[idx] += 1;
            cur_memory = crate::ALLOCATOR.get_memory_usage().1;
        }

        let mut iter = block.into_iter().chain(vec![combiners]).kmerge_by(|a, b| a.0 < b.0);
        let first = iter.next();
        combiners = match first {
            Some(pair) => {
                let mut combiners = vec![pair];
                for (k, c) in iter{
                    if k == combiners.last().unwrap().0 {
                        let pair = combiners.last_mut().unwrap();
                        pair.1 = (aggregator.merge_combiners)((pair.1.clone(), c));
                    } else {
                        combiners.push((k.clone(), c));
                    }
                }
                combiners
            },
            None => Vec::new(),
        };

        if lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
            let min_max_k = sorted_max_key.first_entry().unwrap();
            let seek = &min_max_k.key().0;
            let idx = combiners.binary_search_by(|probe| probe.0.cmp(seek)).unwrap();
            let remained_c = combiners.split_off(idx);
            let res_bl = self.encrypt_buckets(vec![combiners]).remove(0);
            (res_bl, remained_c)
        } else {
            (self.encrypt_buckets(vec![combiners]).remove(0), vec![])
        }
    }

}

impl<K, V, C, KE, CE> ShuffleDependencyTrait for ShuffleDependency<K, V, C, KE, CE>
where
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    fn change_partitioner(&self, reduce_num: usize) {
        let mut cur_partitioner = self.partitioner.write().unwrap();
        if reduce_num != cur_partitioner.get_num_of_partitions() {
            cur_partitioner.set_num_of_partitions(reduce_num);
            self.split_num_unchanged.store(false, atomic::Ordering::SeqCst);
        }
    }

    fn create_buckets(&self, tid: u64) -> *mut u8 {
        let partitioner = self.partitioner.read().unwrap().clone();
        let num_output_splits = partitioner.get_num_of_partitions();
        let buckets: Vec<BTreeMap<K, C>> = (0..num_output_splits)
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>();
        Box::into_raw(Box::new(buckets)) as *mut u8
    }

    //fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>) -> *mut u8 {
    fn do_shuffle_task(&self, tid: u64, iter: Box<dyn Any>, buckets: *mut u8) -> *mut u8 {
        let aggregator = self.aggregator.clone();
        let partitioner = self.partitioner.read().unwrap().clone();
        let mut buckets = unsafe{ Box::from_raw(buckets as *mut Vec<BTreeMap<K, C>>) };
        let mut data = *iter.downcast::<Vec<(K, V)>>().unwrap();
        if aggregator.is_default {
            //data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            for group in data.group_by(|x, y| x.0 == y.0) {
                let k = group[0].0.clone();
                let bucket_id = partitioner.get_partition(&k);
                let bucket = &mut buckets[bucket_id];
                let v = group.iter().map(|(k, v)| v.clone()).collect::<Vec<_>>();
                let v = (Box::new(v) as Box<dyn Any>).downcast::<C>().unwrap();
                if let Some(old_v) = bucket.get_mut(&k) {
                    let input = ((old_v.clone(), *v),);
                    let output = aggregator.merge_combiners.call(input);
                    *old_v = output;
                } else {
                    bucket.insert(k, aggregator.merge_combiners.call(((Default::default(), *v),)));
                }
            }
        } else {
            for (count, i) in data.into_iter().enumerate() {
                let (k, v) = i;
                let bucket_id = partitioner.get_partition(&k);
                let bucket = &mut buckets[bucket_id];
                if let Some(old_v) = bucket.get_mut(&k) {
                    let input = ((old_v.clone(), v),);
                    let output = aggregator.merge_value.call(input);
                    *old_v = output;
                } else {
                    bucket.insert(k, aggregator.create_combiner.call((v,)));
                }
            }
        }

        Box::into_raw(buckets) as *mut u8
    }

    fn finish_buckets(&self, tid: u64, buckets: *mut u8, result_ptr: Option<*mut u8>) -> Option<*mut u8> {
        let buckets = unsafe{ Box::from_raw(buckets as *mut Vec<BTreeMap<K, C>>) };
        let buckets = buckets.into_iter().map(|bucket| bucket.into_iter().collect::<Vec<_>>()).collect::<Vec<_>>();
        let result = self.encrypt_buckets(buckets);
        let ptr = Some(match result_ptr {
            Some(ptr) => {
                crate::ALLOCATOR.set_switch(true);
                let mut res = *unsafe { Box::from_raw(ptr as *mut Vec<Vec<Vec<(KE, CE)>>>) };
                res.push(result.clone());
                crate::ALLOCATOR.set_switch(false);
                to_ptr(res)
            },
            None => {
                res_enc_to_ptr(vec![result])
            },
        });
        ptr
    }

    fn free_buckets(&self, tid: u64, buckets: *mut u8) {
        let mut _buckets = unsafe{ Box::from_raw(buckets as *mut Vec<BTreeMap<K, C>>) };
    }

    fn pre_merge(&self, tid: u64, input: Input) -> usize {
        let data_original = input.get_enc_data::<Vec<Vec<(KE, CE)>>>();
        crate::ALLOCATOR.set_switch(true); 
        let mut data = Vec::new();
        let mut res = Vec::new();
        crate::ALLOCATOR.set_switch(false);

        let mut n = data_original.len();
        let mut c = 0;
        println!("c = {:?}, n = {:?}", c, n);

        while n > MERGE_FACTOR {
            let m = (n - 1) / MERGE_FACTOR + 1;
            crate::ALLOCATOR.set_switch(true); 
            res.resize(m, Vec::new());
            crate::ALLOCATOR.set_switch(false);
            for i in 0..m {
                let start = i * MERGE_FACTOR;
                let end = std::cmp::min((i + 1) * MERGE_FACTOR, n);
                let num_sub_part = end - start;
                let mut combiners: Vec<(K, C)> = Vec::new();
                let mut sorted_max_key: BTreeMap<(K, usize), usize> = BTreeMap::new();
                let buckets_enc = if c == 0 {
                    &data_original[start..end]
                } else {
                    &data[start..end]
                };
                let mut lower = vec![0; num_sub_part];
                let mut upper = vec![1; num_sub_part];
                let upper_bound = buckets_enc
                    .iter()
                    .map(|sub_part| sub_part.len())
                    .collect::<Vec<_>>();
                while lower
                    .iter()
                    .zip(upper_bound.iter())
                    .filter(|(l, ub)| l < ub)
                    .count()
                    > 0
                {
                    upper = upper
                        .iter()
                        .zip(upper_bound.iter())
                        .map(|(l, ub)| std::cmp::min(*l, *ub))
                        .collect::<Vec<_>>();
                    let (res_bl, remained_c) = self.pre_merge_core(input.get_parallel(), buckets_enc, &mut lower, &mut upper, &upper_bound, combiners, &mut sorted_max_key);
                    combiners = remained_c;
                    crate::ALLOCATOR.set_switch(true);
                    res[i].extend_from_slice(&res_bl);
                    crate::ALLOCATOR.set_switch(false);
                    lower = lower
                        .iter()
                        .zip(upper_bound.iter())
                        .map(|(l, ub)| std::cmp::min(*l, *ub))
                        .collect::<Vec<_>>();
                }
            }
            crate::ALLOCATOR.set_switch(true);
            drop(data);
            data = res;
            res = Vec::new();
            crate::ALLOCATOR.set_switch(false);
            n = data.len();
            c += 1;
            println!("c = {:?}, n = {:?}", c, n);
        }
        crate::ALLOCATOR.set_switch(true);
        //to be compatible with bucket encryption in shuffle write
        let res = vec![data];
        crate::ALLOCATOR.set_switch(false);
        let res_ptr = to_ptr(res);
        res_ptr as usize
    }
    

    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8){
        let mut idx = Idx::new();
        let buckets_enc = unsafe { Box::from_raw(p_data_enc as *mut Vec<Vec<(KE, CE)>>) };
        buckets_enc.send(buf, &mut idx);
        forget(buckets_enc);
    }
    
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8) {
        let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Vec<(KE, CE)>>) };
        let buckets_enc = unsafe { Box::from_raw(p_data_enc as *mut Vec<Vec<(KE, CE)>>) };
        v_out.clone_in_place(&buckets_enc);
        forget(v_out);
        //and free encrypted buckets
    }

    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool) {
        assert!(is_enc);
        crate::ALLOCATOR.set_switch(true);
        let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<Vec<(KE, CE)>>>) };
        drop(res);
        crate::ALLOCATOR.set_switch(false);
    }

    fn get_parent(&self) -> OpId {
        self.parent
    }

    fn get_child(&self) -> OpId {
        self.child
    }

    fn get_identifier(&self) -> usize {
        self.identifier
    }

    fn set_parent_and_child(&self, parent_op_id: OpId, child_op_id: OpId) -> Arc<dyn ShuffleDependencyTrait> {
        Arc::new(ShuffleDependency::new(
            self.is_cogroup,
            self.aggregator.clone(),
            self.partitioner.read().unwrap().clone(),
            self.identifier,
            parent_op_id,
            child_op_id,
            self.fe.clone(),
            self.fd.clone(),
        )) as Arc<dyn ShuffleDependencyTrait>
    }

}
