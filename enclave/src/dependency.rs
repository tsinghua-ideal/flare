use core::ops;
use std::any::Any;
use std::boxed::Box;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::iter::Extend;
use std::mem::forget;
use std::sync::{Arc, SgxRwLock as RwLock, atomic::{self, AtomicBool}};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::CAVE;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func};
use crate::op::{res_enc_to_ptr, MAX_ENC_BL, Input, OpId};
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
    fn has_spec_oppty(&self) -> bool;
    //fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>, is_spec: bool) -> *mut u8;
    fn do_shuffle_task(&self, tid: u64, iter: Box<dyn Any>, is_spec: bool) -> *mut u8;
    fn pre_merge(&self, tid: u64, input: Input) -> usize;
    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8);
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8);
    fn free_res_enc(&self, res_ptr: *mut u8);
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

    pub fn encrypt_buckets_spec(&self, buckets: Vec<Vec<(K, C)>>) -> Vec<Vec<u8>> {
        let result = buckets.into_iter()
            .map(|mut bucket| {
                //batch encrypt
                let mut len = bucket.len();
                let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL+1);
                //TODO: need to adjust the block size
                if len >= 1 {    
                    len -= 1;
                    let input = vec![bucket.remove(0)];
                    data_enc.push((self.fe)(input));
                }
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
                bincode::serialize(&data_enc).unwrap()
            })
            .collect::<Vec<_>>();  //BTreeMap to Vec
        result
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

    fn has_spec_oppty(&self) -> bool {
        self.split_num_unchanged.load(atomic::Ordering::SeqCst)
    }

    //fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>, is_spec: bool) -> *mut u8 {
    fn do_shuffle_task(&self, tid: u64, iter: Box<dyn Any>, is_spec: bool) -> *mut u8 {
        let aggregator = self.aggregator.clone();
        let partitioner = self.partitioner.read().unwrap().clone();
        let num_output_splits = partitioner.get_num_of_partitions();
        let mut buckets: Vec<BTreeMap<K, C>> = (0..num_output_splits)
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>();

        //println!("cur mem before shuffle write: {:?}", crate::ALLOCATOR.get_memory_usage()); 
        /* 
        for (count, i) in iter.enumerate() {
            let b = i.into_any().downcast::<(K, V)>().unwrap();
            let (k, v) = *b;
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
        */
        let now = Instant::now();
        for (count, i) in iter.downcast::<Vec<(K, V)>>().unwrap().into_iter().enumerate() {
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
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("tid: {:?}, cur mem after shuffle write: {:?}, shuffle write {:?} s", tid, crate::ALLOCATOR.get_memory_usage(), dur);
        let buckets = buckets.into_iter().map(|bucket| bucket.into_iter().collect::<Vec<_>>()).collect::<Vec<_>>();
        if is_spec {
            let now = Instant::now();
            let result = self.encrypt_buckets_spec(buckets);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("tid: {:?}, cur mem before copy out: {:?}, encrypt {:?} s", tid, crate::ALLOCATOR.get_memory_usage(), dur); 
            let res_ptr = res_enc_to_ptr(result);
            //println!("cur mem after copy out: {:?}", crate::ALLOCATOR.get_memory_usage()); 
            res_ptr
        } else {
            let now = Instant::now();
            let result = self.encrypt_buckets(buckets);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            //println!("cur mem before copy out: {:?}, encrypt {:?} s", crate::ALLOCATOR.get_memory_usage(), dur); 
            let res_ptr = res_enc_to_ptr(result);
            //println!("cur mem after copy out: {:?}", crate::ALLOCATOR.get_memory_usage()); 
            res_ptr
        }
    }
    
    /*
    fn pre_merge(&self, tid: u64, input: Input) -> usize {
        let aggregator = self.aggregator.clone();
        let remained_ptr = CAVE.lock().unwrap().remove(&tid);
        let (mut combiners, mut sorted_max_key): (BTreeMap<K, Option<C>>, BTreeMap<(K, usize), usize>) = match remained_ptr {
            Some((c_ptr, s_ptr)) => (
                *unsafe { Box::from_raw(c_ptr as *mut u8 as *mut BTreeMap<K, Option<C>>) },
                *unsafe { Box::from_raw(s_ptr as *mut u8 as *mut BTreeMap<(K, usize), usize>) }
            ),
            None => (BTreeMap::new(), BTreeMap::new()),
        };
        let buckets_enc = input.get_enc_data::<Vec<Vec<(KE, CE)>>>();
        let lower = input.get_lower();
        let upper = input.get_upper();
        let block_len = input.get_block_len();
        let mut cur_len = 0;
        let upper_bound = buckets_enc.iter().map(|sub_part| sub_part.len()).collect::<Vec<_>>();
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
                            cur_len += 1;
                            self.batch_decrypt(data_enc)
                        },
                        false => Vec::new(),
                    }
                }).collect::<Vec<_>>();
            sorted_max_key = block.iter()
                .enumerate()
                .filter(|(idx, sub_part)| sub_part.last().is_some())
                .map(|(idx, sub_part)| ((sub_part.last().unwrap().0.clone(), idx), idx))
                .collect::<BTreeMap<_, _>>();
        } else {
            block.resize(lower.len(), Vec::new());
        }

        while cur_len < block_len {
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
            cur_len += 1;
            block[idx].append(&mut inc_block); 
            sorted_max_key.insert((block[idx].last().unwrap().0.clone(), idx), idx);
            lower[idx] += 1;
            upper[idx] += 1;
        }

        for (k, c) in block.into_iter().flatten() {
            if let Some(old_c) = combiners.get_mut(&k) {
                let old = old_c.take().unwrap();
                let input = ((old, c),);
                let output = aggregator.merge_combiners.call(input);
                *old_c = Some(output);
            } else {
                combiners.insert(k, Some(c));
            }
        }

        if lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
            let min_max_k = sorted_max_key.first_entry().unwrap();
            let remained_c = combiners.split_off(&min_max_k.key().0);
            let remained_s = sorted_max_key;
            //Temporary stored for next computation
            CAVE.lock().unwrap().insert(tid, 
                (Box::into_raw(Box::new(remained_c)) as *mut u8 as usize, 
                Box::into_raw(Box::new(remained_s)) as *mut u8 as usize)
            );
        }
        let result = self.encrypt_buckets(vec![combiners.into_iter().map(|(k, v)| (k, v.unwrap())).collect()]);
        let res_ptr = res_enc_to_ptr(result);
        res_ptr as usize
    }
    */
    
    fn pre_merge(&self, tid: u64, input: Input) -> usize {
        let remained_ptr = CAVE.lock().unwrap().remove(&tid);
        let (mut combiners, mut sorted_max_key): (Vec<(K, C)>, BTreeMap<(K, usize), usize>) = match remained_ptr {
            Some((c_ptr, s_ptr)) => (
                *unsafe { Box::from_raw(c_ptr as *mut u8 as *mut Vec<(K, C)>) },
                *unsafe { Box::from_raw(s_ptr as *mut u8 as *mut BTreeMap<(K, usize), usize>) }
            ),
            None => (Vec::new(), BTreeMap::new()),
        };
        let buckets_enc = input.get_enc_data::<Vec<Vec<(KE, CE)>>>();
        let lower = input.get_lower();
        let upper = input.get_upper();
        let block_len = input.get_block_len();
        let mut cur_len = 0;
        let upper_bound = buckets_enc.iter().map(|sub_part| sub_part.len()).collect::<Vec<_>>();
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
                            cur_len += 1;
                            self.batch_decrypt(data_enc)
                        },
                        false => Vec::new(),
                    }
                }).collect::<Vec<_>>();
            sorted_max_key = block.iter()
                .enumerate()
                .filter(|(idx, sub_part)| sub_part.last().is_some())
                .map(|(idx, sub_part)| ((sub_part.last().unwrap().0.clone(), idx), idx))
                .collect::<BTreeMap<_, _>>();
        } else {
            block.resize(lower.len(), Vec::new());
        }

        while cur_len < block_len {
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
            cur_len += 1;
            block[idx].append(&mut inc_block); 
            sorted_max_key.insert((block[idx].last().unwrap().0.clone(), idx), idx);
            lower[idx] += 1;
            upper[idx] += 1;
        } 

        combiners = block.into_iter().chain(vec![combiners]).kmerge_by(|a, b| a.0 < b.0).collect::<Vec<_>>();
        
        if lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
            let min_max_k = sorted_max_key.first_entry().unwrap();
            let seek = &min_max_k.key().0;
            let idx = combiners.binary_search_by(|probe| probe.0.cmp(seek)).unwrap();
            let remained_c = combiners.split_off(idx);
            let remained_s = sorted_max_key;
            //Temporary stored for next computation
            CAVE.lock().unwrap().insert(tid, 
                (Box::into_raw(Box::new(remained_c)) as *mut u8 as usize, 
                Box::into_raw(Box::new(remained_s)) as *mut u8 as usize)
            );
        }
        let result = self.encrypt_buckets(vec![combiners]);
        let res_ptr = res_enc_to_ptr(result);
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

    fn free_res_enc(&self, res_ptr: *mut u8) {
        crate::ALLOCATOR.set_switch(true);
        let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<(KE, CE)>>) };
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
