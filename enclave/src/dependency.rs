use std::any::Any;
use std::boxed::Box;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, Barrier, SgxMutex as Mutex, SgxRwLock as RwLock, atomic::{self, AtomicBool}};
use std::time::Instant;
use std::thread::{self, ThreadId, SgxThread};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func};
use crate::op::*;
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use deepsize::DeepSizeOf;
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

impl<K, V, C> From<ShuffleDependency<K, V, C>> for Dependency 
where
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
{
    fn from(shuf_dep: ShuffleDependency<K, V, C>) -> Self {
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
    fn do_shuffle_task(&self, tid: u64, opb: Arc<dyn OpBase>, call_seq: NextOpId, input: Input) -> *mut u8;
    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8);
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8);
    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool);
    fn get_parent(&self) -> OpId;
    fn get_child(&self) -> OpId;
    fn get_identifier(&self) -> usize;
    fn set_parent_and_child(&self, parent_op_id: OpId, child_op_id: OpId) -> Arc<dyn ShuffleDependencyTrait>;
}

impl_downcast!(sync ShuffleDependencyTrait);

pub struct ShuffleDependency<K, V, C> 
where
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
{
    pub is_cogroup: bool,
    pub aggregator: Arc<Aggregator<K, V, C>>,
    pub partitioner: RwLock<Box<dyn Partitioner>>,
    pub split_num_unchanged: Arc<AtomicBool>,
    pub identifier: usize,
    pub parent: OpId,
    pub child: OpId,
}

impl<K, V, C> ShuffleDependency<K, V, C> 
where 
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
{
    pub fn new(
        is_cogroup: bool,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
        identifier: usize,
        parent: OpId,
        child: OpId,
    ) -> Self {
        ShuffleDependency {
            is_cogroup,
            aggregator,
            partitioner: RwLock::new(partitioner),
            split_num_unchanged: Arc::new(AtomicBool::new(true)),
            identifier,
            parent,
            child,
        }
    }

}

impl<K, V, C> ShuffleDependencyTrait for ShuffleDependency<K, V, C>
where
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    C: Data,
{
    fn change_partitioner(&self, reduce_num: usize) {
        let mut cur_partitioner = self.partitioner.write().unwrap();
        if reduce_num != cur_partitioner.get_num_of_partitions() {
            cur_partitioner.set_num_of_partitions(reduce_num);
            self.split_num_unchanged.store(false, atomic::Ordering::SeqCst);
        }
    }

    fn do_shuffle_task(&self, tid: u64, opb: Arc<dyn OpBase>, call_seq: NextOpId, input: Input) -> *mut u8 {
        let op = opb.to_arc_op::<dyn Op<Item = (K, V)>>().unwrap();
        let mut sub_parts: Vec<Vec<(K, V)>> = Vec::new();
        let mut sub_part_size = 0;
        let results = *unsafe{ Box::from_raw(op.narrow(call_seq, input, false) as *mut Vec<Vec<(K, V)>>) };
        for mut block in results {
            let block_size = block.deep_size_of();
            let mut should_create_new = true;
            if let Some(sub_part) = sub_parts.last_mut() {
                if sub_part_size + block_size <= CACHE_LIMIT/MAX_THREAD/input.get_parallel() {
                    sub_part.append(&mut block);
                    sub_part_size += block_size;
                    should_create_new = false;
                } 
            } 
            if should_create_new {
                sub_parts.push(block);
                sub_part_size = block_size;
            }
        }

        let mut handlers = Vec::with_capacity(MAX_THREAD);
        let r = sub_parts.len().saturating_sub(1) / MAX_THREAD + 1;
        for _ in 0..MAX_THREAD {
            let mut sub_parts = sub_parts.split_off(sub_parts.len().saturating_sub(r));
            let handler = thread::Builder::new()
                .spawn(move || {
                    for sub_part in sub_parts.iter_mut() {
                        sub_part.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                    }
                    sub_parts
                }).unwrap();
            handlers.push(handler);
        }
        assert!(sub_parts.is_empty());

        let aggregator = self.aggregator.clone();
        let mut partitioner = self.partitioner.read().unwrap().clone();
        let mut num_output_splits = partitioner.get_num_of_partitions();
        num_output_splits *= MAX_THREAD + 1;
        partitioner.set_num_of_partitions(num_output_splits);

        let mut is_para_shuf = true;
        let mut handlers_res = Vec::with_capacity(MAX_THREAD);
        for (i, handler) in handlers.into_iter().enumerate() {
            let mut buckets_col = Vec::new();
            let mut sub_parts = handler.join().unwrap();
            if i == 0 {
                //sample
                let sample_data = sub_parts.pop().unwrap();
                let sample_len = sample_data.len();
                //shuffle specific
                crate::ALLOCATOR.reset_alloc_cnt();
                buckets_col.push(do_shuffle_task_core(sample_data, &aggregator, &partitioner, num_output_splits));
                let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
                let alloc_cnt_ratio = alloc_cnt as f64/(sample_len as f64);
                println!("for shuffle processing, alloc_cnt per len = {:?}", alloc_cnt_ratio);
                is_para_shuf = alloc_cnt_ratio < PARA_THRESHOLD;
            };
            if is_para_shuf {
                //launch enc
                let aggregator = aggregator.clone();
                let partitioner = partitioner.clone();
                let handler = thread::Builder::new()
                    .spawn(move || {
                        let mut acc = create_enc();
                        if let Some(buckets) = buckets_col.pop() {
                            let buckets_enc = buckets.into_iter().map(|bucket| batch_encrypt(&bucket, false)).collect::<Vec<_>>();
                            merge_enc(&mut acc, &buckets_enc);
                        }
                        for sub_part in sub_parts {
                            let buckets = do_shuffle_task_core(sub_part, &aggregator, &partitioner, num_output_splits);
                            let buckets_enc = buckets.into_iter().map(|bucket| batch_encrypt(&bucket, false)).collect::<Vec<_>>();
                            merge_enc(&mut acc, &buckets_enc);
                        }
                        acc
                    }).unwrap();
                handlers_res.push(handler);
            } else {
                for sub_part in sub_parts {
                    buckets_col.push(do_shuffle_task_core(sub_part, &aggregator, &partitioner, num_output_splits));
                }
                //launch enc
                let handler = thread::Builder::new()
                    .spawn(move || {
                        //acc stays outside enclave
                        let mut acc = create_enc();
                        for buckets in buckets_col {
                            let buckets_enc = buckets.into_iter().map(|bucket| batch_encrypt(&bucket, false)).collect::<Vec<_>>();
                            merge_enc(&mut acc, &buckets_enc);
                        }
                        acc
                    }).unwrap();
                handlers_res.push(handler);
            }
        }

        let mut acc = create_enc();
        for handler in handlers_res {
            combine_enc(&mut acc, handler.join().unwrap());
        }
        to_ptr(acc)
    }

    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8){
        let mut idx = Idx::new();
        let buckets_enc = unsafe { Box::from_raw(p_data_enc as *mut Vec<Vec<ItemE>>) };
        buckets_enc.send(buf, &mut idx);
        forget(buckets_enc);
    }
    
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8) {
        let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Vec<ItemE>>) };
        let buckets_enc = unsafe { Box::from_raw(p_data_enc as *mut Vec<Vec<ItemE>>) };
        v_out.clone_in_place(&buckets_enc);
        forget(v_out);
        //and free encrypted buckets
    }

    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool) {
        assert!(is_enc);
        crate::ALLOCATOR.set_switch(true);
        let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<Vec<ItemE>>>) };
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
        )) as Arc<dyn ShuffleDependencyTrait>
    }

}

pub fn do_shuffle_task_core<K, V, C>(mut data: Vec<(K, V)>, aggregator: &Arc<Aggregator<K, V, C>>, partitioner: &Box<dyn Partitioner>, num_output_splits: usize) -> Vec<Vec<(K, C)>>
where 
    K: Ord + Data,
    V: Data,
    C: Data,
{
    if aggregator.is_default {
        let mut buckets: Vec<Vec<(K, Vec<V>)>> = (0..num_output_splits)
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let mut iter = data.into_iter();
        if let Some((k, v)) = iter.next() {
            let mut bucket_id = partitioner.get_partition(&k);
            buckets[bucket_id].push((k, vec![v]));
            let mut last_k = &buckets[bucket_id][0].0;
            for (k, v) in iter {
                if last_k == &k {
                    drop(last_k);
                    buckets[bucket_id].last_mut().unwrap().1.push(v);
                    last_k = &buckets[bucket_id].last().unwrap().0;
                } else {
                    drop(last_k);
                    bucket_id = partitioner.get_partition(&k);
                    buckets[bucket_id].push((k, vec![v]));
                    last_k = &buckets[bucket_id].last().unwrap().0;
                }
            }
        }
        *(Box::new(buckets) as Box<dyn Any>).downcast::<Vec<Vec<(K, C)>>>().unwrap()
    } else {
        let mut buckets: Vec<Vec<(K, C)>> = (0..num_output_splits)
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for (k, v) in data.into_iter() {
            let bucket_id = partitioner.get_partition(&k);
            let bucket = &mut buckets[bucket_id];
            if bucket.last().map_or(false, |(last_k, _)| &k == last_k) {
                let old_v = &mut bucket.last_mut().unwrap().1;
                let input = ((std::mem::take(old_v), v),);
                *old_v = aggregator.merge_value.call(input);
            } else {
                bucket.push((k, aggregator.create_combiner.call((v,))));
            }
        }
        buckets
    }
}