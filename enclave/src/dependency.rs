use std::any::Any;
use std::boxed::Box;
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, SgxRwLock as RwLock, atomic::{self, AtomicBool}};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func};
use crate::op::{res_enc_to_ptr, to_ptr, ser_encrypt, ser_decrypt, batch_encrypt, batch_decrypt, create_enc, combine_enc, ItemE, CACHE_LIMIT, Input, OpId, SortHelper, column_sort_step_2, merge_enc};
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
    fn do_shuffle_task(&self, tid: u64, iter: Box<dyn Iterator<Item = Box<dyn Any>>>, parallel_num: usize) -> *mut u8;
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

    fn do_shuffle_task(&self, tid: u64, iter: Box<dyn Iterator<Item = Box<dyn Any>>>, parallel_num: usize) -> *mut u8 {
        let aggregator = self.aggregator.clone();
        //sub partition sort, for each vector in sub_parts, it is sorted
        let mut sub_part = Vec::new();
        let mut sub_parts = Vec::new();

        let mut max_len = 0;
        
        for block in iter {
            let mut block = block.downcast::<Vec<(K, V)>>()
                .unwrap().into_iter().map(|(k, v)| (Some(k), v)).collect::<Vec<_>>();
            sub_part.append(&mut block);

            if sub_part.get_size() > CACHE_LIMIT/parallel_num {
                sub_part.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                max_len = std::cmp::max(max_len, sub_part.len());
                sub_parts.push(sub_part);
                sub_part = Vec::new();
            }
        }
        
        if !sub_part.is_empty() {
            sub_part.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            max_len = std::cmp::max(max_len, sub_part.len());
            sub_parts.push(sub_part);
        }

        //partition sort (local sort), and pad so that each sub partition should have the same number of (K, V)
        let mut sort_helper = SortHelper::<K, V>::new(sub_parts, max_len, true);
        sort_helper.sort();
        let (sorted_data, num_real_elem) = sort_helper.take();
        let num_output_splits = self.partitioner.read().unwrap().get_num_of_partitions();
        let chunk_size = num_real_elem.saturating_sub(1) / num_output_splits + 1;
        let buckets_enc = if self.is_cogroup {
            let mut buckets_enc = create_enc();
            for bucket in sorted_data.chunks(chunk_size) {
                merge_enc(&mut buckets_enc, &batch_encrypt(bucket, false));
            }
            buckets_enc
        } else {
            //TODO: 
            column_sort_step_2::<(Option<K>, V)>(tid, sorted_data, max_len, num_output_splits)
        };
        to_ptr(buckets_enc)
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
        let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<ItemE>>) };
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
