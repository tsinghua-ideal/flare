use std::any::Any;
use std::boxed::Box;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, Barrier, SgxMutex as Mutex, SgxRwLock as RwLock, atomic::{self, AtomicBool}};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func};
use crate::obliv_comp::{obliv_agg_presort, obliv_agg_stage1, obliv_global_filter_stage1, obliv_global_filter_stage2_kv, zip_data_marks};
use crate::op::*;
use crate::partitioner::{Partitioner, hash};
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
    fn do_shuffle_task(&self, tid: u64, opb: Arc<dyn OpBase>, call_seq: NextOpId, input: Input) -> *mut u8;
    fn do_shuffle_task_rem(&self, tid: u64, call_seq: NextOpId, input: Input) -> *mut u8;
    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8);
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8);
    fn free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo, is_enc: bool);
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
        let seed = hash(&call_seq.get_cur_rdd_id());
        
        let part_id = call_seq.get_part_id();
        let outer_parallel = input.get_parallel();
    
        let partitioner = self.partitioner.read().unwrap().clone();
        let num_splits_out = partitioner.get_num_of_partitions();

        let results = {
            let (ptr, null_ptr) = op.narrow(call_seq, input, false);
            assert_eq!(0usize, null_ptr as usize);
            *unsafe{ Box::from_raw(ptr as *mut Vec<(Vec<(K, V)>, Vec<bool>)>) }
        };
        
        if self.aggregator.is_default {
            let data_iter = zip_data_marks(results);
            let (data, num_invalids) = obliv_global_filter_stage1(data_iter);
            let data_ptr = Box::into_raw(Box::new(data)) as *mut u8 as usize;
            let mut info = create_enc();
            merge_enc(&mut info, &ser_encrypt(&data_ptr));
            merge_enc(&mut info, &ser_encrypt(&num_invalids));
            to_ptr(info)
        } else {
            let sorted_data = obliv_agg_presort(results, outer_parallel);
            let buckets_enc = obliv_agg_stage1(sorted_data, part_id, false, &self.aggregator, &partitioner, seed, outer_parallel);
            to_ptr(buckets_enc)
        }
    }

    fn do_shuffle_task_rem(&self, tid: u64, call_seq: NextOpId, input: Input) -> *mut u8 {
        let part_id = call_seq.get_part_id();
        let outer_parallel = input.get_parallel();
    
        let partitioner = self.partitioner.read().unwrap().clone();
        let num_splits_out = partitioner.get_num_of_partitions();

        assert!(self.aggregator.is_default);
        let (data_ptr, num_invalids) = input.get_enc_data::<(ItemE, Vec<ItemE>)>();
        let data_ptr: usize = ser_decrypt(&data_ptr.clone());
        let nums_invalids = num_invalids.iter().map(|x| ser_decrypt::<usize>(&x.clone())).collect::<Vec<_>>();
        let data = *unsafe {
            Box::from_raw(data_ptr as *mut u8 as *mut Vec<Vec<((K, V), u64)>>)
        };
        let buckets = obliv_global_filter_stage2_kv(data, part_id, num_splits_out, nums_invalids, outer_parallel);
        let mut buckets_enc = create_enc();
        for bucket in buckets {
            merge_enc(&mut buckets_enc, &batch_encrypt(&bucket, false));
        }
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

    fn free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo, is_enc: bool) {
        if self.aggregator.is_default && dep_info.dep_type() == 1 {
            assert!(is_enc);
            crate::ALLOCATOR.set_switch(true);
            let res = unsafe { Box::from_raw(res_ptr as *mut Vec<ItemE>) };
            drop(res);
            crate::ALLOCATOR.set_switch(false);
        } else {
            assert!(is_enc);
            crate::ALLOCATOR.set_switch(true);
            let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<ItemE>>) };
            drop(res);
            crate::ALLOCATOR.set_switch(false);
        }
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