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

    fn do_shuffle_task(&self, tid: u64, opb: Arc<dyn OpBase>, mut call_seq: NextOpId, input: Input) -> *mut u8 {
        let op = opb.to_arc_op::<dyn Op<Item = (K, V)>>().unwrap();
        let handler_map: Arc<RwLock<HashMap<ThreadId, SgxThread>>> = Arc::new(RwLock::new(HashMap::new()));
        let pending_thread: Arc<Mutex<VecDeque<ThreadId>>> = Arc::new(Mutex::new(VecDeque::new()));

        let mut sample_data = Vec::new();
        //run the first block for profiling
        call_seq.is_step_para = {
            let mut call_seq = call_seq.clone();
            assert!(call_seq.para_info.is_none());
            sample_data = op.compute(&mut call_seq, input)
                .collect::<Vec<_>>()
                .remove(0)
                .map(|(k, v)| (Some(k), v))
                .collect::<Vec<_>>();
            let sample_len = sample_data.len() as f64;
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = alloc_cnt as f64/sample_len;
            println!("for narrow processing, alloc_cnt per len = {:?}", alloc_cnt_ratio);
            call_seq.is_step_para.1 = alloc_cnt_ratio < 0.1;
            //shuffle specific
            crate::ALLOCATOR.reset_alloc_cnt();
            sample_data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = alloc_cnt as f64/sample_len;
            println!("for shuffle processing, alloc_cnt per len = {:?}", alloc_cnt_ratio);
            call_seq.is_step_para.2 = alloc_cnt_ratio < 0.1;            
            call_seq.is_step_para
        };
        let mut max_len = sample_data.len();

        let mut handlers = Vec::with_capacity(MAX_THREAD);
        let narrow_barrier = Arc::new(Barrier::new(MAX_THREAD));
        let shuffle_barrier = Arc::new(Barrier::new(MAX_THREAD));

        for i in 0..MAX_THREAD {
            let op = op.clone();
            let handler_map = handler_map.clone();
            let pending_thread = pending_thread.clone();
            let narrow_barrier = narrow_barrier.clone();
            let shuffle_barrier = shuffle_barrier.clone();
            let mut call_seq = call_seq.clone();
            call_seq.para_info = Some((i, narrow_barrier, handler_map.clone(), pending_thread.clone()));
            let (_, is_para_nar, is_para_shuf) = call_seq.is_step_para;
            
            let builder = thread::Builder::new();
            let handler = builder
                .spawn(move || {
                    let cur_thread = thread::current();
                    let cur_thread_id = cur_thread.id();
                    handler_map.write().unwrap().insert(cur_thread_id, cur_thread);
                    //shuffle specific
                    let mut results: Vec<Vec<(Option<K>, V)>> = Vec::new();
                    let mut sub_part_size = 0;
                    let result_iter = op.compute(&mut call_seq, input);
                    for result_bl in result_iter {
                        let mut result_bl = result_bl.map(|(k, v)| (Some(k), v)).collect::<Vec<_>>();
                        let result_bl_size = result_bl.deep_size_of();
                        let mut should_create_new = true;
                        if let Some(sub_part) = results.last_mut() {
                            if sub_part_size + result_bl_size <= CACHE_LIMIT/MAX_THREAD/input.get_parallel() {
                                sub_part.append(&mut result_bl);
                                sub_part_size += result_bl_size;
                                should_create_new = false;
                            } 
                        } 
                        if should_create_new {
                            results.push(result_bl);
                            sub_part_size = result_bl_size;
                        }
                    }

                    if !is_para_nar && is_para_shuf {
                        release_bottleneck_lock(&handler_map, &pending_thread);
                        barrier_seq2par(&handler_map, &pending_thread);
                        println!("thread {:?} begin shuffle", cur_thread_id);
                    } else if is_para_nar && !is_para_shuf {
                        shuffle_barrier.wait();
                        require_bottleneck_lock(&pending_thread);
                    }

                    //shuffle specific
                    let mut local_max_len = 0;
                    for sub_part in &mut results {
                        sub_part.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                        local_max_len = std::cmp::max(local_max_len, sub_part.len()); 
                    }

                    if !is_para_shuf {
                        release_bottleneck_lock(&handler_map, &pending_thread);
                        barrier_seq2par(&handler_map, &pending_thread);
                        println!("thread {:?} begin encryption", cur_thread_id);
                    }

                    (results, local_max_len)
                }).unwrap();
            handlers.push(handler);
        }

        let mut sub_parts = handlers.into_iter().map(|handler| {
            let (res, len) = handler.join().unwrap();
            max_len = std::cmp::max(max_len, len);
            res.into_iter()
        }).flatten().collect::<Vec<_>>();
        sub_parts.push(sample_data);

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