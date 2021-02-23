use core::ops;
use std::any::Any;
use std::boxed::Box;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::mem::forget;
use std::sync::Arc;
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func};
use crate::op::{res_enc_to_ptr, MAX_ENC_BL, OpId};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use downcast_rs::DowncastSync;

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
    //fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>, is_spec: bool) -> *mut u8;
    fn do_shuffle_task(&self, iter: Box<dyn Any>, is_spec: bool) -> *mut u8;
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
    pub partitioner: Box<dyn Partitioner>,
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
            partitioner,
            identifier,
            parent,
            child,
            fe,
            fd,
        }
    }

    pub fn encrypt_buckets(&self, buckets: Vec<BTreeMap<K, C>>) -> Vec<Vec<(KE, CE)>> {
        let result = buckets.into_iter()
            .map(|bucket| {
                //batch encrypt
                let mut bucket = bucket.into_iter().collect::<Vec<_>>();
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

    pub fn encrypt_buckets_spec(&self, buckets: Vec<BTreeMap<K, C>>) -> Vec<Vec<u8>> {
        let result = buckets.into_iter()
            .map(|bucket| {
                //batch encrypt
                let mut bucket = bucket.into_iter().collect::<Vec<_>>();
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
    //fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>, is_spec: bool) -> *mut u8 {
    fn do_shuffle_task(&self, iter: Box<dyn Any>, is_spec: bool) -> *mut u8 {
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<BTreeMap<K, C>> = (0..num_output_splits)
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>();

        //println!("cur mem before shuffle write: {:?}", crate::ALLOCATOR.lock().get_memory_usage()); 
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

        //println!("cur mem after shuffle write: {:?}", crate::ALLOCATOR.lock().get_memory_usage());

        if is_spec {
            let now = Instant::now();
            let result = self.encrypt_buckets_spec(buckets);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            //println!("cur mem before copy out: {:?}, encrypt {:?} s", crate::ALLOCATOR.lock().get_memory_usage(), dur); 
            let res_ptr = res_enc_to_ptr(result);
            //println!("cur mem after copy out: {:?}", crate::ALLOCATOR.lock().get_memory_usage()); 
            res_ptr
        } else {
            let now = Instant::now();
            let result = self.encrypt_buckets(buckets);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            //println!("cur mem before copy out: {:?}, encrypt {:?} s", crate::ALLOCATOR.lock().get_memory_usage(), dur); 
            let res_ptr = res_enc_to_ptr(result);
            //println!("cur mem after copy out: {:?}", crate::ALLOCATOR.lock().get_memory_usage()); 
            res_ptr
        }
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
        crate::ALLOCATOR.lock().set_switch(true);
        let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<(KE, CE)>>) };
        drop(res);
        crate::ALLOCATOR.lock().set_switch(false);
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
            self.partitioner.clone(),
            self.identifier,
            parent_op_id,
            child_op_id,
            self.fe.clone(),
            self.fd.clone(),
        )) as Arc<dyn ShuffleDependencyTrait>
    }

}
