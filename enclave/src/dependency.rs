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
use crate::op::{res_enc_to_ptr, MAX_ENC_BL};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use downcast_rs::DowncastSync;


#[derive(Clone)]
pub enum Dependency {
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

impl Dependency {
    pub fn get_prev_ids(&self) -> HashSet<usize> {
        match self {
            Dependency::NarrowDependency(nar) => nar.get_prev_ids(),
            Dependency::ShuffleDependency(shuf) => shuf.get_prev_ids(),
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
    fn get_prev_ids(&self) -> HashSet<usize>;
}
impl_downcast!(sync NarrowDependencyTrait);

#[derive(Clone)]
pub struct OneToOneDependency {
    prev_ids: HashSet<usize>, 
}

impl OneToOneDependency {
    pub fn new(prev_ids: HashSet<usize>) -> Self {
        OneToOneDependency{ prev_ids }
    }
}

impl NarrowDependencyTrait for OneToOneDependency {
    fn get_prev_ids(&self) -> HashSet<usize> {
        self.prev_ids.clone()
    }
}

#[derive(Clone)]
pub struct RangeDependency {
    in_start: usize,
    out_start: usize,
    length: usize,
    prev_ids: HashSet<usize>,
}

impl RangeDependency {
    pub fn new(in_start: usize, out_start: usize, length: usize, prev_ids: HashSet<usize>) -> Self {
        RangeDependency { in_start, out_start, length, prev_ids}
    }
}

impl NarrowDependencyTrait for RangeDependency {
    fn get_prev_ids(&self) -> HashSet<usize> {
        self.prev_ids.clone()
    }
}

pub trait ShuffleDependencyTrait: DowncastSync + Send + Sync  { 
    fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>) -> *mut u8;
    fn send_sketch(&self, buf: &mut SizeBuf, p_data_enc: *mut u8);
    fn send_enc_data(&self, p_out: usize, p_data_enc: *mut u8);
    fn free_res_enc(&self, res_ptr: *mut u8);
    fn get_prev_ids(&self) -> HashSet<usize>;
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
    prev_ids: HashSet<usize>,
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
        prev_ids: HashSet<usize>,
        fe: Box<dyn Func(Vec<(K, C)>) -> (KE, CE)>,
        fd: Box<dyn Func((KE, CE)) -> Vec<(K, C)>>,
    ) -> Self {
        ShuffleDependency {
            is_cogroup,
            aggregator,
            partitioner,
            prev_ids,
            fe,
            fd,
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
    fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>) -> *mut u8 {
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<BTreeMap<K, C>> = (0..num_output_splits)
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>();

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

        let now = Instant::now();
        let result = buckets.into_iter()
            .map(|bucket| {
                //batch encrypt
                let mut bucket = bucket.into_iter().collect::<Vec<_>>();
                let mut len = bucket.len();
                let mut data_enc = Vec::with_capacity(len);
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

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave encrypt {:?} s", dur); 
        return res_enc_to_ptr(result);
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

    fn get_prev_ids(&self) -> HashSet<usize> {
        self.prev_ids.clone()
    }

}
