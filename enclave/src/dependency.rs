use std::boxed::Box;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data};
use crate::downcast_rs::DowncastSync;
use crate::partitioner::Partitioner;

#[derive(Clone)]
pub enum Dependency {
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

impl<K: Data + Eq + Hash, V: Data, C: Data> From<ShuffleDependency<K, V, C>> for Dependency {
    fn from(shuf_dep: ShuffleDependency<K, V, C>) -> Self {
        Dependency::ShuffleDependency(Arc::new(shuf_dep) as Arc<dyn ShuffleDependencyTrait>)
    }
}

pub trait NarrowDependencyTrait: DowncastSync + Send + Sync {}
crate::impl_downcast!(sync NarrowDependencyTrait);

#[derive(Clone)]
pub struct OneToOneDependency {}

impl OneToOneDependency {
    pub fn new() -> Self {
        OneToOneDependency {}
    }
}

impl NarrowDependencyTrait for OneToOneDependency {}

#[derive(Clone)]
pub struct RangeDependency {
    in_start: usize,
    out_start: usize,
    length: usize,
}

impl RangeDependency {
    pub fn new(in_start: usize, out_start: usize, length: usize) -> Self {
        RangeDependency { in_start, out_start, length, }
    }
}

impl NarrowDependencyTrait for RangeDependency {}

pub trait ShuffleDependencyTrait: DowncastSync + Send + Sync  { 
    fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>) -> Vec<Vec<u8>>;

}
crate::impl_downcast!(sync ShuffleDependencyTrait);

pub struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub is_cogroup: bool,
    pub aggregator: Arc<Aggregator<K, V, C>>,
    pub partitioner: Box<dyn Partitioner>,
}

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependency<K, V, C> {
    pub fn new(
        is_cogroup: bool,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
    ) -> Self {
        ShuffleDependency {
            is_cogroup,
            aggregator,
            partitioner,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependencyTrait for ShuffleDependency<K, V, C> {
    fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>) -> Vec<Vec<u8>>{
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<HashMap<K, C>> = (0..num_output_splits)
            .map(|_| HashMap::new())
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
        let mut ser_result: Vec<Vec<u8>> = Vec::with_capacity(num_output_splits);
        for bucket in buckets.into_iter() {
            ser_result.push(bincode::serialize(&bucket).unwrap());
        }
        ser_result
    }

}

