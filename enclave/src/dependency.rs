use std::boxed::Box;
use std::collections::{HashMap, HashSet};
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

impl Dependency {
    pub fn get_prev_ids(&self) -> HashSet<usize> {
        match self {
            Dependency::NarrowDependency(nar) => nar.get_prev_ids(),
            Dependency::ShuffleDependency(shuf) => shuf.get_prev_ids(),
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> From<ShuffleDependency<K, V, C>> for Dependency {
    fn from(shuf_dep: ShuffleDependency<K, V, C>) -> Self {
        Dependency::ShuffleDependency(Arc::new(shuf_dep) as Arc<dyn ShuffleDependencyTrait>)
    }
}

pub trait NarrowDependencyTrait: DowncastSync + Send + Sync {
    fn get_prev_ids(&self) -> HashSet<usize>;
}
crate::impl_downcast!(sync NarrowDependencyTrait);

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
    fn get_prev_ids(&self) -> HashSet<usize>;
}
crate::impl_downcast!(sync ShuffleDependencyTrait);

pub struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub is_cogroup: bool,
    pub aggregator: Arc<Aggregator<K, V, C>>,
    pub partitioner: Box<dyn Partitioner>,
    prev_ids: HashSet<usize>,
}

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependency<K, V, C> {
    pub fn new(
        is_cogroup: bool,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
        prev_ids: HashSet<usize>,
    ) -> Self {
        ShuffleDependency {
            is_cogroup,
            aggregator,
            partitioner,
            prev_ids,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependencyTrait for ShuffleDependency<K, V, C> {
    fn do_shuffle_task(&self, iter: Box<dyn Iterator<Item = Box<dyn AnyData>>>) -> *mut u8 {
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<HashMap<K, C>> = (0..num_output_splits)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>();
        /*
        let mut buckets: Vec<HashMap<K, Vec<Box<dyn AnyData>>>> = (0..num_output_splits)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>();
        */
        /*
        let create_combiner = Box::new(|v: Box<dyn AnyData>| vec![v]);
        fn merge_value(
            mut buf: Vec<Box<dyn AnyData>>,
            v: Box<dyn AnyData>,
        ) -> Vec<Box<dyn AnyData>> {
            buf.push(v);
            buf
        }
        let merge_value = Box::new(|(buf, v)| merge_value(buf, v));
        fn merge_combiners(
            mut b1: Vec<Box<dyn AnyData>>,
            mut b2: Vec<Box<dyn AnyData>>,
        ) -> Vec<Box<dyn AnyData>> {
            b1.append(&mut b2);
            b1
        }
        let merge_combiners = Box::new(|(b1, b2)| merge_combiners(b1, b2));
        */

        for (count, i) in iter.enumerate() {
            let b = i.into_any().downcast::<(K, V)>().unwrap();
            let (k, v) = *b;
            //let (k, v_) = *b;
            //let v = Box::new(v_) as Box<dyn AnyData>;
            let bucket_id = partitioner.get_partition(&k);
            let bucket = &mut buckets[bucket_id];
            if let Some(old_v) = bucket.get_mut(&k) {
                let input = ((old_v.clone(), v),);
                let output = aggregator.merge_value.call(input);
                //let output = merge_value.call(input);
                *old_v = output;
            } else {
                bucket.insert(k, aggregator.create_combiner.call((v,)));
                //bucket.insert(k, create_combiner.call((v,)));
            }
        }
        let result = buckets.into_iter()
            .map(|bucket| bucket.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();  //HashMap to Vec
        println!("result = {:?}", result);
        /*
        let r = buckets_c.into_iter()
            .map(|bucket| bucket.into_iter()
                 .map(|(k, c)| (k, c.into_iter().map(|v| Box::new(v) as Box<dyn AnyData>).collect::<Vec<_>>()))
                 .collect::<Vec<_>>())
            .collect::<Vec<_>>();
        */
        crate::ALLOCATOR.lock().set_switch(true);
        let result_enc = result.clone();
        let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
        crate::ALLOCATOR.lock().set_switch(false);
        result_ptr
    }
    
    fn get_prev_ids(&self) -> HashSet<usize> {
        self.prev_ids.clone()
    }

}

