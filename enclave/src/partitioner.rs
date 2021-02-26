use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::any::Any;
use std::marker::PhantomData;
use crate::basic::Data;
use downcast_rs::Downcast;

pub trait Partitioner: Downcast + dyn_clone::DynClone + Send + Sync {
    fn equals(&self, other: &dyn Any) -> bool;
    fn get_num_of_partitions(&self) -> usize; 
    fn get_partition(&self, key: &dyn Any) -> usize;
    fn set_num_of_partitions(&mut self, partitions: usize);
}

dyn_clone::clone_trait_object!(Partitioner);

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[derive(Clone)]
pub struct HashPartitioner<K: Data + Hash + Eq> {
    partitions: usize,
    _marker: PhantomData<K>,
}

// Hash partitioner implementing naive hash function.
impl<K: Data + Hash + Eq> HashPartitioner<K> {
    pub fn new(partitions: usize) -> Self {
        HashPartitioner {
            partitions,
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Hash + Eq> Partitioner for HashPartitioner<K> {
    fn equals(&self, other: &dyn Any) -> bool {
        if let Some(hp) = other.downcast_ref::<HashPartitioner<K>>() {
            self.partitions == hp.partitions
        } else {
            false
        }
    }
    fn get_num_of_partitions(&self) -> usize {
        self.partitions
    }
    fn get_partition(&self, key: &dyn Any) -> usize {
        let key = key.downcast_ref::<K>().unwrap();
        hash(key) as usize % self.partitions
    }
    fn set_num_of_partitions(&mut self, partitions: usize) {
        self.partitions = partitions;
    }
}
