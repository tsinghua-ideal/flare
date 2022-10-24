use std::hash::{Hash, Hasher};
use std::any::Any;
use std::marker::PhantomData;
use crate::basic::Data;
use downcast_rs::Downcast;
use seahash::SeaHasher;

pub trait Partitioner: Downcast + dyn_clone::DynClone + Send + Sync {
    fn equals(&self, other: &dyn Any) -> bool;
    fn get_num_of_partitions(&self) -> usize; 
    fn get_partition(&self, key: &dyn Any) -> usize;
    fn get_partition_with_seed(&self, key: &dyn Any, seed: (u64, u64, u64, u64)) -> usize;
    fn set_num_of_partitions(&mut self, partitions: usize);
}

dyn_clone::clone_trait_object!(Partitioner);

pub(crate) fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = SeaHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn hash_with_seed<T: Hash>(t: &T, seed: (u64, u64, u64, u64)) -> u64 {
    let mut s = SeaHasher::with_seeds(seed.0, seed.1, seed.2, seed.3);
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
    fn get_partition_with_seed(&self, key: &dyn Any, seed: (u64, u64, u64, u64)) -> usize {
        let key = key.downcast_ref::<K>().unwrap();
        hash_with_seed(key, seed) as usize % self.partitions
    }
    fn set_num_of_partitions(&mut self, partitions: usize) {
        self.partitions = partitions;
    }
}
