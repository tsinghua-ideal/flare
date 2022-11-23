
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::any::Any;
use std::marker::PhantomData;
use std::vec::Vec;
use crate::basic::Data;
use crate::op::{ItemE, batch_decrypt};
use downcast_rs::Downcast;

pub trait Partitioner: Downcast + dyn_clone::DynClone + Send + Sync {
    fn equals(&self, other: &dyn Any) -> bool;
    fn get_num_of_partitions(&self) -> usize; 
    fn get_range_bound_src(&self) -> Vec<ItemE>;
    fn get_partition(&self, key: &dyn Any) -> usize;
    fn set_num_of_partitions(&mut self, partitions: usize);
    fn set_range_bound_src(&mut self, range_bound_src: Vec<ItemE>);
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
    fn get_range_bound_src(&self) -> Vec<ItemE> {
        Vec::new()
    }
    fn get_partition(&self, key: &dyn Any) -> usize {
        let key = key.downcast_ref::<K>().unwrap();
        hash(key) as usize % self.partitions
    }
    fn set_num_of_partitions(&mut self, partitions: usize) {
        self.partitions = partitions;
    }
    fn set_range_bound_src(&mut self, range_bound_src: Vec<ItemE>) {

    }

}


#[derive(Clone)]
pub struct RangePartitioner<K: Data + Eq + Ord> {
    ascending: bool,
    partitions: usize,
    range_bounds: Vec<K>,
    samples_enc: Vec<ItemE>,
}

impl<K: Data + Eq + Ord> RangePartitioner<K> {
    pub fn new(
        partitions: usize,
        ascending: bool,
        samples: Vec<K>,
        samples_enc: Vec<ItemE>,
    ) -> Self {
        RangePartitioner {
            ascending,
            partitions,
            range_bounds: Vec::new(),
            samples_enc,
        }
    }
}

impl<K: Data + Eq + Ord> Partitioner for RangePartitioner<K> {
    fn equals(&self, other: &dyn Any) -> bool {
        if let Some(rp) = other.downcast_ref::<RangePartitioner<K>>() {
            return self.partitions == rp.partitions
                && self.ascending == rp.ascending
                && self.range_bounds == rp.range_bounds
                && self.samples_enc == rp.samples_enc;
        } else {
            return false;
        }
    }

    fn get_num_of_partitions(&self) -> usize {
        self.partitions
    }

    fn get_range_bound_src(&self) -> Vec<ItemE> {
        self.samples_enc.clone()
    }

    fn get_partition(&self, key: &dyn Any) -> usize {
        let key = key.downcast_ref::<K>().unwrap();

        if self.partitions <= 1 {
            return 0;
        }

        return match self.range_bounds.binary_search(key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
    }

    fn set_num_of_partitions(&mut self, partitions: usize) {
        self.partitions = partitions;
    }

    fn set_range_bound_src(&mut self, range_bound_src: Vec<ItemE>) {
        self.samples_enc = range_bound_src;
        let mut samples: Vec<K> = batch_decrypt(&self.samples_enc, false);
        let mut range_bounds = Vec::new();

        if self.partitions > 1 {
            samples.sort();

            let step: f64 = samples.len() as f64 / (self.partitions - 1) as f64;
            let mut i: f64 = 0.0;

            for idx in 0..(self.partitions - 1) {
                range_bounds
                    .push(samples[std::cmp::min((i + step) as usize, samples.len() - 1)].clone());
                i += step;
            }
        }
        self.range_bounds = range_bounds;
    }
}