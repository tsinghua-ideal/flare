use std::boxed::Box;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::common::*;
use crate::basic::{Arc as SerArc, Data};
use crate::shuffled::Shuffled;

pub trait Pair<K: Data + Eq + Hash, V: Data>: Common<Item = (K, V)> + Send + Sync {
    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Common<Item = (K, C)>>
    where
        Self: Sized + 'static,
    {
        SerArc::new(Shuffled::new(
            self.get_op(),
            Arc::new(aggregator),
            partitioner,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn Common<Item = (K, Vec<V>)>>
    where
        Self: Sized + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Common<Item = (K, Vec<V>)>>
    where
        Self: Sized + 'static,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }


}

// Implementing the PairRdd trait for all types which implements Rdd
impl<K: Data + Eq + Hash, V: Data, T: Common<Item = (K, V)>> Pair<K, V> for T  {}
