use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, SgxMutex, Weak,
};
use std::boxed::Box;
use std::collections::HashSet;
use std::vec::Vec;
use crate::basic::{AnyData, Data, Arc as SerArc};
use crate::dependency::Dependency;
use crate::partitioner::Partitioner;
mod parallel_collection_op;
pub use parallel_collection_op::*;
mod co_grouped_op;
pub use co_grouped_op::*;
mod flatmapper_op;
pub use flatmapper_op::*;
mod mapper_op;
pub use mapper_op::*;
mod pair_op;
pub use pair_op::*;
mod reduced_op;
pub use reduced_op::*;
mod shuffled_op;
pub use shuffled_op::*;

#[derive(Default)]
pub struct Context {
    next_op_id: Arc<AtomicUsize>,
}

impl Context {
    pub fn new() -> Arc<Self> {
        Arc::new(Context {
            next_op_id: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn new_op_id(self: &Arc<Self>) -> usize {
        self.next_op_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn make_op<T: Data>(self: &Arc<Self>) -> SerArc<dyn Op<Item = T>> {
        SerArc::new(ParallelCollection::new(self.clone()))
    }
}

pub(crate) struct OpVals {
    pub id: usize,
    pub deps: Vec<Dependency>,
    pub context: Weak<Context>,
}

impl OpVals {
    pub fn new(sc: Arc<Context>) -> Self {
        OpVals {
            id: sc.new_op_id(),
            deps: Vec::new(),
            context: Arc::downgrade(&sc),
        }
    }
}

impl<I: Op + ?Sized> Op for SerArc<I> {
    type Item = I::Item;
    fn get_id(&self) -> usize {
        (**self).get_id()
    }
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        (**self).get_op()
    }    
    fn get_context(&self) -> Arc<Context> {
        (**self).get_context()
    }
    fn get_deps(&self) -> Vec<Dependency> {
        (**self).get_deps()
    }
    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        (**self).get_next_deps()
    }
    fn get_prev_ids(&self) -> HashSet<usize> {
        (**self).get_prev_ids()
    }
    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        (**self).compute_by_id(ser_data, ser_data_idx, id, is_shuffle)
    }
    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        (**self).compute(ser_data, ser_data_idx)
    }
}

pub trait Op: Send + Sync + 'static {
    type Item: Data;
    fn get_id(&self) -> usize;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_context(&self) -> Arc<Context>;
    fn get_deps(&self) -> Vec<Dependency>;
    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>>;
    fn get_prev_ids(&self) -> HashSet<usize> {
        let deps = self.get_deps();
        let mut set = HashSet::new();
        for dep in deps {
            for i in dep.get_prev_ids() {
                set.insert(i);
            }  
        };
        set
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>);
    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>>;

    /// Return a new RDD containing only the elements that satisfy a predicate.
    /*
    fn filter<F>(&self, predicate: F) -> SerArc<dyn Op<Item = Self::Item>>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + Clone + Copy + 'static,
        Self: Sized,
    {
        let filter_fn = Fn!(move |_index: usize, 
                                  items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> {
            Box::new(items.filter(predicate))
        });
        SerArc::new(MapPartitions::new(self.get_op(), filter_fn))
    }
    */

    fn map<U: Data, F>(&self, f: F) -> SerArc<dyn Op<Item = U>>
    where
        F: Fn(Self::Item) -> U + Send + Sync + Clone + 'static,
        Self: Sized,
    {
        SerArc::new(Mapper::new(self.get_op(), f))
    }

    fn flat_map<U: Data, F>(&self, f: F) -> SerArc<dyn Op<Item = U>>
    where
        F: Fn(Self::Item) -> Box<dyn Iterator<Item = U>> + Send + Sync + Clone + 'static,
        Self: Sized,
    {
        SerArc::new(FlatMapper::new(self.get_op(), f))
    }

    fn reduce<F>(&self, f: F) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync + Clone + 'static,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();        
        let reduce_partition = move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc { 
                None => vec![],
                Some(e) => vec![e],
            }
        };         
        SerArc::new(Reduced::new(self.get_op(), reduce_partition)) 
    }

}


pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T;
}

impl<T, I> Reduce<T> for I
where
    I: Iterator<Item = T>,
{
    #[inline]
    fn reduce<F>(mut self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T,
    {
        self.next().map(|first| self.fold(first, f))
    }
}
