use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, SgxMutex,
};
use std::boxed::Box;
use std::vec::Vec;
use crate::basic::{AnyData, Data, Arc as SerArc};
use crate::dependency::Dependency;
use crate::mapper::Mapper;
use crate::parallel_collection::ParallelCollection;

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

    pub fn make_op<T: Data>(self: &Arc<Self>) -> SerArc<dyn Common<Item = T>> {
        SerArc::new(ParallelCollection::new(self.clone()))
    }
}

impl<I: Common + ?Sized> Common for SerArc<I> {
    type Item = I::Item;
    fn get_id(&self) -> usize {
        (**self).get_id()
    }
    fn get_op(&self) -> Arc<dyn Common<Item = Self::Item>> {
        (**self).get_op()
    }    
    fn get_context(&self) -> Arc<Context> {
        (**self).get_context()
    }
    fn get_dependencies(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        (**self).get_dependencies()
    }
    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        (**self).compute_by_id(ser_data, ser_data_idx, id, is_shuffle)
    }
    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        (**self).compute(ser_data)
    }
}

pub trait Common: Send + Sync + 'static {
    type Item: Data;
    fn get_id(&self) -> usize;
    fn get_op(&self) -> Arc<dyn Common<Item = Self::Item>>;
    fn get_context(&self) -> Arc<Context>;
    fn get_dependencies(&self) -> Arc<SgxMutex<Vec<Dependency>>>;
    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>);
    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>>;

    fn map<U: Data, F>(&self, f: F) -> SerArc<dyn Common<Item = U>>
    where
        F: Fn(Self::Item) -> U + Send + Sync + Clone + 'static,
        Self: Sized,
    {
        SerArc::new(Mapper::new(self.get_op(), f))
    }

}



