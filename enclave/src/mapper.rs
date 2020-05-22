use std::marker::PhantomData;
use std::sync::{Arc, Weak};
use std::boxed::Box;
use std::vec::Vec;
use crate::common::{Common, Context};
use crate::basic::{Data};


pub struct Mapper<T: Data, U: Data, F>
where
    F: Fn(T) -> U + Clone,
{
    context: Weak<Context>,
    id: usize,
    prev: Arc<dyn Common<Item = T>>,
    f: F,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F> Clone for Mapper<T, U, F>
where
    F: Fn(T) -> U + Clone,
{
    fn clone(&self) -> Self {
        Mapper {
            context: self.context.clone(),
            id: self.id,
            prev: self.prev.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> Mapper<T, U, F>
where
    F: Fn(T) -> U + Clone,
{
    pub(crate) fn new(prev: Arc<dyn Common<Item = T>>, f: F) -> Self {
        let sc = prev.get_context();
        Mapper {
            context: Arc::downgrade(&sc),
            id: sc.new_op_id(),
            prev,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> Common for Mapper<T, U, F>
where
    F: Fn(T) -> U + Send + Sync + Clone + 'static,
{
    type Item = U;
    fn get_id(&self) -> usize {
        self.id
    }
    
    fn get_op(&self) -> Arc<dyn Common<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_context(&self) -> Arc<Context> {
        self.context.upgrade().unwrap()
    }

    fn compute_by_id(&self, ser_data: &[u8], id: usize) -> Vec<u8> {
        if id == self.id {
            let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
            let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
            ser_result
        }
        else if id < self.id {
            self.prev.compute_by_id(ser_data, id)
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        Box::new(self.prev.compute(ser_data).map(self.f.clone()))
    }

}
