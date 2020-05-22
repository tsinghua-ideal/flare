use std::marker::PhantomData;
use std::sync::{Arc, Weak};
use std::boxed::Box;
use std::vec::Vec;
use crate::common::{Common, Context};
use crate::basic::Data;

pub struct ParallelCollection<T> {
    context: Weak<Context>,
    id: usize,
    _marker_t: PhantomData<T>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            context: self.context.clone(),
            id: self.id,
            _marker_t: PhantomData,
        }
    }
} 

impl<T: Data> ParallelCollection<T> {
    pub fn new(context: Arc<Context>) -> Self {
        ParallelCollection {
            context: Arc::downgrade(&context),
            id: context.new_op_id(),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data> Common for ParallelCollection<T> {
    type Item = T;
    fn get_id(&self) -> usize {
        self.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.context.upgrade().unwrap()
    }

    fn get_op(&self) -> Arc<dyn Common<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute_by_id(&self, ser_data: &[u8], id: usize) -> Vec<u8> {
        if id == self.id {
            let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
            let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
            ser_result
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();
        Box::new(data.into_iter())
    }

}
