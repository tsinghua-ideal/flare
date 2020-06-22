use std::boxed::Box;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex, Weak};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::op::{Context, Op, OpBase, OpVals};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::basic::{AnyData, Data};

pub struct Reduced<T: Data, U: Data, F>
where
    F: Fn(Box<dyn Iterator<Item = T>>) -> Vec<U> + Clone,
{
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F> Clone for Reduced<T, U, F>
where
    F: Fn(Box<dyn Iterator<Item = T>>) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        Reduced {
            prev: self.prev.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> Reduced<T, U, F>
where
    F: Fn(Box<dyn Iterator<Item = T>>) -> Vec<U> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F) -> Self {
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id()); 
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        );
        Reduced {
            prev,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> OpBase for Reduced<T, U, F>
where
    F: Fn(Box<dyn Iterator<Item = T>>) -> Vec<U> + Send + Sync + Clone + 'static,
{
    fn get_id(&self) -> usize {
        self.prev.get_id()
    }
    
    fn get_context(&self) -> Arc<Context> {
        self.prev.get_context()
    }
    
    fn get_deps(&self) -> Vec<Dependency> {
        self.prev.get_deps()
    }
    
    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        self.prev.get_next_deps()
    }

    fn iterator(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        self.compute_start(ser_data, ser_data_idx, is_shuffle)
    }
}

impl<T: Data, U: Data, F> Op for Reduced<T, U, F>
where
    F: Fn(Box<dyn Iterator<Item = T>>) -> Vec<U> + Send + Sync + Clone + 'static,
{
    type Item = U;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>){
        if is_shuffle == 2 {
            let result = self.compute(ser_data, ser_data_idx).collect::<Vec<Self::Item>>();
            let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
            let ser_result_idx: Vec<usize> = vec![ser_result.len()];
            (ser_result, ser_result_idx)
        }
        else {
            self.prev.compute_start(ser_data, ser_data_idx, is_shuffle)
        }
    }

    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        let data: Vec<T> = bincode::deserialize(ser_data).unwrap();
        Box::new((self.f)(Box::new(data.into_iter())).into_iter())        
    }

}
