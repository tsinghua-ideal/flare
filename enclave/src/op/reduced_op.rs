use std::boxed::Box;
use std::marker::PhantomData;
use std::mem::forget;
use std::sync::{Arc, SgxMutex, Weak};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::op::{Context, Op, OpBase};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::basic::{Data};

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

    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
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
  
    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8{
        if is_shuffle == 2 {
            let result = self.compute(data_ptr).collect::<Vec<Self::Item>>();
            crate::ALLOCATOR.lock().set_switch(true);
            let result_enc = result.clone(); // encrypt
            let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8; 
            crate::ALLOCATOR.lock().set_switch(false);
            result_ptr
        }
        else {
            self.prev.compute_start(data_ptr, is_shuffle)
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<T>) };
        let data = data_enc.clone();
        forget(data_enc);
        Box::new((self.f)(Box::new(data.into_iter())).into_iter())        
    }

}
