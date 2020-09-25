use std::cmp;
use std::boxed::Box;
use std::marker::PhantomData;
use std::mem::{drop, forget};
use std::sync::{Arc, SgxMutex, Weak};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::op::{Context, Op, OpBase, OpVals};
use crate::basic::{AnyData, Data};
use crate::dependency::{Dependency};

pub struct ParallelCollection<T> {
    vals: Arc<OpVals>, 
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    _marker_t: PhantomData<T>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            _marker_t: PhantomData,
        }
    }
} 

impl<T: Data> ParallelCollection<T> {
    pub fn new(context: Arc<Context>) -> Self {
        let vals = OpVals::new(context.clone());
        ParallelCollection {
            vals: Arc::new(vals),
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data> OpBase for ParallelCollection<T> {
    fn get_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }

    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        self.next_deps.clone()
    }

    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }
}

impl<T: Data> Op for ParallelCollection<T> {
    type Item = T;

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        let next_deps = self.next_deps.lock().unwrap();
        match is_shuffle == 0 {  //TODO maybe need to check if shuf_dep not in dep
            true => {       //No shuffle later
                let result = self.compute(data_ptr)
                    .collect::<Vec<Self::Item>>();
                crate::ALLOCATOR.lock().set_switch(true);
                let result_enc = result.clone(); // encrypt
                let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
                crate::ALLOCATOR.lock().set_switch(false);
                result_ptr
            },
            false => {      //Shuffle later
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) }; 
                let data = data_enc.clone();
                crate::ALLOCATOR.lock().set_switch(true);
                drop(data_enc);
                crate::ALLOCATOR.lock().set_switch(false);
                let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.do_shuffle_task(iter)
            },
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let now = Instant::now();
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) }; 
        //TODO decrypt 
        let data = data_enc.clone();
        forget(data_enc);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave decrypt {:?} s", dur);    
        Box::new(data.into_iter())
    }

}
