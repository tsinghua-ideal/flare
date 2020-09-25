use std::boxed::Box;
use std::marker::PhantomData;
use std::mem::forget;
use std::sync::{Arc, SgxMutex, Weak};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::op::{Context, Op, OpBase, OpVals};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::basic::{AnyData, Data};

pub struct FlatMapper<T: Data, U: Data, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F> Clone for FlatMapper<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapper {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> FlatMapper<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F) -> Self {
        let mut vals = OpVals::new(prev.get_op_base().get_context());
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id()); 
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_ids.clone())
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        );
        FlatMapper {
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            prev,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> OpBase for FlatMapper<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Send + Sync + Clone + 'static,
{
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

impl<T: Data, U: Data, F> Op for FlatMapper<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Send + Sync + Clone + 'static,
{ 
    type Item = U;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        let next_deps = self.next_deps.lock().unwrap();
        match is_shuffle == 0 {
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
        Box::new(self.prev.compute(data_ptr).flat_map(self.f.clone()))
    }

}
