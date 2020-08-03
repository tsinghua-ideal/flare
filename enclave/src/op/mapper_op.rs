use std::boxed::Box;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex, Weak};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::op::{Context, Op, OpBase, OpVals};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::basic::{AnyData, Data};

pub struct Mapper<T: Data, U: Data, F>
where
    F: Fn(T) -> U + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F> Clone for Mapper<T, U, F>
where
    F: Fn(T) -> U + Clone,
{
    fn clone(&self) -> Self {
        Mapper {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
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
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F) -> Self {
        let mut vals = OpVals::new(prev.get_context());
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
        Mapper {
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            prev,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> OpBase for Mapper<T, U, F>
where
    F: Fn(T) -> U + Send + Sync + Clone + 'static,
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
    
    fn iterator(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        self.compute_start(ser_data, ser_data_idx, is_shuffle)
    }
}

impl<T: Data, U: Data, F> Op for Mapper<T, U, F>
where
    F: Fn(T) -> U + Send + Sync + Clone + 'static,
{
    type Item = U;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>){
        assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
        let next_deps = self.next_deps.lock().unwrap();
        match is_shuffle == 0 {
            true => {       //No shuffle later
                let now = Instant::now();
                let result = self.compute(ser_data, ser_data_idx)
                    .collect::<Vec<Self::Item>>();
                println!("result addr = {:?}", result.as_ptr());
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                println!("in enclave compute {:?} s", dur);    
                let now = Instant::now();
                let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                println!("in enclave serialize {:?} s", dur);    
                let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                (ser_result, ser_result_idx)
            },
            false => {      //Shuffle later
                let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();    
                let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
                let shuf_dep = match &next_deps[0] {
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                let ser_result_set = shuf_dep.do_shuffle_task(iter);
                let mut ser_result = Vec::<u8>::with_capacity(std::mem::size_of_val(&ser_result_set));
                let mut ser_result_idx = Vec::<usize>::with_capacity(ser_result.len());
                let mut idx: usize = 0;
                for (i, mut ser_result_bl) in ser_result_set.into_iter().enumerate() {
                    idx += ser_result_bl.len();
                    ser_result.append(&mut ser_result_bl);
                    ser_result_idx[i] = idx;   
                }
                (ser_result, ser_result_idx)
            },
        } 
    }

    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        let now = Instant::now();
        let b = Box::new(self.prev.compute(ser_data, ser_data_idx).map(self.f.clone()));
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave build mapper {:?} s", dur);
        b
    }

}
