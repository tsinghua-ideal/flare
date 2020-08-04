use std::cmp;
use std::boxed::Box;
use std::marker::PhantomData;
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

    fn iterator(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        self.compute_start(ser_data, ser_data_idx, is_shuffle)
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

    fn compute_start(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
        let next_deps = self.next_deps.lock().unwrap();
        match is_shuffle == 0 {  //TODO maybe need to check if shuf_dep not in dep
            true => {       //No shuffle later
                let result = self.compute(ser_data, ser_data_idx)
                    .collect::<Vec<Self::Item>>();
                let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                (ser_result, ser_result_idx)
            },
            false => {      //Shuffle later
                let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();  
                let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                let ser_result_set = shuf_dep.do_shuffle_task(iter);
                let mut ser_result = Vec::<u8>::with_capacity(std::mem::size_of_val(&ser_result_set));
                let mut ser_result_idx = Vec::<usize>::with_capacity(cmp::max(ser_result.len(), 1));
                let mut idx: usize = 0;
                for (i, mut ser_result_bl) in ser_result_set.into_iter().enumerate() {
                    let bl_len = ser_result_bl.len();
                    idx += bl_len;
                    ser_result.append(&mut ser_result_bl);
                    ser_result_idx.push(idx); 
                }
                (ser_result, ser_result_idx)
            },
        }
    }

    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        let now = Instant::now();
        assert!(ser_data_idx.len()==1);
        let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("data addr = {:?}", data.as_ptr());
        println!("in enclave deserialize {:?} s", dur);    
        let now = Instant::now();
        let b = Box::new(data.into_iter());
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave into iterator {:?} s", dur);
        b
    }

}
