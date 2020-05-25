use std::cmp;
use std::boxed::Box;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex, Weak};
use std::vec::Vec;
use crate::common::{Common, Context};
use crate::basic::{AnyData, Data};
use crate::dependency::{Dependency};

pub struct ParallelCollection<T> {
    context: Weak<Context>,
    id: usize,
    dependencies: Arc<SgxMutex<Vec<Dependency>>>,
    _marker_t: PhantomData<T>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            context: self.context.clone(),
            id: self.id,
            dependencies: self.dependencies.clone(),
            _marker_t: PhantomData,
        }
    }
} 

impl<T: Data> ParallelCollection<T> {
    pub fn new(context: Arc<Context>) -> Self {
        ParallelCollection {
            context: Arc::downgrade(&context),
            id: context.new_op_id(),
            dependencies: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
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

    fn get_dependencies(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        self.dependencies.clone()
    }

    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        if id == self.id {
            assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
            let dep = self.dependencies.lock().unwrap();
            match self.id > dep.len() || is_shuffle == 0 {
                true => {       //No shuffle later
                    let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
                    let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                    let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                    (ser_result, ser_result_idx)
                },
                false => {      //Shuffle later
                    let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();  
                    let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
                    let shuf_dep = match &dep[id] {
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
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();
        Box::new(data.into_iter())
    }

}
