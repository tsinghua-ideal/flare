use std::boxed::Box;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex, Weak};
use std::vec::Vec;
use crate::common::{Common, Context};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::basic::{AnyData, Data};

pub struct Mapper<T: Data, U: Data, F>
where
    F: Fn(T) -> U + Clone,
{
    context: Weak<Context>,
    id: usize,
    dependencies: Arc<SgxMutex<Vec<Dependency>>>,
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
            dependencies: self.dependencies.clone(),
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
        prev.get_dependencies().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new())
            )
        );

        Mapper {
            context: Arc::downgrade(&sc),
            id: sc.new_op_id(),
            dependencies: prev.get_dependencies(),
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

    fn get_dependencies(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        self.dependencies.clone()
    }

    fn compute_by_id (&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>){
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
        else if id < self.id {
            self.prev.compute_by_id(ser_data, ser_data_idx, id, is_shuffle)
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        Box::new(self.prev.compute(ser_data).map(self.f.clone()))
    }

}
