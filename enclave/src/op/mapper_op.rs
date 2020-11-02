use std::boxed::Box;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::op::{Context, Op, OpE, OpBase, OpVals};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::basic::{AnyData, Data, Func, SerFunc};

pub struct Mapper<T: Data, U: Data, UE: Data, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> Vec<UE> + Clone,
    FD: Func(Vec<UE>) -> Vec<U> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> Clone for Mapper<T, U, UE, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> Vec<UE> + Clone,
    FD: Func(Vec<UE>) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        Mapper {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> Mapper<T, U, UE, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> Vec<UE> + Clone,
    FD: Func(Vec<UE>) -> Vec<U> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F, fe: FE, fd: FD) -> Self {
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
            fe,
            fd,
        }
    }
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> OpBase for Mapper<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> Vec<UE>,
    FD: SerFunc(Vec<UE>) -> Vec<U>,
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

impl<T: Data, U: Data, UE: Data, F, FE, FD> Op for Mapper<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> Vec<UE>,
    FD: SerFunc(Vec<UE>) -> Vec<U>,
{
    type Item = U;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        Box::new(self.prev.compute(data_ptr).map(self.f.clone()))
    }

}

impl<T: Data, U: Data, UE: Data, F, FE, FD> OpE for Mapper<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> Vec<UE>,
    FD: SerFunc(Vec<UE>) -> Vec<U>,
{
    type ItemE = UE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>>
    }

    fn get_fd(&self) -> Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>>
    }
}


