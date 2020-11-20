use std::boxed::Box;
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::basic::{Data, Func, SerFunc};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::op::{Context, Op, OpE, OpBase};
use crate::serialization_free::{Construct, Idx, SizeBuf};

pub struct Reduced<T, TE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
{
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<T, TE, F, FE, FD> Clone for Reduced<T, TE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        Reduced {
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, TE, F, FE, FD> Reduced<T, TE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F, fe: FE, fd: FD) -> Self {
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id()); 
        /*
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        );
        */
        Reduced {
            prev,
            f,
            fe,
            fd,
        }
    }
}

impl<T, TE, F, FE, FD> OpBase for Reduced<T, TE, F, FE, FD>
where
    T: Data,
    TE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        
        match is_shuffle {
            3 => {
                let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<TE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
                forget(buf);
            }, 
            _ => {
                self.prev.build_enc_data_sketch(p_buf, p_data_enc, is_shuffle);
            },
        } 

        
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            3 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<TE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<TE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }, 
            _ => {
                self.prev.clone_enc_data_out(p_out, p_data_enc, is_shuffle);
            },
        } 
        
    }

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

impl<T, TE, F, FE, FD> Op for Reduced<T, TE, F, FE, FD>
where
    T: Data,
    TE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>,
{
    type Item = T;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8{
        //3 is only for reduce
        if is_shuffle == 3 {
            self.narrow(data_ptr)
        }
        else {
            self.prev.compute_start(data_ptr, is_shuffle)
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<TE>) };
        let data = self.batch_decrypt(&data_enc); //need to check security
        forget(data_enc);
        Box::new((self.f)(Box::new(data.into_iter())).into_iter())        
    }

}

impl<T, TE, F, FE, FD> OpE for Reduced<T, TE, F, FE, FD>
where
    T: Data,
    TE: Data,   
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>,
{
    type ItemE = TE;
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