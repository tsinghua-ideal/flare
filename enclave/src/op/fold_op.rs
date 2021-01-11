use std::boxed::Box;
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::basic::{Data, Func, SerFunc};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::op::{CacheMeta, Context, Op, OpE, OpBase};
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::custom_thread::PThread;

pub struct Fold<T, TE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<T, TE, F, FE, FD> Clone for Fold<T, TE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        Fold {
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, TE, F, FE, FD> Fold<T, TE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
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
        Fold {
            prev,
            f,
            fe,
            fd,
        }
    }
}

impl<T, TE, F, FE, FD> OpBase for Fold<T, TE, F, FE, FD>
where
    T: Data,
    TE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            3 => self.step0_of_clone(p_buf, p_data_enc, is_shuffle), 
            _ => self.prev.build_enc_data_sketch(p_buf, p_data_enc, is_shuffle),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            3 => self.step1_of_clone(p_out, p_data_enc, is_shuffle), 
            _ => self.prev.clone_enc_data_out(p_out, p_data_enc, is_shuffle),
        } 
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            3 => self.free_res_enc(res_ptr),
            _ => self.prev.call_free_res_enc(res_ptr, is_shuffle),
        };
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

    fn iterator(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        self.compute_start(tid, data_ptr, is_shuffle, cache_meta)
    }
}

impl<T, TE, F, FE, FD> Op for Fold<T, TE, F, FE, FD>
where
    T: Data,
    TE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
{
    type Item = T;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8{
        //3 is only for reduce and fold
        if is_shuffle == 3 {
            let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<TE>) };
            let data = self.batch_decrypt(*data_enc.clone()); //need to check security
            forget(data_enc);
            let result = (self.f)(Box::new(data.into_iter()));
            let now = Instant::now();
            let result_enc = self.batch_encrypt(result); 
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("in enclave encrypt {:?} s", dur); 
            let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
            return result_ptr;  
        }
        else {
            self.narrow(data_ptr, cache_meta)
        }
    }

    fn compute(&self, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let (res_iter, handle) = self.prev.compute(data_ptr, cache_meta);
        (Box::new((self.f)(res_iter).into_iter()), handle)  
    }

}

impl<T, TE, F, FE, FD> OpE for Fold<T, TE, F, FE, FD>
where
    T: Data,
    TE: Data,   
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
{
    type ItemE = TE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Self::ItemE>
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Self::ItemE)->Vec<Self::Item>>
    }
}