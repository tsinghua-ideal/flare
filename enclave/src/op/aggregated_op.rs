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

pub struct Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    UE: Data, 
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> Vec<U> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    prev: Arc<dyn OpE<Item = T, ItemE = TE>>,
    sf: SF,
    cf: CF,
    fe: FE,
    fd: FD,
}

impl<T, U, TE, UE, SF, CF, FE, FD> Clone for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> Vec<U> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        Aggregated {
            prev: self.prev.clone(),
            sf: self.sf.clone(),
            cf: self.cf.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, U, TE, UE, SF, CF, FE, FD> Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> Vec<U> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn OpE<Item = T, ItemE = TE>>, sf: SF, cf: CF, fe: FE, fd: FD) -> Self {
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id()); 
        /*
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        );
        */
        Aggregated {
            prev,
            sf,
            cf,
            fe,
            fd,
        }
    }
}

impl<T, U, TE, UE, SF, CF, FE, FD> OpBase for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> Vec<U>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
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

impl<T, U, TE, UE, SF, CF, FE, FD> Op for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> Vec<U>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    type Item = U;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8{
        //3 is only for reduce & fold
        if is_shuffle == 3 {
            self.narrow(data_ptr, cache_meta)
        }
        else {
            self.prev.compute_start(tid, data_ptr, is_shuffle, cache_meta)
        }
    }

    fn compute(&self, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<TE>) };
        let len = data_enc.len();
        let mut reduced = Vec::new();
        for i in 0..len {
            let block = self.prev.get_fd()(data_enc[i].clone());
            reduced.push((self.sf)(Box::new(block.into_iter())));  
        }
        forget(data_enc);
        (Box::new((self.cf)(Box::new(reduced.into_iter())).into_iter()), None)
    }

}

impl<T, U, TE, UE, SF, CF, FE, FD> OpE for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> Vec<U>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    type ItemE = UE;
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