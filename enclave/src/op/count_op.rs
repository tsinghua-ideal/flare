use std::any::TypeId;
use std::boxed::Box;
use std::mem::forget;
use std::raw::TraitObject;
use std::sync::{Arc, SgxMutex};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::basic::{Data, Func, SerFunc};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::op::{CacheMeta, Context, NextOpId, Op, OpE, OpBase, ser_encrypt, ser_decrypt};
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::custom_thread::PThread;

pub struct Count<T, TE>
where
    T: Data, 
    TE: Data, 
{
    prev: Arc<dyn OpE<Item = T, ItemE = TE>>,
}

impl<T, TE> Clone for Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    fn clone(&self) -> Self {
        Count {
            prev: self.prev.clone(),
        }
    }
}

impl<T, TE> Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    pub(crate) fn new(prev: Arc<dyn OpE<Item = T, ItemE = TE>>) -> Self {
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id()); 
        Count {
            prev,
        }
    }
}

impl<T, TE> OpBase for Count<T, TE>
where
    T: Data, 
    TE: Data,
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

    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        
		self.compute_start(tid, call_seq, data_ptr, is_shuffle)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = u64>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = u64>;
            let vtable = unsafe {
                std::mem::transmute::<_, TraitObject>(x).vtable
            };
            let data = Arc::into_raw(self);
            Some(TraitObject {
                data: data as *mut (),
                vtable: vtable,
            })
        } else {
            None
        }
    }
}

impl<T, TE> Op for Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    type Item = u64;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8{
        //3 is only for reduce & fold
        if is_shuffle == 3 {
            //self.narrow(call_seq, data_ptr)
            let (result_iter, handle) = self.compute(call_seq, data_ptr);
            let result = result_iter.collect::<Vec<Self::Item>>()[0];
            if let Some(handle) = handle {
                handle.join();
            }
            result as *mut u8
        }
        else {
            self.prev.compute_start(tid, call_seq, data_ptr, is_shuffle)
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<TE>) };
        let len = data_enc.len();
        let mut count = 0;
        for i in 0..len {
            let block = self.prev.get_fd()(data_enc[i].clone());
            count += block.len(); 
        }
        forget(data_enc);
        (Box::new(vec![count as u64].into_iter()), None)
    }

}

impl<T, TE> OpE for Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    type ItemE = Vec<u64>;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        Box::new(|v: Vec<u64>| v)
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        Box::new(|v: Vec<u64>| v)
    }
}