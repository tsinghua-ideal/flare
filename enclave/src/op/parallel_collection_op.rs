use std::any::TypeId;
use std::boxed::Box;
use std::marker::PhantomData;
use std::mem::{forget, drop};
use std::raw::TraitObject;
use std::sync::{Arc, SgxMutex};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::basic::{AnyData, Data, Func, SerFunc};
use crate::dependency::{Dependency};
use crate::op::{CacheMeta, Context, NextOpId, Op, OpE, OpBase, OpVals};
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::custom_thread::PThread;

pub struct ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    vals: Arc<OpVals>, 
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    fe: FE,
    fd: FD,
    _marker_t: PhantomData<T>,
    _marker_te: PhantomData<TE>,
}

impl<T, TE, FE, FD> Clone for ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        ParallelCollection {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
            _marker_t: PhantomData,
            _marker_te: PhantomData,
        }
    }
} 

impl<T, TE, FE, FD> ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    pub fn new(context: Arc<Context>, fe: FE, fd: FD) -> Self {
        let vals = OpVals::new(context.clone());
        ParallelCollection {
            vals: Arc::new(vals),
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            fe,
            fd,
            _marker_t: PhantomData,
            _marker_te: PhantomData,
        }
    }
}

impl<T, TE, FE, FD> OpBase for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, is_shuffle),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, is_shuffle), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 => self.free_res_enc(res_ptr),
            1 => {
                let next_deps = self.get_next_deps().lock().unwrap().clone();
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.free_res_enc(res_ptr);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

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

    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        
		self.compute_start(tid, call_seq, data_ptr, is_shuffle, cache_meta)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = T>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = T>;
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

impl<T, TE, FE, FD> Op for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
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

    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(call_seq, data_ptr, cache_meta)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, data_ptr, cache_meta)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let now = Instant::now();
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<TE>) }; 
        //println!("In parallel_collection_op(before decryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        let data = self.batch_decrypt(*data_enc.clone());
        //println!("In parallel_collection_op(after decryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        forget(data_enc);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave decrypt {:?} s", dur);    
        (Box::new(data.into_iter()), None)
    }

}

impl<T, TE, FE, FD> OpE for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
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
