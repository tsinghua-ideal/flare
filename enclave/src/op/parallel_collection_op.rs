use std::boxed::Box;
use std::marker::PhantomData;
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use crate::basic::{AnyData, Data, Func, SerFunc};
use crate::dependency::{Dependency};
use crate::op::{Context, Op, OpE, OpBase, OpVals};
use crate::serialization_free::{Construct, Idx, SizeBuf};

pub struct ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
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
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
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
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
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
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match is_shuffle {
            0 => {
                let encrypted = self.get_next_deps().lock().unwrap().is_empty();
                if encrypted {
                    let mut idx = Idx::new();
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<TE>) };
                    data_enc.send(&mut buf, &mut idx);
                    forget(data_enc);
                } else {
                    let mut idx = Idx::new();
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<T>) };
                    let data_enc = self.batch_encrypt(&data);
                    data_enc.send(&mut buf, &mut idx);
                    forget(data);
                }
            }, 
            1 => {
                let next_deps = self.get_next_deps().lock().unwrap().clone();
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.send_sketch(&mut buf, p_data_enc);
            },
            _ => panic!("invalid is_shuffle"),
        } 
        forget(buf);
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<TE>) };
                let encrypted = self.get_next_deps().lock().unwrap().is_empty();
                if encrypted {
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<TE>) };
                    v_out.clone_in_place(&data_enc);
                } else {
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<T>) };
                    let data_enc = Box::new(self.batch_encrypt(&data));
                    v_out.clone_in_place(&data_enc);
                    forget(data); //data may be used later
                }
                forget(v_out);
            }, 
            1 => {
                let next_deps = self.get_next_deps().lock().unwrap().clone();
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.send_enc_data(p_out, p_data_enc);
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

    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }
}

impl<T, TE, FE, FD> Op for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
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

    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let now = Instant::now();
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<TE>) }; 
        let data = self.batch_decrypt(&data_enc);
        forget(data_enc);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave decrypt {:?} s", dur);    
        Box::new(data.into_iter())
    }

}

impl<T, TE, FE, FD> OpE for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
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
