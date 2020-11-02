use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, HashSet};
use std::cmp::{Ordering, Reverse};
use std::vec::Vec;
use std::sync::{
    atomic::{self, AtomicPtr, AtomicUsize},
    Arc, SgxMutex, Weak,
};

use aes_gcm::Aes128Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};

use crate::{lp_boundary, opmap};
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::dependency::Dependency;
use crate::partitioner::Partitioner;
mod parallel_collection_op;
pub use parallel_collection_op::*;
mod co_grouped_op;
pub use co_grouped_op::*;
mod flatmapper_op;
pub use flatmapper_op::*;
mod local_file_reader;
pub use local_file_reader::*;
mod mapper_op;
pub use mapper_op::*;
mod map_partitions_op;
pub use map_partitions_op::*;
mod pair_op;
pub use pair_op::*;
mod reduced_op;
pub use reduced_op::*;
mod shuffled_op;
pub use shuffled_op::*;

pub fn map_id(id: usize) -> usize {
    let lp_bd = unsafe{ lp_boundary.load(atomic::Ordering::Relaxed).as_ref()}.unwrap();
    if lp_bd.len() == 0 {
        return id;
    }
    let mut sum_rep = 0;
    for i in lp_bd {
        let lower_bound = i.0 + sum_rep;
        let upper_bound = lower_bound + i.2 * (i.1 - i.0);
        if id <= lower_bound {   //outside loop
            return id - sum_rep;
        } else if id > lower_bound && id <= upper_bound { //inside loop
            let dedup_id = id - sum_rep;
            return  (dedup_id + i.1 - 2 * i.0 - 1) % (i.1 - i.0) + i.0 + 1;
        }
        sum_rep += (i.2 - 1) * (i.1 - i.0);
    }
    return id - sum_rep;
}

pub fn load_opmap() -> &'static mut BTreeMap<usize, Arc<dyn OpBase>> {
    unsafe { 
        opmap.load(atomic::Ordering::Relaxed)
            .as_mut()
    }.unwrap()
}

pub fn insert_opmap(op_id: usize, op_base: Arc<dyn OpBase>) {
    let op_map = load_opmap();
    op_map.insert(op_id, op_base);
}

#[inline(always)]
pub fn encrypt(pt: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.encrypt(nonce, pt).expect("encryption failure")
}

#[inline(always)]
pub fn divide_ct(ct: Vec<u8>, len: usize) -> Vec<Option<Vec<u8>>> {
    let mut ct_div = vec![None; len];
    ct_div[0] = Some(ct);
    ct_div
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn recover_ct(ct_div: Vec<Option<Vec<u8>>>) -> Vec<u8> {
    ct_div[0].clone().unwrap()
}

#[derive(Default)]
pub struct Context {
    next_op_id: Arc<AtomicUsize>,
}

impl Context {
    pub fn new() -> Arc<Self> {
        Arc::new(Context {
            next_op_id: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn new_op_id(self: &Arc<Self>) -> usize {
        self.next_op_id.fetch_add(1, atomic::Ordering::SeqCst)
    }

    pub fn make_op<T, TE, FE, FD>(self: &Arc<Self>, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = T, ItemE = TE>> 
    where
        T: Data,
        TE: Data,
        FE: SerFunc(Vec<T>) -> Vec<TE>,
        FD: SerFunc(Vec<TE>) -> Vec<T>,
    {
        let new_op = SerArc::new(ParallelCollection::new(self.clone(), fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    /// Load from a distributed source and turns it into a parallel collection.
    pub fn read_source<F, C, FE, FD, I: Data, O: Data, OE: Data>(
        self: &Arc<Self>,
        config: C,
        func: F,
        fe: FE,
        fd: FD,
    ) -> impl OpE<Item = O, ItemE = OE>
    where
        F: SerFunc(I) -> O,
        C: ReaderConfiguration<I>,
        FE: SerFunc(Vec<O>) -> Vec<OE>,
        FD: SerFunc(Vec<OE>) -> Vec<O>,
    {
        //need to do insert opmap
        config.make_reader(self.clone(), func, fe, fd)
    }

}

pub(crate) struct OpVals {
    pub id: usize,
    pub deps: Vec<Dependency>,
    pub context: Weak<Context>,
}

impl OpVals {
    pub fn new(sc: Arc<Context>) -> Self {
        OpVals {
            id: sc.new_op_id(),
            deps: Vec::new(),
            context: Arc::downgrade(&sc),
        }
    }
}

pub trait OpBase: Send + Sync {
    fn get_id(&self) -> usize;
    fn get_context(&self) -> Arc<Context>;
    fn get_deps(&self) -> Vec<Dependency>;
    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>>;
    fn get_prev_ids(&self) -> HashSet<usize> {
        let deps = self.get_deps();
        let mut set = HashSet::new();
        for dep in deps {
            for i in dep.get_prev_ids() {
                set.insert(i);
            }  
        };
        set
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8;
}

impl PartialOrd for dyn OpBase {
    fn partial_cmp(&self, other: &dyn OpBase) -> Option<Ordering> {
        Some(self.get_id().cmp(&other.get_id()))
    }
}

impl PartialEq for dyn OpBase {
    fn eq(&self, other: &dyn OpBase) -> bool {
        self.get_id() == other.get_id()
    }
}

impl Eq for dyn OpBase {}

impl Ord for dyn OpBase {
    fn cmp(&self, other: &dyn OpBase) -> Ordering {
        self.get_id().cmp(&other.get_id())
    }
}

impl<I: OpE + ?Sized> OpBase for SerArc<I> {
    fn get_id(&self) -> usize {
        (**self).get_op_base().get_id()
    }
    fn get_context(&self) -> Arc<Context> {
        (**self).get_op_base().get_context()
    }
    fn get_deps(&self) -> Vec<Dependency> {
        (**self).get_op_base().get_deps()
    }
    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        (**self).get_op_base().get_next_deps()
    }
    fn get_prev_ids(&self) -> HashSet<usize> {
        (**self).get_op_base().get_prev_ids()
    }
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        (**self).get_op_base().iterator(data_ptr, is_shuffle)
    }
}

impl<I: OpE + ?Sized> Op for SerArc<I> {
    type Item = I::Item;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        (**self).get_op()
    } 
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        (**self).get_op_base()
    }
    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        (**self).compute_start(data_ptr, is_shuffle)
    }
    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        (**self).compute(data_ptr)
    }
}

impl<I: OpE + ?Sized> OpE for SerArc<I> {
    type ItemE = I::ItemE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        (**self).get_ope()
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>> {
        (**self).get_fe()
    }

    fn get_fd(&self) -> Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>> {
        (**self).get_fd()
    }
}


pub trait Op: OpBase + 'static {
    type Item: Data;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_op_base(&self) -> Arc<dyn OpBase>;
    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8;
    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>>;

}

pub trait OpE: Op {
    type ItemE: Data;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>;

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>>;

    fn get_fd(&self) -> Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>>;

    fn narrow(&self, data_ptr: *mut u8) -> *mut u8 {
        let result = self.compute(data_ptr).collect::<Vec<Self::Item>>();
        let result_enc = self.get_fe()(result.clone()); 
        //println!("op_id = {:?}, \n result = {:?}, \n result_enc = {:?}", self.get_id(), result, result_enc);
        crate::ALLOCATOR.lock().set_switch(true);
        let result = result_enc.clone(); 
        let result_ptr = Box::into_raw(Box::new(result)) as *mut u8;
        crate::ALLOCATOR.lock().set_switch(false);
        result_ptr
    } 

    fn shuffle(&self, data_ptr: *mut u8) -> *mut u8 {
        let next_deps = self.get_next_deps().lock().unwrap().clone();
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::ItemE>) };
        let data = self.get_fd()(*(data_enc.clone())); //need to check security
        crate::ALLOCATOR.lock().set_switch(true);
        drop(data_enc);
        crate::ALLOCATOR.lock().set_switch(false);
        let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
        let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
            Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
            Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
        };
        shuf_dep.do_shuffle_task(iter)
    }
    /// Return a new RDD containing only the elements that satisfy a predicate.
    /*
    fn filter<F>(&self, predicate: F) -> SerArc<dyn Op<Item = Self::Item>>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + Clone + Copy + 'static,
        Self: Sized,
    {
        let filter_fn = Fn!(move |_index: usize, 
                                  items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> {
            Box::new(items.filter(predicate))
        });
        SerArc::new(MapPartitions::new(self.get_op(), filter_fn))
    }
    */

    fn map<U, UE, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = U, ItemE = UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        F: SerFunc(Self::Item) -> U,
        FE: SerFunc(Vec<U>) -> Vec<UE>,
        FD: SerFunc(Vec<UE>) -> Vec<U>,
    {
        let new_op = SerArc::new(Mapper::new(self.get_op(), f, fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn flat_map<U, UE, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = U, ItemE = UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
        FE: SerFunc(Vec<U>) -> Vec<UE>,
        FD: SerFunc(Vec<UE>) -> Vec<U>,
    {
        let new_op = SerArc::new(FlatMapper::new(self.get_op(), f, fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn reduce<F>(&self, f: F) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();        
        let reduce_partition = move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc { 
                None => vec![],
                Some(e) => vec![e],
            }
        };         
        let new_op = SerArc::new(Reduced::new(self.get_op(), reduce_partition, self.get_fe(), self.get_fd()));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

}

pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T;
}

impl<T, I> Reduce<T> for I
where
    I: Iterator<Item = T>,
{
    #[inline]
    fn reduce<F>(mut self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T,
    {
        self.next().map(|first| self.fold(first, f))
    }
}
