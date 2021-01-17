use std::any::{Any, TypeId};
use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, HashMap, HashSet};
use std::cmp::{min, Ordering, Reverse};
use std::mem::forget;
use std::raw::TraitObject;
use std::sync::{
    atomic::{self, AtomicUsize},
    Arc, SgxMutex as Mutex, SgxRwLock as RwLock, Weak,
};
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;

use aes_gcm::{Aes128Gcm, aead::generic_array::functional::MappedGenericSequence};
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use sgx_types::*;

use crate::{CACHE, lp_boundary, opmap};
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::dependency::Dependency;
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::custom_thread::PThread;

mod aggregated_op;
pub use aggregated_op::*;
mod count_op;
pub use count_op::*;
mod co_grouped_op;
pub use co_grouped_op::*;
mod flatmapper_op;
pub use flatmapper_op::*;
mod fold_op;
pub use fold_op::*;
mod local_file_reader;
pub use local_file_reader::*;
mod mapper_op;
pub use mapper_op::*;
mod map_partitions_op;
pub use map_partitions_op::*;
mod pair_op;
pub use pair_op::*;
mod parallel_collection_op;
pub use parallel_collection_op::*;
mod reduced_op;
pub use reduced_op::*;
mod shuffled_op;
pub use shuffled_op::*;
mod union_op;
pub use union_op::*;

pub const MAX_ENC_BL: usize = 1000;

extern "C" {
    pub fn ocall_cache_to_outside(ret_val: *mut u8,   //write successfully or not
        rdd_id: usize,
        part_id: usize,
        sub_part_id: usize,
        data_ptr: usize,
    ) -> sgx_status_t;

    pub fn ocall_cache_from_outside(ret_val: *mut usize,  //ptr outside enclave
        rdd_id: usize,
        part_id: usize,
        sub_part_id: usize,
    ) -> sgx_status_t;
}

#[repr(C)]
#[derive(Clone)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    cached_rdd_id: usize,
    part_id: usize,
    sub_part_id: usize,
    steps_to_caching: usize,
    steps_to_cached: usize,
}

impl CacheMeta {
    pub fn new(
        caching_rdd_id: usize,
        cached_rdd_id: usize,
        part_id: usize,
        steps_to_caching: usize,
        steps_to_cached: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            cached_rdd_id,
            part_id,
            sub_part_id: 0, 
            steps_to_caching,
            steps_to_cached,
        }
    }

    fn set_sub_part_id(&mut self, sub_part_id: usize) {
        self.sub_part_id = sub_part_id;
    }

    fn count_caching_down(&mut self) -> bool {
        if self.steps_to_caching == usize::MAX || self.caching_rdd_id == 0 {
            println!("steps_to_caching {:?}", self.steps_to_caching);
            return false;
        }
        let (i, res) = self.steps_to_caching.overflowing_sub(1);
        self.steps_to_caching = i;
        println!("steps_to_caching {:?}", self.steps_to_caching);
        res
    }

    fn count_cached_down(&mut self) -> bool {
        if self.steps_to_cached == usize::MAX || self.cached_rdd_id == 0 {
            println!("steps_to_cached {:?}, cached_rdd_id {:?}", self.steps_to_cached, self.cached_rdd_id);
            return false;
        }
        let (i, res) = self.steps_to_cached.overflowing_sub(1);
        self.steps_to_cached = i;
        println!("steps_to_cached {:?}", self.steps_to_cached);
        res
    }
}



#[derive(Clone)]
pub struct OpCache {
    //<(cached_rdd_id, part_id, sub_part_id), data>, data can not be Any object, required by lazy_static
    map: Arc<RwLock<HashMap<(usize, usize, usize), usize>>>,
    //<(cached_rdd_id, part_id), HashSet<cached_sub_part_id>>
    //contains all sub_part_ids whose corresponding value is cached
    subpid_map: Arc<RwLock<HashMap<(usize, usize), HashSet<usize>>>>,
}

impl OpCache{
    pub fn new() -> Self {
        OpCache {
            map: Arc::new(RwLock::new(HashMap::new())),
            subpid_map: Arc::new(RwLock::new(HashMap::new())), 
        }
    }

    pub fn get(&self, key: (usize, usize, usize)) -> Option<usize> {
        self.map.read().unwrap().get(&key).map(|x| *x)
    }

    pub fn insert(&self, key: (usize, usize, usize), value: usize) -> Option<usize> {
        self.map.write().unwrap().insert(key, value)
    }

    //free the value?
    pub fn remove(&self, key: (usize, usize, usize)) -> Option<usize> {
        self.map.write().unwrap().remove(&key)
    }

    pub fn insert_subpid(&self, rdd_id: usize, part_id: usize, sub_part_id: usize) {
        let mut res =  self.subpid_map.write().unwrap();
        if let Some(set) = res.get_mut(&(rdd_id, part_id)) {
            set.insert(sub_part_id);
            return;
        }
        let mut hs = HashSet::new();
        hs.insert(sub_part_id);
        res.insert((rdd_id, part_id), hs);
    }

    pub fn remove_subpid(&self, rdd_id: usize, part_id: usize, sub_part_id: usize) {
        self.subpid_map.write()
            .unwrap()
            .get_mut(&(rdd_id, part_id))
            .unwrap()
            .remove(&sub_part_id);
    }

    pub fn get_subpid(&self, rdd_id: usize, part_id: usize) -> Vec<usize> {
        match self.subpid_map.read()
            .unwrap()
            .get(&(rdd_id, part_id)) 
        {
            Some(v) => v.iter().cloned().collect(),
            None => Vec::new(),
        }
    }

    pub fn clear_by_rid_pid(&self, rdd_id: usize, part_id: usize) {
        let sub_part_ids = match self.subpid_map.write().unwrap().remove(&(rdd_id, part_id)) {
            Some(ids) => ids,
            None => return,
        };
        let mut map = self.map.write().unwrap();
        for sub_part_id in sub_part_ids {
            map.remove(&(rdd_id, part_id, sub_part_id));
        }
    }

    pub fn clear_by_rid_pid_spid(&self, rdd_id: usize, part_id: usize, sub_part_id: usize) {
        self.map.write().unwrap().remove(&(rdd_id, part_id, sub_part_id));
    }
}

pub struct NextOpId<'a> {
    rdd_ids: &'a Vec<(usize, usize)>,
    cur_seg: usize,
    cur_rdd_id: usize,
}

impl<'a> NextOpId<'a> {
    pub fn new(rdd_ids: &'a Vec<(usize, usize)>) -> Self {
        NextOpId {
            rdd_ids,
            cur_seg: 0,
            cur_rdd_id: rdd_ids[0].0,
        }
    }

    pub fn get_next_op(&mut self) -> &'static Arc<dyn OpBase> {
        let id = self.get_next_id_and_advance();
        load_opmap().get(&id).unwrap()
    }

    fn get_next_id_and_advance(&mut self) -> usize {
        let (upper, lower) = self.rdd_ids[self.cur_seg];
        let cur_rdd_id;
        if self.cur_rdd_id >= lower && self.cur_rdd_id <= upper {
            cur_rdd_id = self.cur_rdd_id;
            self.cur_rdd_id -= 1;
        } else {
            self.cur_seg += 1;
            self.cur_rdd_id = self.rdd_ids[self.cur_seg].0;
            cur_rdd_id = self.cur_rdd_id;
            self.cur_rdd_id -= 1;
        }
        map_id(cur_rdd_id)
    }

}

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
pub fn ser_encrypt<T>(pt: Vec<T>) -> Vec<u8> 
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
    match TypeId::of::<u8>() == TypeId::of::<T>() {
        true => {
            let (ptr, len, cap) = pt.into_raw_parts();
            let rebuilt = unsafe {
                let ptr = ptr as *mut u8;
                Vec::from_raw_parts(ptr, len, cap)
            };
            encrypt(&rebuilt)
        },
        false => encrypt(bincode::serialize(&pt).unwrap().as_ref()),
    } 
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn ser_decrypt<T>(ct: Vec<u8>) -> Vec<T> 
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
    if ct.len() == 0 {
        return Vec::new();
    }
    match TypeId::of::<u8>() == TypeId::of::<T>() {
        true => {
            let pt = decrypt(&ct);
            let (ptr, len, cap) = pt.into_raw_parts();
            let rebuilt = unsafe {
                let ptr = ptr as *mut T;
                Vec::from_raw_parts(ptr, len, cap)
            };
            rebuilt
        },
        false => bincode::deserialize(decrypt(&ct).as_ref()).unwrap(),
    }

}

pub fn res_enc_to_ptr<T: Clone>(result_enc: T) -> *mut u8 {
    let result_ptr;
    if crate::immediate_cout {
        crate::ALLOCATOR.lock().set_switch(true);
        result_ptr = Box::into_raw(Box::new(result_enc.clone())) as *mut u8;
        crate::ALLOCATOR.lock().set_switch(false);
    } else {
        result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
    }
    result_ptr
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
        FE: SerFunc(Vec<T>) -> TE,
        FD: SerFunc(TE) -> Vec<T>,
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
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>,
    {
        //need to do insert opmap
        config.make_reader(self.clone(), func, fe, fd)
    }

    pub fn union<T: Data, TE: Data>(rdds: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> impl OpE<Item = T, ItemE = TE> {
        Union::new(rdds)
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
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8);
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8);
    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8);
    fn get_id(&self) -> usize;
    fn get_context(&self) -> Arc<Context>;
    fn get_deps(&self) -> Vec<Dependency>;
    fn get_next_deps(&self) -> Arc<Mutex<Vec<Dependency>>>;
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
    fn need_encryption(&self) -> bool {
        let mut flag = true;
        let next_deps = self.get_next_deps().lock().unwrap().clone();
        assert!(next_deps.len() == 1 || next_deps.len() == 0);
        for dep in next_deps {
            flag = match dep {
                Dependency::ShuffleDependency(_) => false,
                Dependency::NarrowDependency(_) => true,
            };
        }
        flag
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8;
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject>;
}

impl dyn OpBase {
    pub fn to_arc_op<T: ?Sized + Any>(self: Arc<Self>) -> Option<Arc<T>> {
        if let Some(mut u) = self.__to_arc_op(TypeId::of::<T>()) {
            Some(unsafe{
                Arc::from_raw(*std::mem::transmute::<_, &mut *mut T>(&mut u))
            })
        } else {
            None
        }
    }
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
    //need to avoid freeing the encrypted data for subsequent "clone_enc_data_out"
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        (**self).build_enc_data_sketch(p_buf, p_data_enc, is_shuffle);
    }
    //need to free the encrypted data. But how to deal with task failure?
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        (**self).clone_enc_data_out(p_out, p_data_enc, is_shuffle);
    }
    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        (**self).call_free_res_enc(res_ptr, is_shuffle);
    }
    fn get_id(&self) -> usize {
        (**self).get_op_base().get_id()
    }
    fn get_context(&self) -> Arc<Context> {
        (**self).get_op_base().get_context()
    }
    fn get_deps(&self) -> Vec<Dependency> {
        (**self).get_op_base().get_deps()
    }
    fn get_next_deps(&self) -> Arc<Mutex<Vec<Dependency>>> {
        (**self).get_op_base().get_next_deps()
    }
    fn get_prev_ids(&self) -> HashSet<usize> {
        (**self).get_op_base().get_prev_ids()
    }
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        (**self).get_op_base().iterator_start(tid, call_seq, data_ptr, is_shuffle, cache_meta)
    }
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        (**self).clone().__to_arc_op(id)
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
    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        (**self).compute_start(tid, call_seq, data_ptr, is_shuffle, cache_meta)
    }
    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        (**self).compute(call_seq, data_ptr, cache_meta)
    }
    fn cache(&self, data: Vec<Self::Item>) {
        (**self).cache(data);
    }
}

impl<I: OpE + ?Sized> OpE for SerArc<I> {
    type ItemE = I::ItemE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        (**self).get_ope()
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        (**self).get_fe()
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        (**self).get_fd()
    }
}


pub trait Op: OpBase + 'static {
    type Item: Data;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_op_base(&self) -> Arc<dyn OpBase>;
    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8;
    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>);
    fn cache(&self, data: Vec<Self::Item>) {
        ()
    }
}

pub trait OpE: Op {
    type ItemE: Data;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>;

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE>;

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>>;
    
    fn cache_from_outside(&self, key: (usize, usize, usize)) -> Vec<Self::Item> {
        let mut ptr: usize = 0;
        let sgx_status = unsafe { 
            ocall_cache_from_outside(&mut ptr, key.0, key.1, key.2)
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] OCALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        if ptr == 0 {
            return Vec::new();
        }
        let ct_ = unsafe {
            Box::from_raw(ptr as *mut u8 as *mut Vec<Self::ItemE>)
        };
        let ct = ct_.clone();
        forget(ct_);
        self.batch_decrypt(*ct)
    }

    fn cache_to_outside(&self, key: (usize, usize, usize), value: Vec<Self::Item>) -> PThread {
        let op = self.get_ope();
        let handle = unsafe {
            PThread::new(Box::new(move || {
                let ct = op.batch_encrypt(value);
                //println!("finish encryption, memory usage {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                crate::ALLOCATOR.lock().set_switch(true);
                let ct_ptr = Box::into_raw(Box::new(ct.clone())) as *mut u8 as usize;
                crate::ALLOCATOR.lock().set_switch(false);
                //println!("finish copy out, memory usage {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                let mut res = 0;
                ocall_cache_to_outside(&mut res, key.0, key.1, key.2, ct_ptr);
                //TODO: Handle the case res != 0
            }))
        }.unwrap();
        handle
    }

    fn get_and_remove_cached_data(&self, key: (usize, usize, usize)) -> Vec<Self::Item> {
        let val = match CACHE.remove(key) {
            //cache inside enclave
            Some(val) => {
                CACHE.remove_subpid(key.0, key.1, key.2);
                *unsafe {
                    Box::from_raw(val as *mut u8 as *mut Vec<Self::Item>)
                }
            },
            //cache outside enclave
            None => {
                self.cache_from_outside(key)
            },
        };
        val
    }

    fn set_cached_data(&self, key: (usize, usize, usize), iter: Box<dyn Iterator<Item = Self::Item>>) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>){              
        let res = iter.collect::<Vec<_>>();
        //println!("After collect, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        //cache inside enclave
        CACHE.insert(key, Box::into_raw(Box::new(res.clone())) as *mut u8 as usize);
        //println!("After cache inside enclave, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        CACHE.insert_subpid(key.0, key.1, key.2);
        //cache outside enclave
        
        let handle = self.cache_to_outside(key, res.clone());
        //println!("After launch encryption thread, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        (Box::new(res.into_iter()), Some(handle))
        
        /*
        (Box::new(res.into_iter()), None)
        */
    }

    fn step0_of_clone(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match is_shuffle {
            0 | 2  => {
                let encrypted = self.need_encryption();
                if encrypted {
                    let mut idx = Idx::new();
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                    data_enc.send(&mut buf, &mut idx);
                    forget(data_enc);
                } else {
                    let mut idx = Idx::new();
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::Item>) };
                    let data_enc = self.batch_encrypt(*data.clone());
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
            3 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            }
            _ => panic!("invalid is_shuffle"),
        };
        forget(buf);
    }

    fn step1_of_clone(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 2 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Self::ItemE>) };
                let encrypted = self.need_encryption();
                if encrypted {
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                    v_out.clone_in_place(&data_enc);
                } else {
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::Item>) };
                    let data_enc = Box::new(self.batch_encrypt(*data.clone()));
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
            3 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Self::ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Self::ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }
            _ => panic!("invalid is_shuffle"),
        }  
    }

    fn free_res_enc(&self, res_ptr: *mut u8) {
        crate::ALLOCATOR.lock().set_switch(true);
        let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Self::ItemE>) };
        drop(res);
        crate::ALLOCATOR.lock().set_switch(false);
    }

    fn batch_encrypt(&self, mut data: Vec<Self::Item>) -> Vec<Self::ItemE> {
        let mut len = data.len();
        let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL);
        while len >= MAX_ENC_BL {
            len -= MAX_ENC_BL;
            let remain = data.split_off(MAX_ENC_BL);
            let input = data;
            data = remain;
            data_enc.push(self.get_fe()(input));
        }
        if len != 0 {
            data_enc.push(self.get_fe()(data));
        }
        data_enc
    }

    fn batch_encrypt_ref(&self, data: &Vec<Self::Item>) -> Vec<Self::ItemE> {
        let len = data.len();
        let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL);
        let mut cur = 0;
        while cur < len {
            let next = min(len, cur + MAX_ENC_BL);
            data_enc.push(self.get_fe()(data[cur..next].to_vec()));
            cur = next;
        }
        data_enc
    }

    fn batch_decrypt(&self, data_enc: Vec<Self::ItemE>) -> Vec<Self::Item> {
        let mut data = Vec::new();
        for block in data_enc {
            let mut pt = self.get_fd()(block);
            data.append(&mut pt); //need to check security
        }
        data
    }

    fn narrow(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        let (result_iter, handle) = self.compute(call_seq, data_ptr, cache_meta);
        /*
        println!("In narrow(before join), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        if let Some(handle) = handle {
            handle.join();
        }
        */
        
        let result = result_iter.collect::<Vec<Self::Item>>();
        //println!("In narrow(before encryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        let result_ptr = match self.need_encryption() {
            true => {
                let now = Instant::now();
                let result_enc = self.batch_encrypt(result); 
                //println!("In narrow(after encryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                println!("in enclave encrypt {:?} s", dur); 
                res_enc_to_ptr(result_enc) 
            },
            false => {
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8;
                result_ptr
            },
        };
        
        if let Some(handle) = handle {
            handle.join();
        }
        
        result_ptr
    } 

    fn shuffle(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        let (data_iter, handle) = self.compute(call_seq, data_ptr, cache_meta);
        let data = data_iter.collect::<Vec<Self::Item>>();
        let next_deps = self.get_next_deps().lock().unwrap().clone();
        let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
        let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
            Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
            Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
        };
        let result_ptr = shuf_dep.do_shuffle_task(iter);
        if let Some(handle) = handle {
            handle.join();
        }
        result_ptr
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
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
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
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
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
        let reduce_partition = Box::new(move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc { 
                None => vec![],
                Some(e) => vec![e],
            }
        });         
        let new_op = SerArc::new(Reduced::new(self.get_op(), reduce_partition, self.get_fe(), self.get_fd()));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn fold<F>(&self, init: Self::Item, f: F) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition = Box::new(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                vec![iter.fold(zero.clone(), &cf)]
        });
        let new_op = SerArc::new(Fold::new(self.get_op(), reduce_partition, self.get_fe(), self.get_fd()));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = U, ItemE = UE>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let zero = init.clone();
        let reduce_partition = Box::new(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn)
        );
        let zero = init.clone();
        let combine = Box::new(
            move |iter: Box<dyn Iterator<Item = U>>| vec![iter.fold(zero.clone(), &comb_fn)]
        );
        let new_op = SerArc::new(Aggregated::new(self.get_ope(), reduce_partition, combine, fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn count(&self) -> SerArc<dyn OpE<Item = u64, ItemE = Vec<u64>>>
    where
        Self: Sized,
    {
        let new_op = SerArc::new(Count::new(self.get_ope()));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    } 

    fn union(
        &self,
        other: Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>,
    ) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
    {
        let new_op = SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>,
            other,
        ]));
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
