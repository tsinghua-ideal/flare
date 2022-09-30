use core::panic::Location;
use std::any::{Any, TypeId};
use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, hash_map::DefaultHasher, HashMap, VecDeque};
use std::cmp::{min, Ordering, Reverse};
use std::hash::{Hash, Hasher};
use std::mem::forget;
use std::ops::{Deref, DerefMut};
use std::raw::TraitObject;
use std::string::ToString;
use std::sync::{
    atomic::{self, AtomicBool, AtomicU32, AtomicUsize},
    Arc, SgxMutex as Mutex, SgxRwLock as RwLock, Weak,
};
use std::time::Instant;
use std::thread::{self, JoinHandle};
use std::untrusted::time::InstantEx;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use sgx_types::*;
use rand::{Rng, SeedableRng};

use crate::{CACHE, Fn, OP_MAP};
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::custom_thread::PThread;
use crate::dependency::{Dependency, OneToOneDependency, ShuffleDependencyTrait};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::utils;
use crate::utils::random::{BernoulliCellSampler, BernoulliSampler, PoissonSampler, RandomSampler};

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
mod partitionwise_sampled_op;
pub use partitionwise_sampled_op::*;
mod reduced_op;
pub use reduced_op::*;
mod shuffled_op;
pub use shuffled_op::*;
mod union_op;
pub use union_op::*;
mod zip_op;
pub use zip_op::*;

pub type ItemE = Vec<u8>;
type ResIter<T> = Box<dyn Iterator<Item = Box<dyn Iterator<Item = T>>>>;

pub const MAX_ENC_BL: usize = 1024;
pub const CACHE_LIMIT: usize = 4_000_000;
pub const ENABLE_CACHE_INSIDE: bool = false;
pub const MAX_THREAD: usize = 1;
pub const PARA_THRESHOLD: f64 = 3.0;
pub type Result<T> = std::result::Result<T, &'static str>;

extern "C" {
    pub fn ocall_cache_to_outside(ret_val: *mut u8,   //write successfully or not
        rdd_id: usize,
        part_id: usize,
        data_ptr: usize,
    ) -> sgx_status_t;

    pub fn ocall_cache_from_outside(ret_val: *mut usize,  //ptr outside enclave
        rdd_id: usize,
        part_id: usize,
    ) -> sgx_status_t;
}

pub fn default_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn load_opmap() -> &'static mut BTreeMap<OpId, Arc<dyn OpBase>> {
    unsafe { 
        OP_MAP.load(atomic::Ordering::Relaxed)
            .as_mut()
    }.unwrap()
}

pub fn insert_opmap(op_id: OpId, op_base: Arc<dyn OpBase>) {
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
pub fn ser_encrypt<T>(pt: &T) -> Vec<u8>
where
    T: ?Sized + serde::Serialize,
{
    encrypt(bincode::serialize(pt).unwrap().as_ref())
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn ser_decrypt<T>(ct: &[u8]) -> T
where
    T: serde::de::DeserializeOwned,
{
    bincode::deserialize(decrypt(ct).as_ref()).unwrap()
}

pub fn batch_encrypt<T: Data>(data: &[T], is_enc_outside: bool) -> Vec<ItemE> 
{
    if is_enc_outside {
        let acc = create_enc();
        data.chunks(MAX_ENC_BL).map(|x| ser_encrypt(x)).fold(acc, |mut acc, x| {
            crate::ALLOCATOR.set_switch(true);
            acc.push(x.clone());
            crate::ALLOCATOR.set_switch(false);
            acc
        })
    } else {
        data.chunks(MAX_ENC_BL).map(|x| ser_encrypt(x)).collect::<Vec<_>>()
    }
}

pub fn batch_decrypt<T: Data>(data_enc: &[ItemE], is_enc_outside: bool) -> Vec<T> 
{
    if is_enc_outside {
        data_enc.iter().map(|x| ser_decrypt::<Vec<T>>(&x.clone())).flatten().collect::<Vec<_>>()
    } else {
        data_enc.iter().map(|x| ser_decrypt::<Vec<T>>(x)).flatten().collect::<Vec<_>>()
    }
}

//The result_enc stays inside
pub fn res_enc_to_ptr<T: Clone>(result_enc: T) -> *mut u8 {
    let result_ptr;
    if crate::immediate_cout {
        crate::ALLOCATOR.set_switch(true);
        result_ptr = Box::into_raw(Box::new(result_enc.clone())) as *mut u8;
        crate::ALLOCATOR.set_switch(false);
    } else {
        result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
    }
    result_ptr
}

//acc stays outside enclave, and v stays inside enclave
pub fn merge_enc<T: Clone>(acc: &mut Vec<T>, v: &T) {
    crate::ALLOCATOR.set_switch(true);
    let v = v.clone();
    acc.push(v);
    crate::ALLOCATOR.set_switch(false);
}

pub fn combine_enc<T: Clone>(acc: &mut Vec<T>, mut other: Vec<T>) {
    crate::ALLOCATOR.set_switch(true);
    acc.append(&mut other);
    drop(other);
    crate::ALLOCATOR.set_switch(false);
}

pub fn create_enc<T: Clone>() -> Vec<T> {
    crate::ALLOCATOR.set_switch(true);
    let acc = Vec::new();
    crate::ALLOCATOR.set_switch(false);
    acc
}

//The result_enc stays outside
pub fn to_ptr<T: Clone>(result_enc: T) -> *mut u8 {
    crate::ALLOCATOR.set_switch(true);
    let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
    crate::ALLOCATOR.set_switch(false);
    result_ptr
}

#[track_caller]
fn get_text_loc_id() -> u64 {
    let loc = Location::caller();
    let file = loc.file().replace("_", "");
    let line = loc.line();
    let num = 0;
    let id = OpId::new(
        &file,
        line,
        num,
    ).h;
    id
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    caching_op_id: OpId,
    caching_part_id: usize,
    pub cached_rdd_id: usize,
    cached_op_id: OpId,
    cached_part_id: usize,
}

impl CacheMeta {
    pub fn new(
        caching_rdd_id: usize,
        caching_op_id: OpId,
        caching_part_id: usize,
        cached_rdd_id: usize,
        cached_op_id: OpId,
        cached_part_id: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            caching_op_id,
            caching_part_id,
            cached_rdd_id,
            cached_op_id,
            cached_part_id,
        }
    }

    pub fn transform(self) -> Self {
        CacheMeta {
            caching_rdd_id: 0,
            caching_op_id: Default::default(),
            caching_part_id: Default::default(),
            cached_rdd_id: self.caching_rdd_id,
            cached_op_id: self.caching_op_id,
            cached_part_id: self.caching_part_id,
        }
    }

}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DepInfo {
    is_shuffle: u8,
    identifier: usize,
    parent_rdd_id: usize,
    child_rdd_id: usize, 
    parent_op_id: OpId,
    child_op_id: OpId,
}

impl DepInfo {
    pub fn new(is_shuffle: u8,
        identifier: usize,
        parent_rdd_id: usize,
        child_rdd_id: usize,
        parent_op_id: OpId,
        child_op_id: OpId,
    ) -> Self {
        DepInfo {
            is_shuffle,
            identifier,
            parent_rdd_id,
            child_rdd_id,
            parent_op_id,
            child_op_id, 
        }
    }

    pub fn padding_new(is_shuffle: u8) -> Self {
        DepInfo {
            is_shuffle,
            identifier: 0,
            parent_rdd_id: 0,
            child_rdd_id: 0, 
            parent_op_id: Default::default(),
            child_op_id: Default::default(),
        }
    }

    pub fn get_op_key(&self) -> (OpId, OpId) {
        (
            self.parent_op_id,
            self.child_op_id,
        )
    }

    pub fn get_identifier(&self) -> usize {
        self.identifier
    }

    /// 0 for narrow, 1 for shuffle write, 2 for shuffle read, 3 for action
    pub fn dep_type(&self) -> u8 {
        self.is_shuffle
    }

}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Input {
    pub data: usize,
    parallel_num: usize,
}

impl Input {
    pub fn new<T: Data>(data: &T, 
        parallel_num: usize,
    ) -> Self {
        let data = data as *const T as usize;
        Input {
            data,
            parallel_num,
        }
    }

    pub fn get_enc_data<T>(&self) -> &T {
        unsafe { (self.data as *const T).as_ref() }.unwrap()
    }

    pub fn get_parallel(&self) -> usize {
        self.parallel_num
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Text<T, TE>
{
    data: Option<T>,
    data_enc: Option<TE>,
    id: u64,
}

impl<T, TE> Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    #[track_caller]
    pub fn new(data: Option<T>, data_enc: Option<TE>) -> Self {
        let id = get_text_loc_id();
        Text {
            data,
            data_enc,
            id,
        }
    }

    pub fn get_ct(&self) -> TE {
        self.data_enc.clone().unwrap()
    }

    pub fn get_ct_ref(&self) -> &TE {
        self.data_enc.as_ref().unwrap()
    }

    pub fn get_ct_mut(&mut self) -> &mut TE {
        self.data_enc.as_mut().unwrap()
    }

    pub fn update_from_tail_info(&mut self, tail_info: &TailCompInfo) {
        let text = bincode::deserialize(&tail_info.get(self.id).unwrap()).unwrap();
        *self = text;
    }
}

impl<T> Text<T, Vec<u8>>
where
    T: Data,
{
    #[track_caller]
    pub fn rec(tail_info: &TailCompInfo) -> Self {
        let id = get_text_loc_id();
        let mut text: Text<T, Vec<u8>> = match tail_info.get(id) {
            Some(bytes) => {
                bincode::deserialize(&bytes).unwrap()
            },
            None => {
                Default::default()
            },
        };
        if text.data.is_none() && text.data_enc.is_some() {
            text.data = Some(ser_decrypt(text.data_enc.as_ref().unwrap()));
            text.data_enc = None;
        }
        text
    }

    //currently it is only used for the case
    //where the captured variables are encrypted
    pub fn get_pt(&self) -> T {
        if self.data.is_some() {
            self.data.clone().unwrap()
        } else if self.data_enc.is_some() {
            ser_decrypt(self.data_enc.as_ref().unwrap())
        } else {
            Default::default()
        }
    }
}

impl<T> Text<Vec<T>, Vec<Vec<u8>>>
where
    T: Data,
{
    #[track_caller]
    pub fn rec(tail_info: &TailCompInfo) -> Self {
        let id = get_text_loc_id();
        let mut text: Text<Vec<T>, Vec<Vec<u8>>> = match tail_info.get(id) {
            Some(bytes) => {
                bincode::deserialize(&bytes).unwrap()
            },
            None => {
                Default::default()
            },
        };
        if text.data.is_none() && text.data_enc.is_some() {
            text.data = Some(text.data_enc.as_ref()
                .unwrap()
                .iter()
                .map(|x| ser_decrypt::<Vec<T>>(x).into_iter())
                .flatten()
                .collect::<Vec<_>>()
            );
            text.data_enc = None;
        }
        text
    }

    //currently it is only used for the case
    //where the captured variables are encrypted
    pub fn get_pt(&self) -> Vec<T> {
        if self.data.is_some() {
            self.data.clone().unwrap()
        } else if self.data_enc.is_some() {
            self.data_enc
                .as_ref()
                .unwrap()
                .iter()
                .map(|x| ser_decrypt::<Vec<T>>(x).into_iter())
                .flatten()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        }
    }
}

impl<T, TE> Deref for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.data.as_ref().unwrap()
    }
}

impl<T, TE> DerefMut for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.as_mut().unwrap()
    }
}

#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct TailCompInfo {
    m: HashMap<u64, Vec<u8>>,
}

impl TailCompInfo {
    pub fn new() -> Self {
        TailCompInfo { m: HashMap::new() }
    }

    pub fn insert<T, TE>(&mut self, text: &Text<T, TE>)
    where
        T: Data,
        TE: Data,
    {
        self.m.insert(text.id, bincode::serialize(text).unwrap());
    }

    pub fn remove(&mut self, id: u64) -> Vec<u8> {
        self.m.remove(&id).unwrap()
    }

    pub fn get(&self, id: u64) -> Option<Vec<u8>> {
        match self.m.get(&id) {
            Some(ser) => Some(ser.clone()),
            None => None,
        }
    }

    pub fn clear(&mut self) {
        self.m.clear();
    }
}

#[derive(Clone)]
pub struct OpCache {
    //temperarily save the point of data, which is ready to sent out
    // <(cached_rdd_id, part_id), data ptr>
    out_map: Arc<RwLock<HashMap<(usize, usize), usize>>>,
}

impl OpCache{
    pub fn new() -> Self {
        OpCache {
            out_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn send(&self, key: (usize, usize)) {
        if let Some(ct_ptr) = self.out_map.write().unwrap().remove(&key) {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, key.0, key.1, ct_ptr); }
            //TODO: Handle the case res != 0
        }
    }

    pub fn clear(&self) {
        let out_map = std::mem::take(&mut *self.out_map.write().unwrap());
        //normally the map is empty, and it should not enter the following loop
        for ((rdd_id, part_id), data_ptr) in out_map.into_iter() {
            let mut res = 0;
            unsafe { ocall_cache_to_outside(&mut res, rdd_id, part_id, data_ptr); }
        }
    }

}
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpId {
    h: u64,
}

impl OpId {
    pub fn new(file: &str, line: u32, num: usize) -> Self {
        let h = default_hash(&(file.to_string(), line, num));
        OpId {
            h,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NextOpId {
    tid: u64,
    rdd_ids: Vec<usize>,
    op_ids: Vec<OpId>,
    part_ids: Vec<usize>,
    cur_idx: usize,
    cache_meta: CacheMeta,
    captured_vars: HashMap<usize, Vec<Vec<u8>>>,
    is_shuffle: bool,
    pub para_range: Option<(usize, usize)>,
    //if the decryption step and the narrow processing parallelizable 
    pub is_step_para: (bool, bool, bool),
    //used to decide is_step_para
    pub sample_len: usize,
}

impl<'a> NextOpId {
    pub fn new(tid: u64, 
        rdd_ids: Vec<usize>, 
        op_ids: Vec<OpId>, 
        part_ids: Vec<usize>, 
        cache_meta: CacheMeta, 
        captured_vars: HashMap<usize, Vec<Vec<u8>>>, 
        dep_info: &DepInfo,
    ) -> Self {
        let is_shuffle = dep_info.is_shuffle == 1;
        NextOpId {
            tid,
            rdd_ids,
            op_ids,
            part_ids,
            cur_idx: 0,
            cache_meta,
            captured_vars,
            is_shuffle,
            para_range: None,
            is_step_para: (true, true, true),
            sample_len: 0,
        }
    }

    fn get_cur_rdd_id(&self) -> usize {
        self.rdd_ids[self.cur_idx]
    }

    fn get_cur_op_id(&self) -> OpId {
        self.op_ids[self.cur_idx]
    }
    
    fn get_part_id(&self) -> usize {
        self.part_ids[self.cur_idx]
    }

    pub fn get_cur_op(&self) -> &'static Arc<dyn OpBase> {
        let cur_op_id = self.get_cur_op_id();
        load_opmap().get(&cur_op_id).unwrap()
    }

    pub fn get_next_op(&mut self) -> &'static Arc<dyn OpBase> {
        self.cur_idx += 1;
        let next_op_id = self.get_cur_op_id();
        load_opmap().get(&next_op_id).unwrap()
    }

    pub fn get_ser_captured_var(&self) -> Option<&Vec<Vec<u8>>> {
        let cur_rdd_id = self.get_cur_rdd_id();
        self.captured_vars.get(&cur_rdd_id)
    }

    pub fn get_caching_doublet(&self) -> (usize, usize) {
        (
            self.cache_meta.caching_rdd_id,
            self.cache_meta.caching_part_id,
        )
    }

    pub fn get_cached_doublet(&self) -> (usize, usize) {
        (
            self.cache_meta.cached_rdd_id,
            self.cache_meta.cached_part_id,
        )
    }

    pub fn have_cache(&self) -> bool {
        self.get_cur_rdd_id() == self.cache_meta.cached_rdd_id
        && self.cache_meta.cached_rdd_id != 0
    }

    pub fn need_cache(&self) -> bool {
        self.get_cur_rdd_id() == self.cache_meta.caching_rdd_id
        && self.cache_meta.caching_rdd_id != 0
    }

    pub fn is_caching_final_rdd(&self) -> bool {
        self.cur_idx == 0
        //It seems not useful unless a cached rdd in shuffle task is simultaniously relied on in another result task
        && !self.is_shuffle
    }

    pub fn is_head(&self) -> bool {
        self.cur_idx == self.rdd_ids.len() - 1
    }
}

#[derive(Default)]
pub struct Context {
    last_loc_file: RwLock<&'static str>,
    last_loc_line: AtomicU32,
    num: AtomicUsize,
    in_loop: AtomicBool,
    is_tail_comp: AtomicBool,
}

impl Context {
    pub fn new() -> Result<Arc<Self>> {
        Ok(Arc::new(Context {
            last_loc_file: RwLock::new("null"),
            last_loc_line: AtomicU32::new(0), 
            num: AtomicUsize::new(0),
            in_loop: AtomicBool::new(false),
            is_tail_comp: AtomicBool::new(false),
        }))
    }

    pub fn add_num(self: &Arc<Self>, addend: usize) -> usize {
        self.num.fetch_add(addend, atomic::Ordering::SeqCst)
    }

    pub fn set_num(self: &Arc<Self>, num: usize) {
        self.num.store(num, atomic::Ordering::SeqCst)
    }

    pub fn enter_loop(self: &Arc<Self>) {
        self.in_loop.store(true, atomic::Ordering::SeqCst);
    }

    pub fn leave_loop(self: &Arc<Self>) {
        self.in_loop.store(false, atomic::Ordering::SeqCst);
    }

    pub fn set_is_tail_comp(self: &Arc<Self>, is_tail_comp: bool) {
        self.is_tail_comp.store(is_tail_comp, atomic::Ordering::SeqCst)
    }

    pub fn get_is_tail_comp(self: &Arc<Self>) -> bool {
        self.is_tail_comp.load(atomic::Ordering::SeqCst)
    }

    pub fn new_op_id(self: &Arc<Self>, loc: &'static Location<'static>) -> OpId {
        use atomic::Ordering::SeqCst;
        let file = loc.file();
        let line = loc.line();
        
        let num = if *self.last_loc_file.read().unwrap() != file || self.last_loc_line.load(SeqCst) != line {
            *self.last_loc_file.write().unwrap() = file;
            self.last_loc_line.store(line, SeqCst);
            self.num.store(0, SeqCst);
            0
        } else {
            self.num.load(SeqCst)
        };
        
        let op_id = 
        OpId::new(
            file,
            line,
            num,
        );
        //println!("op, file = {:?}, line = {:?}, num = {:?}, op_id = {:?}", file, line, num, op_id);
        op_id
    }

    #[track_caller]
    pub fn make_op<T>(self: &Arc<Self>, num_splits: usize) -> SerArc<dyn Op<Item = T>> 
    where
        T: Data,
    {
        let new_op = SerArc::new(ParallelCollection::new(self.clone(), num_splits));
        if !self.get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    /// Load from a distributed source and turns it into a parallel collection.
    #[track_caller]
    pub fn read_source<C, I: Data, O: Data>(
        self: &Arc<Self>,
        config: C,
        func: Option< Box<dyn Func(I) -> O >>,
        sec_func: Option< Box<dyn Func(I) -> Vec<ItemE> >>,
    ) -> SerArc<dyn Op<Item = O>>
    where
        C: ReaderConfiguration<I>,
    {
        config.make_reader(self.clone(), func, sec_func)
    }

    #[track_caller]
    pub fn union<T: Data>(rdds: &[Arc<dyn Op<Item = T>>]) -> impl Op<Item = T> {
        Union::new(rdds)
    }

}

pub(crate) struct OpVals {
    pub id: OpId,
    pub split_num: AtomicUsize,
    pub deps: Vec<Dependency>,
    pub context: Weak<Context>,
    pub in_loop: bool,
}

impl OpVals {
    #[track_caller]
    pub fn new(sc: Arc<Context>, split_num: usize) -> Self {
        let loc = Location::caller(); 
        OpVals {
            id: sc.new_op_id(loc),
            split_num: AtomicUsize::new(split_num),
            deps: Vec::new(),
            context: Arc::downgrade(&sc),
            in_loop: sc.in_loop.load(atomic::Ordering::SeqCst),
        }
    }
}

pub trait OpBase: Send + Sync {
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo);
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo);
    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo);
    fn fix_split_num(&self, split_num: usize) {
        unreachable!()
    }
    fn get_op_id(&self) -> OpId;
    fn get_context(&self) -> Arc<Context>;
    fn get_deps(&self) -> Vec<Dependency> {
        unreachable!()
    }
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        unreachable!()
    }
    fn get_next_shuf_dep(&self, dep_info: &DepInfo) -> Option<Arc<dyn ShuffleDependencyTrait>> {
        let cur_key = dep_info.get_op_key();
        let next_deps = self.get_next_deps();
        let res = match next_deps.read().unwrap().get(&cur_key) {
            Some(dep) => match dep {
                Dependency::ShuffleDependency(shuf_dep) => Some(shuf_dep.clone()),
                Dependency::NarrowDependency(nar_dep) => None,
            },
            None => None,
        };
        res
    }
    //supplement
    fn sup_next_shuf_dep(&self, dep_info: &DepInfo, reduce_num: usize) {
        let cur_key = dep_info.get_op_key();
        let next_deps = self.get_next_deps().read().unwrap().clone();
        match next_deps.get(&cur_key) {
            None => {  //not exist, add the dependency
                let child = load_opmap().get(&cur_key.1).unwrap();
                for value in child.get_deps() {
                    match value {
                        Dependency::ShuffleDependency(shuf_dep) => {
                            if dep_info.identifier == shuf_dep.get_identifier() {
                                self.get_next_deps().write().unwrap().insert(
                                    cur_key, 
                                    Dependency::ShuffleDependency(
                                        shuf_dep.set_parent_and_child(cur_key.0, cur_key.1)
                                    )
                                );
                                break;
                            }
                        },
                        Dependency::NarrowDependency(_) => (),
                    };
                }  
            },
            Some(dep) => { //already exist, check and change the partitioner
                match dep {
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep.change_partitioner(reduce_num),
                    Dependency::NarrowDependency(nar_dep) => panic!("should not be narrow dep"),
                };
            },
        }
    }
    fn sup_next_nar_dep(&self, child: OpId) {
        let next_deps = self.get_next_deps().read().unwrap().clone();
        let parent = self.get_op_id();
        let next_dep = next_deps.get(&(parent, child));
        match next_dep {
            None => {
                self.get_next_deps().write().unwrap().insert(
                    (parent, child),
                    Dependency::NarrowDependency(
                        Arc::new(OneToOneDependency::new(parent, child))
                    )
                );
            },
            Some(dep) => match dep {
                Dependency::NarrowDependency(_) => (),
                Dependency::ShuffleDependency(_) => panic!("dependency conflict!"),
            },
        }
    }
    fn is_in_loop(&self) -> bool {
        unreachable!()
    }
    fn number_of_splits(&self) -> usize {
        unreachable!()
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn iterator_start(&self, call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        unreachable!()
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        unreachable!()
    }
    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        unreachable!()
    }
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        unreachable!()
    }
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
        Some(self.get_op_id().cmp(&other.get_op_id()))
    }
}

impl PartialEq for dyn OpBase {
    fn eq(&self, other: &dyn OpBase) -> bool {
        self.get_op_id() == other.get_op_id()
    }
}

impl Eq for dyn OpBase {}

impl Ord for dyn OpBase {
    fn cmp(&self, other: &dyn OpBase) -> Ordering {
        self.get_op_id().cmp(&other.get_op_id())
    }
}

impl<I: Op + ?Sized> OpBase for SerArc<I> {
    //need to avoid freeing the encrypted data for subsequent "clone_enc_data_out"
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).build_enc_data_sketch(p_buf, p_data_enc, dep_info);
    }
    //need to free the encrypted data. But how to deal with task failure?
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).clone_enc_data_out(p_out, p_data_enc, dep_info);
    }
    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        (**self).call_free_res_enc(res_ptr, is_enc, dep_info);
    }
    fn fix_split_num(&self, split_num: usize) {
        (**self).fix_split_num(split_num)
    }
    fn get_op_id(&self) -> OpId {
        (**self).get_op_base().get_op_id()
    }
    fn get_context(&self) -> Arc<Context> {
        (**self).get_op_base().get_context()
    }
    fn get_deps(&self) -> Vec<Dependency> {
        (**self).get_op_base().get_deps()
    }
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        (**self).get_op_base().get_next_deps()
    }
    fn is_in_loop(&self) -> bool {
        (**self).get_op_base().is_in_loop()
    }
    fn number_of_splits(&self) -> usize {
        (**self).get_op_base().number_of_splits()
    }
    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        (**self).get_op_base().iterator_start(call_seq, input, dep_info)
    }
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        (**self).randomize_in_place(input, seed, num)
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        (**self).set_sampler(with_replacement, fraction)
    }
    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        (**self).etake(input, should_take, have_take)
    }
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        (**self).clone().__to_arc_op(id)
    }
}

impl<I: Op + ?Sized> Op for SerArc<I> {
    type Item = I::Item;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        (**self).get_op()
    } 
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        (**self).get_op_base()
    }
    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        (**self).get_cache_space()
    }
    fn compute_start(&self, call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        (**self).compute_start(call_seq, input, dep_info)
    }
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        (**self).compute(call_seq, input)
    }
    fn cache(&self, data: Vec<Self::Item>) {
        (**self).cache(data);
    }
}


pub trait Op: OpBase + 'static {
    type Item: Data;
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>;
    fn get_op_base(&self) -> Arc<dyn OpBase>;
    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        unreachable!()
    }
    fn compute_start(&self, call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    //the deault is for narrow begin
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache || (call_seq.get_cur_rdd_id() == call_seq.cache_meta.cached_rdd_id && data_ptr == 0) {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
        }
        
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let res_iter = self.parallel_control(call_seq, data_enc);
        
        if need_cache {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }
    fn cache(&self, data: Vec<Self::Item>) {
        ()
    }
    fn cache_from_outside(&self, key: (usize, usize)) -> Option<&'static Vec<ItemE>> {
        let mut ptr: usize = 0;
        let sgx_status = unsafe { 
            ocall_cache_from_outside(&mut ptr, key.0, key.1)
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] OCALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        if ptr == 0 {
            return None;
        }
        /*
        let ct_ = unsafe {
            Box::from_raw(ptr as *mut u8 as *mut Vec<ItemE>)
        };
        let ct = ct_.clone();
        forget(ct_);
        batch_decrypt(*ct)
        */
        Some(unsafe {
            (ptr as *const u8 as *const Vec<ItemE>).as_ref()
        }.unwrap())
    }

    fn cache_to_outside(&self, key: (usize, usize), value: &Vec<Self::Item>) -> Option<PThread> {
        //let handle = unsafe {
        //    PThread::new(Box::new(move || {
        let ct = batch_encrypt(value, true);
        //println!("finish encryption, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        let mut out_map = CACHE.out_map.write().unwrap();
        let acc = match out_map.remove(&key) {
            Some(ptr) => {
                crate::ALLOCATOR.set_switch(true);
                let mut acc = *unsafe { Box::from_raw(ptr as *mut Vec<ItemE>) };
                combine_enc(&mut acc, ct);
                crate::ALLOCATOR.set_switch(false);
                to_ptr(acc)
            },
            None => to_ptr(ct), 
        } as usize;
        out_map.insert(key, acc);
        //println!("finish copy out, memory usage {:?} B", crate::ALLOCATOR.get_memory_usage());
        //    }))
        //}.unwrap();
        //Some(handle)
        None
    }

    fn get_and_remove_cached_data(&self, call_seq: &mut NextOpId) -> ResIter<Self::Item> {
        let key = call_seq.get_cached_doublet();
        //cache inside enclave
        let have_cache_inside = {
            let cache_space = self.get_cache_space();
            let l = cache_space.lock().unwrap();
            l.contains_key(&key)
        };
        
        if have_cache_inside {
            //another version of parallel_control, there are no decryption step
            match std::mem::take(&mut call_seq.para_range) {
                Some((b, e)) => {
                    let r = e.saturating_sub(b);
                    if r == 0 {
                        Box::new(vec![].into_iter()) 
                    } else {
                        let cache_space = self.get_cache_space();
                        let mut l = cache_space.lock().unwrap();
                        let (data, is_empty) = {
                            let v = l.get_mut(&key).unwrap();
                            let data = v.split_off(v.len() - r);
                            (data, v.is_empty())
                        };
                        if is_empty {
                            println!("remove cache key");
                            l.remove(&key).unwrap();
                        }
                        Box::new(data.into_iter().map(|item| Box::new(item.into_iter()) as Box<dyn Iterator<Item = _>>))
                    }
                },
                None => {
                    let sample_data = {
                        let cache_space = self.get_cache_space();
                        let mut cache_space = cache_space.lock().unwrap();
                        let entry = cache_space.get_mut(&key).unwrap();
                        call_seq.para_range = Some((0, entry.len().saturating_sub(1)));
                        entry.pop().unwrap()
                    };
                    call_seq.sample_len = sample_data.len();
                    //profile begin
                    crate::ALLOCATOR.reset_alloc_cnt();
                    Box::new(vec![sample_data].into_iter().map(move |data| {
                        Box::new(data.into_iter()) as Box<dyn Iterator<Item = _>>
                    }))
                }
            }
        } else {
            let data_enc = self.cache_from_outside(key).unwrap();
            self.parallel_control(call_seq, data_enc)
        }
    }

    fn set_cached_data(&self, call_seq: &NextOpId, res_iter: ResIter<Self::Item>, is_caching_final_rdd: bool) -> ResIter<Self::Item> {
        let ope = self.get_op();
        let op_id = self.get_op_id();
        let key = call_seq.get_caching_doublet();
        let cache_space = self.get_cache_space();

        Box::new(res_iter.map(move |iter| {
            //cache outside enclave
            if is_caching_final_rdd {
                iter
            } else {
                let res = iter.collect::<Vec<_>>();
                //cache inside enclave
                if ENABLE_CACHE_INSIDE {
                    let mut cache_space = cache_space.lock().unwrap();
                    let data = cache_space.entry(key).or_insert(Vec::new());
                    data.push(res.clone());
                } 
                //it should be always cached outside for inter-machine communcation
                let _handle = ope.cache_to_outside(key, &res);
                Box::new(res.into_iter()) as Box<dyn Iterator<Item = _>>
            }
        }))
    }

    fn step0_of_clone(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match dep_info.dep_type() {
            0 | 2 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            }, 
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.send_sketch(&mut buf, p_data_enc);
            },
            3 | 4 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            }
            _ => panic!("invalid is_shuffle"),
        };
        forget(buf);
    }

    fn step1_of_clone(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }, 
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.send_enc_data(p_out, p_data_enc);
            },
            3 | 4 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<ItemE>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<ItemE>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            }
            _ => panic!("invalid is_shuffle"),
        }  
    }

    fn free_res_enc(&self, res_ptr: *mut u8, is_enc: bool) {
        if is_enc {
            crate::ALLOCATOR.set_switch(true);
            let res = unsafe { Box::from_raw(res_ptr as *mut Vec<ItemE>) };
            drop(res);
            crate::ALLOCATOR.set_switch(false);
        } else {
            let _res = unsafe { Box::from_raw(res_ptr as *mut Vec<Self::Item>) };
        }
    }

    //note that data_enc is outside enclave
    fn parallel_control(&self, call_seq: &mut NextOpId, data_enc: &Vec<ItemE>) -> ResIter<Self::Item> {
        match std::mem::take(&mut call_seq.para_range) {
            Some((b, e)) => {
                let mut data = if let Some(data_enc) = data_enc.get(b..e) {
                    data_enc.iter().map(|x| ser_decrypt::<Vec<Self::Item>>(&x.clone())).collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                if call_seq.is_step_para.0 ^ call_seq.is_step_para.1 {
                    
                    let key = (call_seq.get_cur_rdd_id(), call_seq.get_part_id());
                    //originally it cannot happen that call_seq.get_caching_doublet() == call_seq.get_cached_doublet()
                    //however, in parallel processing, since we manually set cached key for special use, the situation can occur 
                    if key == call_seq.get_caching_doublet() {
                        for block in &data {
                            let _handle = self.cache_to_outside(key, &block);
                        }
                    }
                    //cache the data inside enclave for parallel processing
                    let cache_space = self.get_cache_space();
                    let mut cache_space = cache_space.lock().unwrap();
                    let entry = cache_space.entry(key).or_insert(Vec::new());
                    entry.append(&mut data);
                    Box::new(Vec::new().into_iter())
                } else {
                    Box::new(data.into_iter().map(|item| Box::new(item.into_iter()) as Box<dyn Iterator<Item = _>>))
                }
            },
            None => {
                //for profile
                crate::ALLOCATOR.reset_alloc_cnt();
                let data = if data_enc.is_empty() {
                    Vec::new()
                } else {
                    ser_decrypt::<Vec<Self::Item>>(&data_enc[0].clone())
                };
                let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
                call_seq.sample_len = data.len();
                let alloc_cnt_ratio = alloc_cnt as f64/(call_seq.sample_len as f64);
                println!("for decryption, alloc_cnt per len = {:?}", alloc_cnt_ratio);
                call_seq.is_step_para.0 = alloc_cnt_ratio < PARA_THRESHOLD;
                call_seq.para_range = Some((1, data_enc.len()));

                //profile begin
                crate::ALLOCATOR.reset_alloc_cnt();
                Box::new(vec![data].into_iter().map(move |data| {
                    Box::new(data.into_iter()) as Box<dyn Iterator<Item = _>>
                }))
            }
        }
    }

    fn spawn_enc_thread(&self, mut results: Vec<Vec<Self::Item>>, handlers: &mut Vec<JoinHandle<Vec<ItemE>>>) {
        let mut remaining = results.len();
        let r = remaining.saturating_sub(1) / MAX_THREAD + 1;
        for _ in 0..MAX_THREAD {
            let data = results.split_off(results.len() - std::cmp::min(r, remaining));
            remaining = remaining.saturating_sub(r);
            let handler = thread::Builder::new()
                .spawn(move || {
                    let mut acc = create_enc();
                    for block in data {
                        combine_enc(&mut acc, batch_encrypt(&block, true));
                    }
                    acc
                }).unwrap();
            handlers.push(handler);
        }
    }

    fn spawn_dec_nar_thread(&self, call_seq: &NextOpId, input: Input, handlers: &mut Vec<JoinHandle<Vec<Vec<Self::Item>>>>, only_dec: bool) {
        let (s, len) = call_seq.para_range.as_ref().unwrap();
        let r = (len.saturating_sub(*s)).saturating_sub(1) / MAX_THREAD + 1;
        let mut b = *s;
        let mut e = std::cmp::min(b + r, *len);
        for _ in 0..MAX_THREAD {
            let op = self.get_op();
            let mut call_seq = call_seq.clone();
            call_seq.para_range = Some((b, e));
            b = e;
            e = std::cmp::min(b + r, *len);
            let handler = thread::Builder::new()
                .spawn(move || {
                    let results = op.compute(&mut call_seq, input).map(|result| result.collect::<Vec<_>>()).collect::<Vec<_>>();
                    if only_dec {
                        assert!(results.into_iter().flatten().collect::<Vec<_>>().is_empty());
                        Vec::new()
                    } else {
                        results
                    }
                }).unwrap();
            handlers.push(handler);
        }
    }

    fn spawn_dec_nar_enc_thread(&self, call_seq: &NextOpId, input: Input, handlers: &mut Vec<JoinHandle<Vec<ItemE>>>, only_dec: bool) {
        let (s, len) = call_seq.para_range.as_ref().unwrap();
        let r = (len.saturating_sub(*s)).saturating_sub(1) / MAX_THREAD + 1;
        let mut b = *s;
        let mut e = std::cmp::min(b + r, *len);
        for _ in 0..MAX_THREAD {
            let op = self.get_op();
            let mut call_seq = call_seq.clone();
            call_seq.para_range = Some((b, e));
            b = e;
            e = std::cmp::min(b + r, *len);
            let handler = thread::Builder::new()
                .spawn(move || {
                    let results = op.compute(&mut call_seq, input).map(|result| result.collect::<Vec<_>>()).collect::<Vec<_>>();
                    if only_dec {
                        assert!(results.into_iter().flatten().collect::<Vec<_>>().is_empty());
                        Vec::new()
                    } else {
                        let mut acc = create_enc();
                        for result in results {
                            combine_enc(&mut acc, batch_encrypt(&result, true));
                        }
                        acc
                    }
                }).unwrap();
            handlers.push(handler);
        }
    }

    fn narrow(&self, mut call_seq: NextOpId, mut input: Input, need_enc: bool) -> *mut u8 {
        let mut acc = create_enc();
        //run the first block for profiling
        let mut results = Vec::new();
        {
            let mut call_seq_sample = call_seq.clone();
            assert!(call_seq_sample.para_range.is_none());
            let sample_data = self.compute(&mut call_seq_sample, input).collect::<Vec<_>>().remove(0).collect::<Vec<_>>();
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = alloc_cnt as f64/(call_seq_sample.sample_len as f64);
            println!("for narrow processing, alloc_cnt per len = {:?}", alloc_cnt_ratio);
            call_seq.para_range = call_seq_sample.para_range;
            call_seq.is_step_para.0 = call_seq_sample.is_step_para.0;
            call_seq.is_step_para.1 = alloc_cnt_ratio < PARA_THRESHOLD;
            //narrow specific
            if need_enc {
                let block_enc = batch_encrypt(&sample_data, true);
                combine_enc(&mut acc, block_enc);
            } else {
                results.push(sample_data);
            }
        };

        let mut handlers_pt =  Vec::with_capacity(MAX_THREAD);
        let mut handlers_ct = Vec::with_capacity(MAX_THREAD);
        if !call_seq.is_step_para.0 && !call_seq.is_step_para.1 {
            results.append(&mut self.compute(&mut call_seq, input).map(|result| result.collect::<Vec<_>>()).collect::<Vec<_>>());
            if need_enc {
                self.spawn_enc_thread(results, &mut handlers_ct);
                results = Vec::new();
            }
        } else if call_seq.is_step_para.0 ^ call_seq.is_step_para.1 {
            //for cache inside, range begin = 0, and for cache outside or no cache, range begin = 1;
            let (s, len) = call_seq.para_range.as_ref().unwrap();
            if *s == 1 {
                //decryption needed
                if !call_seq.is_step_para.0 {
                    let mut call_seq = call_seq.clone();
                    assert!(self.compute(&mut call_seq, input).flatten().collect::<Vec<_>>().is_empty());
                } else {
                    if need_enc {
                        self.spawn_dec_nar_enc_thread(&call_seq, input, &mut handlers_ct, true); 
                    } else {
                        self.spawn_dec_nar_thread(&call_seq, input, &mut handlers_pt, true); 
                    }
                }
                //set cached key
                call_seq.cache_meta.cached_rdd_id = *call_seq.rdd_ids.last().unwrap();
                call_seq.cache_meta.cached_part_id = *call_seq.part_ids.last().unwrap();
                input.data = 0;
            } 
            {
                //narrow
                if !call_seq.is_step_para.1 {
                    for handler in handlers_ct {
                        assert!(handler.join().unwrap().is_empty());
                    }
                    for handler in handlers_pt {
                        handler.join().unwrap();
                    }
                    handlers_ct = Vec::with_capacity(MAX_THREAD);
                    handlers_pt = Vec::with_capacity(MAX_THREAD);
                    let mut call_seq = call_seq.clone();
                    results.append(&mut self.compute(&mut call_seq, input).map(|result| result.collect::<Vec<_>>()).collect::<Vec<_>>());
                    if need_enc {
                        self.spawn_enc_thread(results, &mut handlers_ct);
                        results = Vec::new();
                    }
                } else {
                    if need_enc {
                        self.spawn_dec_nar_enc_thread(&call_seq, input, &mut handlers_ct, false);
                    } else {
                        self.spawn_dec_nar_thread(&call_seq, input, &mut handlers_pt, false);
                    }
                }
            }
        } else {
            if need_enc {
                self.spawn_dec_nar_enc_thread(&call_seq, input, &mut handlers_ct, false);
            } else {
                self.spawn_dec_nar_thread(&call_seq, input, &mut handlers_pt, false);
            }
        }
        for handler in handlers_ct {
            combine_enc(&mut acc, handler.join().unwrap());
        }
        for handler in handlers_pt {
            results.append(&mut handler.join().unwrap());
        }
        //cache to outside
        let key = call_seq.get_caching_doublet();
        CACHE.send(key);
        if need_enc {
            to_ptr(acc)
        } else {
            acc.is_empty();
            crate::ALLOCATOR.set_switch(true);
            drop(acc);
            crate::ALLOCATOR.set_switch(false);
            Box::into_raw(Box::new(results)) as *mut u8
        }
    } 

    fn shuffle(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
        let tid = call_seq.tid;
        let now = Instant::now();

        let opb = self.get_op_base();
        let result_ptr = shuf_dep.do_shuffle_task(tid, opb, call_seq, input);

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("tid: {:?}, shuffle write: {:?}s", tid, dur);
        result_ptr
    }

    fn randomize_in_place_(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        let sample_enc = unsafe{ (input as *const Vec<ItemE>).as_ref() }.unwrap(); 
        let mut sample: Vec<Self::Item> = batch_decrypt(sample_enc, true);
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };
        utils::randomize_in_place(&mut sample, &mut rng);
        sample = sample.into_iter().take(num as usize).collect();
        let sample_enc = batch_encrypt(&sample, true);
        to_ptr(sample_enc)
    }

    fn take_(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        let data_enc = unsafe{ (input as *const Vec<ItemE>).as_ref() }.unwrap(); 
        let mut data: Vec<Self::Item> = batch_decrypt(data_enc, true);
        data = data.into_iter().take(should_take).collect();
        *have_take = data.len();
        let data_enc = batch_encrypt(&data, true);
        to_ptr(data_enc)
    }

    /// Return a new RDD containing only the elements that satisfy a predicate.
    #[track_caller]
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
        let new_op = SerArc::new(MapPartitions::new(self.get_op(), filter_fn));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn map<U, F>(&self, f: F) -> SerArc<dyn Op<Item = U>>
    where
        Self: Sized,
        U: Data,
        F: SerFunc(Self::Item) -> U,
    {
        let new_op = SerArc::new(Mapper::new(self.get_op(), f));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn flat_map<U, F>(&self, f: F) -> SerArc<dyn Op<Item = U>>
    where
        Self: Sized,
        U: Data,
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
    {
        let new_op = SerArc::new(FlatMapper::new(self.get_op(), f));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn reduce<F>(&self, f: F) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();        
        let reduce_partition = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc { 
                None => vec![],
                Some(e) => vec![e],
            }
        });         
        let new_op = SerArc::new(Reduced::new(self.get_op(), reduce_partition));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::<Self::Item, ItemE>::new(None, None))
    }

    //secure_*** are used to receive
    #[track_caller]
    fn secure_reduce<F>(&self, f: F, tail_info: &TailCompInfo) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        Ok(Text::<Self::Item, ItemE>::rec(tail_info))
    }

    #[track_caller]
    fn fold<F>(&self, init: Self::Item, f: F) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                iter.fold(zero.clone(), &cf)
        });
        let new_op = SerArc::new(Fold::new(self.get_op(), reduce_partition));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::<Self::Item, ItemE>::new(None, None))
    }

    #[track_caller]
    fn secure_fold<F>(&self, init: Self::Item, f: F, tail_info: &TailCompInfo) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        Ok(Text::<Self::Item, ItemE>::rec(tail_info))
    }

    #[track_caller]
    fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<Text<U, ItemE>>
    where
        Self: Sized,
        U: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn)
        );
        let zero = init.clone();
        let combine = Fn!(
            move |iter: Box<dyn Iterator<Item = U>>| iter.fold(zero.clone(), &comb_fn)
        );
        let new_op = SerArc::new(Aggregated::new(self.get_op(), reduce_partition, combine));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(Text::<U, ItemE>::new(None, None))
    }

    #[track_caller]
    fn secure_aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF, tail_info: &TailCompInfo) -> Result<Text<U, ItemE>>
    where
        Self: Sized,
        U: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        Ok(Text::<U, ItemE>::rec(tail_info))
    }

    #[track_caller]
    fn collect(&self) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::new(None, None))
    }

    #[track_caller]
    fn secure_collect(&self, tail_info: &TailCompInfo) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::rec(tail_info))
    }
    
    #[track_caller]
    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let new_op = SerArc::new(Count::new(self.get_op()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        Ok(0)
    } 

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
    {
        let mapped = self.map(Box::new(Fn!(|x| (Some(x), None)))
            as Box<
                dyn Func(Self::Item) -> (Option<Self::Item>, Option<Self::Item>),
            >);
        self.get_context().add_num(1);
        let reduced_by_key = mapped.reduce_by_key(Box::new(Fn!(|(_x, y)| y)),
            num_partitions);
        self.get_context().add_num(1);
        reduced_by_key.map(Box::new(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, _y) = x;
            x.unwrap()
        })))
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct(&self) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    #[track_caller]
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        let new_op = SerArc::new(PartitionwiseSampled::new(self.get_op(), sampler, true));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn take_sample(
        &self,
        with_replacement: bool,
        _num: u64,
        _seed: Option<u64>,
    ) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
    where
        Self: Sized,
    {
        let _initial_count = self.count()?;
        let op = self.sample(with_replacement, 0 as f64); //padding
        let _count = op.count()?;
        op.collect()
    }

    #[track_caller]
    fn secure_take_sample(
        &self,
        with_replacement: bool,
        _num: u64,
        _seed: Option<u64>,
        tail_info: &TailCompInfo,
    ) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
    where
        Self: Sized,
    {
        let op = self.sample(with_replacement, 0 as f64); //padding
        let r = op.secure_collect(tail_info);
        r
    }

    #[track_caller]
    fn take(&self, num: usize) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::new(None, None))
    }

    #[track_caller]
    fn secure_take(&self, num: usize, tail_info: &TailCompInfo) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>> 
    where
        Self: Sized,
    {
        Ok(Text::<Vec<Self::Item>, Vec<ItemE>>::rec(tail_info))
    }

    #[track_caller]
    fn union(
        &self,
        other: Arc<dyn Op<Item = Self::Item>>,
    ) -> SerArc<dyn Op<Item = Self::Item>>
    where
        Self: Clone,
    {
        let new_op = SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn Op<Item = Self::Item>>,
            other,
        ]));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn zip<S>(
        &self,
        second: Arc<dyn Op<Item = S>>,
    ) -> SerArc<dyn Op<Item = (Self::Item, S)>>
    where
        Self: Clone,
        S: Data,
    {
        let new_op = SerArc::new(Zipped::new(
            Arc::new(self.clone()) as Arc<dyn Op<Item = Self::Item>>,
            second,
        ));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn key_by<T, F>(&self, func: F) -> SerArc<dyn Op<Item = (T, Self::Item)>>
    where
        Self: Sized,
        T: Data,
        F: SerFunc(&Self::Item) -> T,
    {
        self.map(Fn!(move |k: Self::Item| -> (T, Self::Item) {
            let t = (func)(&k);
            (t, k)
        }))
    }

}