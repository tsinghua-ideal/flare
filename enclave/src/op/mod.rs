use core::panic::Location;
use std::any::{Any, TypeId};
use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, hash_map::DefaultHasher, HashMap, HashSet};
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
use std::untrusted::time::InstantEx;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use sgx_types::*;
use rand::{Rng, SeedableRng};

use crate::{BRANCH_OP_HIS, CACHE, Fn, OP_MAP};
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::custom_thread::PThread;
use crate::dependency::{Dependency, OneToOneDependency, ShuffleDependencyTrait};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::utils;
use crate::utils::bounded_priority_queue::BoundedPriorityQueue;
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

type PT<T, TE> = Text<T, TE, 
    Box<dyn Fn(T) -> TE>, 
    Box<dyn Fn(TE) -> T>
    >;
pub type OText<T> = Text<T, T, Box<dyn Fn(T) -> T>, Box<dyn Fn(T) -> T> >; 

pub const MAX_ENC_BL: usize = 1000;
pub type Result<T> = std::result::Result<T, &'static str>;

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
    pub fn ocall_get_addr_map_len(ret_val: *mut usize,
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

fn batch_encrypt<T, TE, FE>(mut data: Vec<T>, fe: FE) -> Vec<TE> 
where
    FE: Func(Vec<T>)->TE
{
    let mut len = data.len();
    let mut data_enc = Vec::with_capacity(len);
    while len >= MAX_ENC_BL {
        len -= MAX_ENC_BL;
        let remain = data.split_off(MAX_ENC_BL);
        let input = data;
        data = remain;
        data_enc.push(fe(input));
    }
    if len != 0 {
        data_enc.push(fe(data));
    }
    data_enc
}

fn batch_decrypt<T, TE, FD>(data_enc: Vec<TE>, fd: FD) -> Vec<T> 
where
    FD: Func(TE)->Vec<T>
{
    let mut data = Vec::new();
    for block in data_enc {
        let mut pt = fd(block);
        data.append(&mut pt); //need to check security
    }
    data
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
#[derive(Clone, Copy, Debug)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    caching_op_id: OpId,
    cached_rdd_id: usize,
    cached_op_id: OpId,
    part_id: usize,
    sub_part_id: usize,
    is_survivor: u8,
}

impl CacheMeta {
    pub fn new(
        caching_rdd_id: usize,
        caching_op_id: OpId,
        cached_rdd_id: usize,
        cached_op_id: OpId,
        part_id: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            caching_op_id,
            cached_rdd_id,
            cached_op_id,
            part_id,
            sub_part_id: 0,
            is_survivor: 0,
        }
    }

    fn set_sub_part_id(&mut self, sub_part_id: usize) {
        self.sub_part_id = sub_part_id;
    }

    pub fn transform(self) -> Self {
        CacheMeta {
            caching_rdd_id: 0,
            caching_op_id: Default::default(),
            cached_rdd_id: self.caching_rdd_id,
            cached_op_id: self.caching_op_id,
            part_id: self.part_id,
            sub_part_id: self.sub_part_id,
            is_survivor: self.is_survivor,
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
        // The last three items is useful only when is_shuffle == 1x, x == 0 or x == 1
        DepInfo {
            is_shuffle,
            identifier,
            parent_rdd_id,
            child_rdd_id,
            parent_op_id,
            child_op_id, 
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

    pub fn dep_type(&self) -> u8 {
        self.is_shuffle / 10
    }

    fn need_encryption(&self) -> bool {
        match self.is_shuffle % 10 {
            0 => false,
            1 => true,
            _ => true,
        }
    }

}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Input {
    data: usize,
    lower: usize,
    upper: usize,
    block_size: usize,
}

impl Input {
    pub fn new<T: Data>(data: &T, lower: &mut Vec<usize>, upper: &mut Vec<usize>, block_size: usize) -> Self {
        let data = data as *const T as usize;
        let lower = lower as *mut Vec<usize> as usize;
        let upper = upper as *mut Vec<usize> as usize;
        Input {
            data,
            lower,
            upper,
            block_size,
        }
    }

    pub fn padding() -> Self {
        Input {
            data: 0,
            lower: 0,
            upper: 0,
            block_size: 0,
        }
    }

    pub fn get_enc_data<T>(&self) -> &T {
        unsafe { (self.data as *const T).as_ref() }.unwrap()
    }

    pub fn get_lower(&self) -> &mut Vec<usize> {
        unsafe { (self.lower as *mut Vec<usize>).as_mut() }.unwrap()
    }

    pub fn get_upper(&self) -> &mut Vec<usize> {
        unsafe { (self.upper as *mut Vec<usize>).as_mut() }.unwrap()
    }

    pub fn get_block_size(&self) -> usize {
        self.block_size
    }

}

#[derive(Debug, Default)]
pub struct Text<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Fn(T) -> TE, 
    FD: Fn(TE) -> T,
{
    data: T,
    id: u64,
    bfe: Option<FE>,
    bfd: Option<FD>,
}

impl<T, TE, FE, FD> Text<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Fn(T) -> TE, 
    FD: Fn(TE) -> T,
{
    #[track_caller]
    pub fn new(data: T, bfe: Option<FE>, bfd: Option<FD>) -> Self {
        let id = get_text_loc_id();
        Text {
            data,
            id,
            bfe,
            bfd,
        }
    }

    #[track_caller]
    pub fn rec(tail_info: &TailCompInfo, bfe: Option<FE>, bfd: Option<FD>) -> Self {
        let id = get_text_loc_id();
        let data = match tail_info.get(id) {
            Some(data_enc) => {
                match &bfd {
                    Some(bfd) => bfd(data_enc),
                    None => {
                        let data_enc = Box::new(data_enc) as Box<dyn Any>;
                        *data_enc.downcast::<T>().unwrap()
                    },
                }
            },
            None => {
                Default::default()
            },
        };

        Text {
            data,
            id,
            bfe,
            bfd,
        }
    }

    pub fn get_ct(&self) -> TE {
        match &self.bfe {
            Some(bfe) => bfe(self.data.clone()),
            None => {
                let data_enc = Box::new(self.data.clone()) as Box<dyn Any>;
                *data_enc.downcast::<TE>().unwrap()
            },
        }
    }

    pub fn get_pt(&self) -> T {
        self.data.clone()
    }

    pub fn get_tail_info(&self) -> (u64, &T) {
        (self.id, &self.data)
    }

    pub fn update_from_tail_info(&mut self, tail_info: &TailCompInfo) {
        let data_enc = tail_info.get(self.id).unwrap();
        self.data = match &self.bfd {
            Some(bfd) => bfd(data_enc),
            None => {
                let data_enc = Box::new(data_enc) as Box<dyn Any>;
                *data_enc.downcast::<T>().unwrap()
            },
        };
    }

}

impl<T, TE, FE, FD> Deref for Text<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Fn(T) -> TE, 
    FD: Fn(TE) -> T,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T, TE, FE, FD> DerefMut for Text<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Fn(T) -> TE, 
    FD: Fn(TE) -> T,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct TailCompInfo {
    m: HashMap<u64, Vec<u8>>,
}

impl TailCompInfo {
    pub fn new() -> Self {
        TailCompInfo {
            m: HashMap::new(),
        }
    }

    pub fn insert<T, TE, FE, FD>(&mut self, text: &Text<T, TE, FE, FD>)
    where
        T: Data,
        TE: Data,
        FE: Fn(T) -> TE, 
        FD: Fn(TE) -> T,
    {
        let ser = bincode::serialize(&text.get_ct()).unwrap();
        self.m.insert(text.id, ser);
    }

    pub fn remove<TE: Data>(&mut self, id: u64) -> TE {
        let ser = self.m.remove(&id).unwrap();
        bincode::deserialize(&ser).unwrap()
    }

    pub fn get<TE: Data>(&self, id: u64) -> Option<TE> {
        match self.m.get(&id) {
            Some(ser) => Some(bincode::deserialize(&ser).unwrap()),
            None => None,
        }
    }

    pub fn clear(&mut self) {
        self.m.clear();
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

pub struct NextOpId<'a> {
    rdd_ids: &'a Vec<usize>,
    op_ids: &'a Vec<OpId>,
    split_nums: Vec<usize>,
    cur_idx: usize,
    cache_meta: CacheMeta,
    captured_vars: HashMap<usize, Vec<Vec<u8>>>,
    is_spec: bool,
}

impl<'a> NextOpId<'a> {
    pub fn new(rdd_ids: &'a Vec<usize>, op_ids: &'a Vec<OpId>, split_nums: Option<&'a Vec<usize>>, cache_meta: CacheMeta, captured_vars: HashMap<usize, Vec<Vec<u8>>>, is_spec: bool) -> Self {
        let split_nums = match split_nums {
            Some(nums) => nums.clone(),
            None => vec![],
        };
        NextOpId {
            rdd_ids,
            op_ids,
            split_nums,
            cur_idx: 0,
            cache_meta,
            captured_vars,
            is_spec,
        }
    }

    fn get_cur_rdd_id(&self) -> usize {
        self.rdd_ids[self.cur_idx]
    }

    fn get_cur_op_id(&self) -> OpId {
        self.op_ids[self.cur_idx]
    }

    fn fix_split_num(&self) {
        if !self.is_spec {
            let op = self.get_cur_op();
            op.fix_split_num(self.split_nums[self.cur_idx]);
        }
    }

    fn pop_reduce_num(&mut self) -> usize {
        self.split_nums.remove(0)
    }

    fn get_part_id(&self) -> usize {
        self.cache_meta.part_id
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

    pub fn get_caching_triplet(&self) -> (usize, usize, usize) {
        (
            self.cache_meta.caching_rdd_id,
            self.cache_meta.part_id,
            self.cache_meta.sub_part_id,
        )
    }

    pub fn get_cached_triplet(&self) -> (usize, usize, usize) {
        (
            self.cache_meta.cached_rdd_id,
            self.cache_meta.part_id,
            self.cache_meta.sub_part_id,
        )
    }

    pub fn have_cache(&self) -> bool {
        self.get_cur_rdd_id() == self.cache_meta.cached_rdd_id
    }

    pub fn need_cache(&self) -> bool {
        self.get_cur_rdd_id() == self.cache_meta.caching_rdd_id
    }

    pub fn is_caching_final_rdd(&self) -> bool {
        self.rdd_ids[0] == self.cache_meta.caching_rdd_id
    }

    pub fn is_survivor(&self) -> bool {
        match self.cache_meta.is_survivor {
            0 => false,
            1 => true,
            _ => panic!("invalid is_survivor"),
        }
    }
}

pub struct SpecOpId {
    cur_op_id: OpId,
    spec_rdd_ids: Vec<usize>,
    spec_op_ids: Vec<OpId>,
    end: bool,
}

impl SpecOpId {
    pub fn new(cache_meta: CacheMeta) -> Self {
        if cache_meta.caching_rdd_id != 0 {
            SpecOpId {
                cur_op_id: cache_meta.caching_op_id,
                spec_rdd_ids: vec![cache_meta.caching_rdd_id],
                spec_op_ids: vec![cache_meta.caching_op_id],
                end: false,
            }
        } else {
            SpecOpId {
                cur_op_id: Default::default(),
                spec_rdd_ids: Vec::new(),
                spec_op_ids: Vec::new(),
                end: true,
            }
        }
    }

    pub fn advance(&mut self) -> bool {
        let parent_op_id = self.cur_op_id;
        let parent_op = load_opmap().get(&parent_op_id).unwrap(); 
        let may_child_op_id = BRANCH_OP_HIS.read()
            .unwrap()
            .get(&parent_op_id)
            .map(|x| x.clone());

        let child_op_id = if may_child_op_id.is_some() && 
            (self.cur_op_id != self.spec_op_ids[0] || self.spec_op_ids.len() == 1) 
        {
            may_child_op_id.unwrap()
        } else {
            self.end = false;
            return true;
        };

        let child_op = load_opmap().get(&child_op_id).unwrap();
        if child_op.has_spec_oppty() {
            self.spec_rdd_ids.push(0);  //just for padding
            self.spec_op_ids.push(child_op_id);
            //or move it to co_grouped, parallel, etc
            //padding with 0, 0
            let dep_info = DepInfo::new(0, 0, 0, 0, parent_op_id, child_op_id);
            if let Some(shuf_dep) = parent_op.get_next_shuf_dep(&dep_info) {
                self.end = true;
            } 
        } else {
            self.end = false;
            return true;
        }
        
        self.cur_op_id = child_op_id;
        self.end
    }

    pub fn get_spec_call_seq(&mut self, dep_info: &DepInfo) -> (Vec<usize>, Vec<OpId>) {
        let len = self.spec_op_ids.len();
        if len < 2 {
            return (Vec::new(), Vec::new());
        }
        let pair = self.spec_op_ids.get((len-2)..len);
        let flag = !(
            pair.is_some()
            && pair.unwrap() == &[dep_info.parent_op_id, dep_info.child_op_id]
            && dep_info.dep_type() == 1
        ) && self.end;
        
        match flag {
            true => {
                self.spec_rdd_ids.reverse();
                self.spec_op_ids.reverse();
                (self.spec_rdd_ids.clone(), self.spec_op_ids.clone())
            },
            false => (Vec::new(), Vec::new()),
        }
    }

    pub fn is_end(&self) -> bool {
        self.end
    }

}

#[derive(Default)]
pub struct Context {
    last_loc_file: RwLock<&'static str>,
    last_loc_line: AtomicU32,
    num: AtomicUsize,
    is_tail_comp: AtomicBool,
}

impl Context {
    pub fn new() -> Result<Arc<Self>> {
        Ok(Arc::new(Context {
            last_loc_file: RwLock::new("null"),
            last_loc_line: AtomicU32::new(0), 
            num: AtomicUsize::new(0),
            is_tail_comp: AtomicBool::new(false),
        }))
    }

    pub fn add_num(self: &Arc<Self>, addend: usize) -> usize {
        self.num.fetch_add(addend, atomic::Ordering::SeqCst)
    }

    pub fn set_num(self: &Arc<Self>, num: usize) {
        self.num.store(num, atomic::Ordering::SeqCst)
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
        op_id
    }

    #[track_caller]
    pub fn make_op<T, TE, FE, FD>(self: &Arc<Self>, fe: FE, fd: FD, num_splits: usize) -> SerArc<dyn OpE<Item = T, ItemE = TE>> 
    where
        T: Data,
        TE: Data,
        FE: SerFunc(Vec<T>) -> TE,
        FD: SerFunc(TE) -> Vec<T>,
    {
        let new_op = SerArc::new(ParallelCollection::new(self.clone(), fe, fd, num_splits));
        if !self.get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    /// Load from a distributed source and turns it into a parallel collection.
    #[track_caller]
    pub fn read_source<C, FE, FD, I: Data, O: Data, OE: Data>(
        self: &Arc<Self>,
        config: C,
        func: Option< Box<dyn Func(I) -> O >>,
        sec_func: Option< Box<dyn Func(I) -> Vec<OE> >>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        C: ReaderConfiguration<I>,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>,
    {
        config.make_reader(self.clone(), func, sec_func, fe, fd)
    }

    #[track_caller]
    pub fn union<T: Data, TE: Data>(rdds: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> impl OpE<Item = T, ItemE = TE> {
        Union::new(rdds)
    }

}

pub(crate) struct OpVals {
    pub id: OpId,
    pub split_num: AtomicUsize,
    pub deps: Vec<Dependency>,
    pub context: Weak<Context>,
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
        }
    }
}

pub trait OpBase: Send + Sync {
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo);
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo);
    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo);
    fn fix_split_num(&self, split_num: usize);
    fn get_op_id(&self) -> OpId;
    fn get_context(&self) -> Arc<Context>;
    fn get_deps(&self) -> Vec<Dependency>;
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>;
    fn get_next_shuf_dep(&self, dep_info: &DepInfo) -> Option<Arc<dyn ShuffleDependencyTrait>> {
        let cur_key = dep_info.get_op_key();
        let next_deps = self.get_next_deps();
        let mut res = None; 
        match next_deps.read().unwrap().get(&cur_key) {
            Some(dep) => match dep {
                Dependency::ShuffleDependency(shuf_dep) => res = Some(shuf_dep.clone()),
                Dependency::NarrowDependency(nar_dep) => res = None,
            },
            None => res = None,
        };
        res
    }
    //supplement
    fn sup_next_shuf_dep(&self, dep_info: &DepInfo) {
        let cur_key = dep_info.get_op_key();
        BRANCH_OP_HIS.write().unwrap().insert(cur_key.0, cur_key.1);
        let next_deps = self.get_next_deps().read().unwrap().clone();
        if next_deps.get(&cur_key).is_none() {
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
        };
    }
    fn or_insert_nar_child(&self, child: OpId) {
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
    //has speculative opportunity
    fn has_spec_oppty(&self) -> bool;
    fn number_of_splits(&self) -> usize;
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8;
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        unreachable!()
    }
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

impl<I: OpE + ?Sized> OpBase for SerArc<I> {
    //need to avoid freeing the encrypted data for subsequent "clone_enc_data_out"
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).build_enc_data_sketch(p_buf, p_data_enc, dep_info);
    }
    //need to free the encrypted data. But how to deal with task failure?
    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        (**self).clone_enc_data_out(p_out, p_data_enc, dep_info);
    }
    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo) {
        (**self).call_free_res_enc(res_ptr, dep_info);
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
    fn has_spec_oppty(&self) -> bool {
        (**self).get_op_base().has_spec_oppty()
    }
    fn number_of_splits(&self) -> usize {
        (**self).get_op_base().number_of_splits()
    }
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        (**self).get_op_base().iterator_start(tid, call_seq, input, dep_info)
    }
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        (**self).randomize_in_place(input, seed, num)
    }
    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        (**self).set_sampler(with_replacement, fraction)
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
    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        (**self).compute_start(tid, call_seq, input, dep_info)
    }
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        (**self).compute(call_seq, input)
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
    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8;
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>);
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
        /*
        let ct_ = unsafe {
            Box::from_raw(ptr as *mut u8 as *mut Vec<Self::ItemE>)
        };
        let ct = ct_.clone();
        forget(ct_);
        self.batch_decrypt(*ct)
        */
        let ct = unsafe {
            (ptr as *const u8 as *const Vec<Self::ItemE>).as_ref()
        }.unwrap();
        self.batch_decrypt(ct.clone())
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
                println!("get cached data inside enclave");
                CACHE.remove_subpid(key.0, key.1, key.2);
                *unsafe {
                    Box::from_raw(val as *mut u8 as *mut Vec<Self::Item>)
                }
            },
            //cache outside enclave
            None => {
                println!("get cached data outside enclave");
                self.cache_from_outside(key)
            },
        };
        val
    }

    fn set_cached_data(&self, is_survivor: bool, is_final: bool, key: (usize, usize, usize), iter: Box<dyn Iterator<Item = Self::Item>>) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {              
        let res = iter.collect::<Vec<_>>();
        //println!("After collect, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        if is_survivor {
            //cache inside enclave
            CACHE.insert(key, Box::into_raw(Box::new(res.clone())) as *mut u8 as usize);
            //println!("After cache inside enclave, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
            CACHE.insert_subpid(key.0, key.1, key.2);
        }
        //cache outside enclave
        if is_final {
            (Box::new(res.into_iter()), None)
        } else {
            let handle = self.cache_to_outside(key, res.clone());
            //println!("After launch encryption thread, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
            (Box::new(res.into_iter()), Some(handle))
        }
    }

    fn step0_of_clone(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match dep_info.dep_type() {
            0 | 2  => {
                let encrypted = dep_info.need_encryption();
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
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
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

    fn step1_of_clone(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Self::ItemE>) };
                let encrypted = dep_info.need_encryption();
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
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
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
        let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL+1);
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
        let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL+1);
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

    fn narrow(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        let (result_iter, handle) = self.compute(call_seq, input);
        /*
        println!("In narrow(before join), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        if let Some(handle) = handle {
            handle.join();
        }
        */
        
        let result = result_iter.collect::<Vec<Self::Item>>();
        //println!("In narrow(before encryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        let result_ptr = match dep_info.need_encryption() {
            true => {
                let now = Instant::now();
                let result_enc = self.batch_encrypt(result); 
                //println!("In narrow(after encryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                //println!("cur mem before copy out: {:?}, encrypt {:?} s", crate::ALLOCATOR.lock().get_memory_usage(), dur); 
                let res_ptr = res_enc_to_ptr(result_enc);
                //println!("cur mem after copy out: {:?}", crate::ALLOCATOR.lock().get_memory_usage()); 
                res_ptr
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

    fn shuffle(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
        if !call_seq.is_spec {
            let reduce_num = call_seq.pop_reduce_num();
            shuf_dep.change_partitioner(reduce_num);
        }
        let (data_iter, handle) = self.compute(call_seq, input);
        let data = data_iter.collect::<Vec<Self::Item>>();
        //let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
        let iter = Box::new(data) as Box<dyn Any>;
        let result_ptr = shuf_dep.do_shuffle_task(iter, call_seq.is_spec);
        if let Some(handle) = handle {
            handle.join();
        }
        result_ptr
    }

    fn randomize_in_place_(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        let sample_enc = unsafe{ (input as *const Vec<Self::ItemE>).as_ref() }.unwrap(); 
        let mut sample = self.batch_decrypt(sample_enc.to_vec());
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };
        utils::randomize_in_place(&mut sample, &mut rng);
        sample = sample.into_iter().take(num as usize).collect();
        let sample_enc = self.batch_encrypt(sample);
        res_enc_to_ptr(sample_enc)
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

    #[track_caller]
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
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
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
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn reduce<F, UE, FE, FD>(&self, f: F, fe: FE, fd: FD) -> Result<PT<Vec<Self::Item>, Vec<UE>>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Vec<Self::Item>) -> UE,
        FD: SerFunc(UE) -> Vec<Self::Item>,
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
        let new_op = SerArc::new(Reduced::new(self.get_ope(), reduce_partition, fe.clone(), fd.clone()));
        insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        let bfe = Box::new(move |data| {
            batch_encrypt(data, fe.clone())
        });
        let bfd = Box::new(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        });
        Ok(Text::new(Default::default(), Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn secure_reduce<F, UE, FE, FD>(&self, f: F, fe: FE, fd: FD, tail_info: &TailCompInfo) -> Result<PT<Vec<Self::Item>, Vec<UE>>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Vec<Self::Item>) -> UE,
        FD: SerFunc(UE) -> Vec<Self::Item>,
    {
        let bfe = Box::new(move |data| {
            batch_encrypt(data, fe.clone())
        });
        let bfd = Box::new(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        });
        Ok(Text::rec(tail_info, Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn fold<F, UE, FE, FD>(&self, init: Self::Item, f: F, fe: FE, fd: FD) -> Result<PT<Vec<Self::Item>, Vec<UE>>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Vec<Self::Item>) -> UE,
        FD: SerFunc(UE) -> Vec<Self::Item>,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                vec![iter.fold(zero.clone(), &cf)]
        });
        let new_op = SerArc::new(Fold::new(self.get_ope(), reduce_partition, fe.clone(), fd.clone()));
        insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        let bfe = Box::new(move |data| {
            batch_encrypt(data, fe.clone())
        });
        let bfd = Box::new(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        });
        Ok(Text::new(Default::default(), Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn secure_fold<F, UE, FE, FD>(&self, init: Self::Item, f: F, fe: FE, fd: FD, tail_info: &TailCompInfo) -> Result<PT<Vec<Self::Item>, Vec<UE>>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Vec<Self::Item>) -> UE,
        FD: SerFunc(UE) -> Vec<Self::Item>,
    {
        let bfe = Box::new(move |data| {
            batch_encrypt(data, fe.clone())
        });
        let bfd = Box::new(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        });
        Ok(Text::rec(tail_info, Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD) -> Result<PT<Vec<U>, Vec<UE>>>
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
        let reduce_partition = Fn!(
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn)
        );
        let zero = init.clone();
        let combine = Fn!(
            move |iter: Box<dyn Iterator<Item = U>>| vec![iter.fold(zero.clone(), &comb_fn)]
        );
        let new_op = SerArc::new(Aggregated::new(self.get_ope(), reduce_partition, combine, fe.clone(), fd.clone()));
        insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        let bfe = Box::new(move |data| {
            batch_encrypt(data, fe.clone())
        });
        let bfd = Box::new(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        });
        Ok(Text::new(Default::default(), Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn secure_aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD, tail_info: &TailCompInfo) -> Result<PT<Vec<U>, Vec<UE>>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let bfe = Box::new(move |data| {
            batch_encrypt(data, fe.clone())
        });
        let bfd = Box::new(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        });
        Ok(Text::rec(tail_info, Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn collect(&self) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>> 
    where
        Self: Sized,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(move |data| batch_encrypt(data, fe.clone()));
        let bfd = Box::new(move |data_enc| batch_decrypt(data_enc, fd.clone()));
        Ok(Text::new(vec![], Some(bfe), Some(bfd)))
    }

    #[track_caller]
    fn secure_collect(&self, tail_info: &TailCompInfo) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>> 
    where
        Self: Sized,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(move |data| batch_encrypt(data, fe.clone()));
        let bfd = Box::new(move |data_enc| batch_decrypt(data_enc, fd.clone()));
        Ok(Text::rec(tail_info, Some(bfe), Some(bfd)))
    }

    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let new_op = SerArc::new(Count::new(self.get_ope()));
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
    ) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
        Self::ItemE: Data + Eq + Hash + Ord,
    {
        let fe_c = self.get_fe();
        let fe_wrapper_mp0 = Box::new(move |v: Vec<(Option<Self::Item>, Option<Self::Item>)>| {
            let (vx, vy): (Vec<Option<Self::Item>>, Vec<Option<Self::Item>>) = v.into_iter().unzip();
            let ct_x = (fe_c)(vx.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>());
            (ct_x, vy)
        }); 
        let fd_c = self.get_fd();
        let fd_wrapper_mp0 = Box::new(move |v: (Self::ItemE, Vec<Option<Self::Item>>)| {
            let (vx, vy) = v;
            let pt_x = (fd_c)(vx).into_iter().map(|x| Some(x));
            pt_x.zip(vy.into_iter()).collect::<Vec<_>>()
        });
        let fe_wrapper_rd = fe_wrapper_mp0.clone();
        let fd_wrapper_rd = fd_wrapper_mp0.clone();
        let fe = self.get_fe();       
        let fe_wrapper_mp1 = Box::new(move |v: Vec<Self::Item>| {
            let ct = (fe)(v);
            ct
        });
        let fd = self.get_fd();
        let fd_wrapper_mp1 = Box::new(move |v: Self::ItemE| {
            let pt = (fd)(v);
            pt
        });
        
        let mapped = self.map(Box::new(Fn!(|x| (Some(x), None)))
            as Box<
                dyn Func(Self::Item) -> (Option<Self::Item>, Option<Self::Item>),
            >, fe_wrapper_mp0, fd_wrapper_mp0);
        self.get_context().add_num(1);
        let reduced_by_key = mapped.reduce_by_key(Box::new(Fn!(|(_x, y)| y)),
            num_partitions,
            fe_wrapper_rd,
            fd_wrapper_rd);
        self.get_context().add_num(1);
        reduced_by_key.map(Box::new(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, _y) = x;
            x.unwrap()
        })), fe_wrapper_mp1, fd_wrapper_mp1)
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct(&self) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash + Ord,
        Self::ItemE: Data + Eq + Hash + Ord,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    #[track_caller]
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        let new_op = SerArc::new(PartitionwiseSampled::new(self.get_op(), sampler, true, self.get_fe(), self.get_fd()));
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
    ) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>>
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
    ) -> Result<PT<Vec<Self::Item>, Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        let op = self.sample(with_replacement, 0 as f64); //padding
        let r = op.secure_collect(tail_info);
        r
    }

    #[track_caller]
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
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
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
