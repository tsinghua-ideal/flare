use std::boxed::Box;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;

use crate::CAVE;
use crate::aggregator::Aggregator;
use crate::basic::{Data, Func, SerFunc};
use crate::dependency::{Dependency, ShuffleDependency, ShuffleDependencyTrait};
use crate::op::{CacheMeta, Context, Op, OpE, OpBase, OpVals};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::custom_thread::PThread;

pub struct Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    parent: Arc<dyn Op<Item = (K, V)>>,
    aggregator: Arc<Aggregator<K, V, C>>,
    part: Box<dyn Partitioner>,
    fe: FE,
    fd: FD,
}

impl<K, V, C, KE, CE, FE, FD> Clone for Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    fn clone(&self) -> Self {
        Shuffled {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            part: self.part.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    pub(crate) fn new(
        parent: Arc<dyn Op<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let mut prev_ids = parent.get_prev_ids();
        prev_ids.insert(parent.get_id());
        let dep = Dependency::ShuffleDependency(Arc::new(
            ShuffleDependency::new(
                false,
                aggregator.clone(),
                part.clone(),
                prev_ids,
                Box::new(fe.clone()),
                Box::new(fd.clone()),
            ),
        ));
        let ctx = parent.get_context();
        let mut vals = OpVals::new(ctx);
        vals.deps.push(dep.clone());
        let vals = Arc::new(vals);
        parent.get_next_deps().lock().unwrap().push(dep);
        Shuffled {
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            parent,
            aggregator,
            part,
            fe,
            fd,
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> OpBase for Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 | 2  => self.step0_of_clone(p_buf, p_data_enc, is_shuffle),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 | 2 => self.step1_of_clone(p_out, p_data_enc, is_shuffle),
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 2 => self.free_res_enc(res_ptr),
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

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
    }
    
    fn iterator(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        self.compute_start(tid, data_ptr, is_shuffle, cache_meta)
    }
}

impl<K, V, C, KE, CE, FE, FD> Op for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
    type Item = (K, C);

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        match is_shuffle {
            0 => {      //No shuffle
                self.narrow(data_ptr, cache_meta)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr, cache_meta)
            }
            2 => {      //Shuffle read
                let aggregator = self.aggregator.clone(); 
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<(KE, CE)>>) };
                let data = data_enc.clone()
                    .into_iter()
                    .map(|v | {
                        //batch_decrypt
                        let mut data = Vec::new();
                        for block in v {
                            let mut pt = self.get_fd()(block);
                            data.append(&mut pt); //need to check security
                        }
                        data
                    }).collect::<Vec<_>>();
                forget(data_enc);
                let remained_ptr = CAVE.lock().unwrap().remove(&tid);
                let mut combiners: BTreeMap<K, Option<C>> = match remained_ptr {
                    Some(ptr) => *unsafe { Box::from_raw(ptr as *mut u8 as *mut BTreeMap<K, Option<C>>) },
                    None => BTreeMap::new(),
                };
                if data.len() > 0 {    
                    let mut min_max_kv = data[0].last().unwrap(); 
                    for idx in 1..data.len() {
                        let cand = data[idx].last().unwrap();
                        if min_max_kv.0 > cand.0 {
                            min_max_kv = cand;
                        }
                    }
                    let min_max_k = min_max_kv.0.clone();

                    for (k, c) in data.into_iter().flatten() {
                        if let Some(old_c) = combiners.get_mut(&k) {
                            let old = old_c.take().unwrap();
                            let input = ((old, c),);
                            let output = aggregator.merge_combiners.call(input);
                            *old_c = Some(output);
                        } else {
                            combiners.insert(k, Some(c));
                        }
                    }
                    let remained = combiners.split_off(&min_max_k);
                    //Temporary stored for next computation
                    CAVE.lock().unwrap().insert(tid, Box::into_raw(Box::new(remained)) as *mut u8 as usize);
                }
                let result = combiners.into_iter().map(|(k, v)| (k, v.unwrap())).collect::<Vec<Self::Item>>();
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8; 
                result_ptr
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<(K, C)>) };
        (Box::new(data.into_iter()), None)
    }
}

impl<K, V, C, KE, CE, FE, FD> OpE for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
    type ItemE = (KE, CE);
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
