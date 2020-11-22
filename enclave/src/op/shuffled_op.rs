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
use crate::op::{Context, Op, OpE, OpBase, OpVals};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};


pub struct Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> Vec<(KE, CE)> + Clone,
    FD: Func(Vec<(KE, CE)>) -> Vec<(K, C)> + Clone,
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
    FE: Func(Vec<(K, C)>) -> Vec<(KE, CE)> + Clone,
    FD: Func(Vec<(KE, CE)>) -> Vec<(K, C)> + Clone,
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
    FE: Func(Vec<(K, C)>) -> Vec<(KE, CE)> + Clone,
    FD: Func(Vec<(KE, CE)>) -> Vec<(K, C)> + Clone,
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
                fe.clone(),
                fd.clone(),
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
    FE: SerFunc(Vec<(K, C)>) -> Vec<(KE, CE)>,
    FD: SerFunc(Vec<(KE, CE)>) -> Vec<(K, C)>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match is_shuffle {
            0 | 2 => {
                let encrypted = self.get_next_deps().lock().unwrap().is_empty();
                if encrypted {
                    let mut idx = Idx::new();
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, CE)>) };
                    data_enc.send(&mut buf, &mut idx);
                    forget(data_enc);
                } else {
                    let mut idx = Idx::new();
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(K, C)>) };
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
            0 | 2 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<(KE, CE)>) };
                let encrypted = self.get_next_deps().lock().unwrap().is_empty();
                if encrypted {
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, CE)>) };
                    v_out.clone_in_place(&data_enc);
                } else {
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(K, C)>) };
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

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
    }
    
    fn iterator(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(tid, data_ptr, is_shuffle)
    }
}

impl<K, V, C, KE, CE, FE, FD> Op for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> Vec<(KE, CE)>,
    FD: SerFunc(Vec<(KE, CE)>) -> Vec<(K, C)>, 
{
    type Item = (K, C);

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {      //No shuffle
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            }
            2 => {      //Shuffle read
                let aggregator = self.aggregator.clone(); 
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<(KE, CE)>>) };
                // encryption block size: 1
                let data = data_enc.clone()
                    .into_iter()
                    .map(|v |
                        self.get_fd()(v)
                    ).collect::<Vec<_>>();
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
                // encryption block size: 1
                /*
                let result_enc = self.get_fe()(result); 
                let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8; 
                */
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8; 
                result_ptr
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        /*
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<(KE, CE)>) };
        // for group_by, self.fd naturally encrypt/decrypt per row, that is, encryption block size = 1
        let data = self.get_fd()(*(data_enc.clone()));
        forget(data_enc);
        */
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<(K, C)>) };
        Box::new(data.into_iter())
    }
}

impl<K, V, C, KE, CE, FE, FD> OpE for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> Vec<(KE, CE)>,
    FD: SerFunc(Vec<(KE, CE)>) -> Vec<(K, C)>, 
{
    type ItemE = (KE, CE);
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
