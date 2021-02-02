use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use crate::CAVE;
use crate::aggregator::Aggregator;
use crate::dependency::{
    NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::op::*;

#[derive(Clone)]
pub struct CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
    FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    pub(crate) op0: Arc<dyn OpE<Item = (K, V), ItemE = (KE, VE)>>,
    pub(crate) op1: Arc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
    pub(crate) part: Box<dyn Partitioner>,
    fe: FE,
    fd: FD,
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where
K: Data + Eq + Hash + Ord,
V: Data,
W: Data,
KE: Data + Eq + Hash + Ord,
VE: Data,
CE: Data,
WE: Data,
DE: Data,
FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
    #[track_caller]
    pub fn new(op0: Arc<dyn OpE<Item = (K, V), ItemE = (KE, VE)>>,
               op1: Arc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
               fe: FE,
               fd: FD,
               part: Box<dyn Partitioner>) -> Self 
    {
        let context = op1.get_context();
        let mut vals = OpVals::new(context.clone());
        let mut deps = Vec::new();
        let cur_id = vals.id;
        let op0_id = op0.get_op_id();
        let op1_id = op1.get_op_id();
             
        if op0
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(op0_id, cur_id)) as Arc<dyn NarrowDependencyTrait>,
            ));
            op0.get_next_deps().write().unwrap().insert(
                (op0_id, cur_id),
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(op0_id, cur_id))
                )
            );
        } else {
            //TODO need revision if fe & fd of group_by is passed 
            let fe = op0.get_fe();
            let fe_wrapper = Box::new(move |v: Vec<(K, Vec<V>)>| {
                let (x, y): (Vec<K>, Vec<Vec<V>>) = v.into_iter().unzip();
                let mut y_padding = Vec::new();
                y_padding.resize_with(x.len(), Default::default);
                let (ct_x, _) = (fe)(x.into_iter()
                    .zip(y_padding.into_iter())
                    .collect::<Vec<_>>()
                );
                (ct_x, ser_encrypt(y))
            });

            let fd = op0.get_fd();
            let fd_wrapper = Box::new(move |v: (KE, Vec<u8>)| {
                let (x, y) = v;
                let y_padding: VE = Default::default();
                let (pt_x, _): (Vec<K>, Vec<V>) = (fd)((x, y_padding)).into_iter().unzip();
                let pt_y: Vec<Vec<V>> = ser_decrypt(y);
                pt_x.into_iter()
                    .zip(pt_y.into_iter())
                    .collect::<Vec<_>>()
            });

            let aggr = Arc::new(Aggregator::<K, V, _>::default());
            let dep = Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                    0,
                    op0_id,
                    cur_id,
                    fe_wrapper,
                    fd_wrapper,
                )) as Arc<dyn ShuffleDependencyTrait>,
            );
            deps.push(dep.clone());
            op0.get_next_deps().write().unwrap().insert(
                (op0_id, cur_id),
                dep,
            );
        }

        if op1
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(op1_id, cur_id)) as Arc<dyn NarrowDependencyTrait>,
            ));
            op1.get_next_deps().write().unwrap().insert(
                (op1_id, cur_id),
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(op1_id, cur_id))
                )
            ); 
        } else {
            //TODO need revision if fe & fd of group_by is passed 
            let fe = op1.get_fe();
            let fe_wrapper = Box::new(move |v: Vec<(K, Vec<W>)>| {
                let (x, y): (Vec<K>, Vec<Vec<W>>) = v.into_iter().unzip();
                let mut y_padding = Vec::new();
                y_padding.resize_with(x.len(), Default::default);
                let (ct_x, _) = (fe)(x.into_iter()
                    .zip(y_padding.into_iter())
                    .collect::<Vec<_>>()
                );
                (ct_x, ser_encrypt(y))
            });

            let fd = op1.get_fd();
            let fd_wrapper = Box::new(move |v: (KE, Vec<u8>)| {
                let (x, y) = v;
                let y_padding: WE = Default::default();
                let (pt_x, _): (Vec<K>, Vec<W>) = (fd)((x, y_padding)).into_iter().unzip();
                let pt_y: Vec<Vec<W>> = ser_decrypt(y);
                pt_x.into_iter()
                    .zip(pt_y.into_iter())
                    .collect::<Vec<_>>()
            });

            let aggr = Arc::new(Aggregator::<K, W, _>::default());
            let dep = Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                    1,
                    op1_id,
                    cur_id,
                    fe_wrapper,
                    fd_wrapper,
                )) as Arc<dyn ShuffleDependencyTrait>,
            );
            deps.push(dep.clone());
            op1.get_next_deps().write().unwrap().insert(
                (op1_id, cur_id),
                dep,
            );
        }
        
        vals.deps = deps;
        let vals = Arc::new(vals);
        CoGrouped {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            op0,
            op1,
            fe,
            fd,
            part,
        }
    }
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> OpBase for CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
    FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 | 2  => self.step0_of_clone(p_buf, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 | 2 => self.step1_of_clone(p_out, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 => self.free_res_enc(res_ptr),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn get_op_id(&self) -> OpId {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }

    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        self.next_deps.clone()
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        let part = self.part.clone() as Box<dyn Partitioner>;
        Some(part)
    }

    fn has_spec_oppty(&self) -> bool {
        true
    }
    
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8{
        
		self.compute_start(tid, call_seq, data_ptr, dep_info)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = (K, (Vec<V>, Vec<W>))>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (K, (Vec<V>, Vec<W>))>;
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

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> Op for CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD>
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>, 
{
    type Item = (K, (Vec<V>, Vec<W>));  
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {       //No shuffle
                self.narrow(call_seq, data_ptr, dep_info)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, data_ptr, dep_info)
            },
            2 => {      //shuffle read
                let mut dur_sum = 0.0;
                //TODO need revision if fe & fd of group_by is passed 
                let data_enc = unsafe { 
                    Box::from_raw(data_ptr as *mut (
                        Vec<(KE, VE)>, 
                        Vec<Vec<(KE, Vec<u8>)>>, 
                        Vec<(KE, WE)>, 
                        Vec<Vec<(KE, Vec<u8>)>>
                    ))
                };
                let remained_ptr = CAVE.lock().unwrap().remove(&tid);
                let mut agg: BTreeMap<K, (Vec<V>, Vec<W>)> = match remained_ptr {
                    Some(ptr) => *unsafe { Box::from_raw(ptr as *mut u8 as *mut BTreeMap<K, (Vec<V>, Vec<W>)>) },
                    None => BTreeMap::new(),
                };
                let deps = self.get_deps();
                let mut min_max_k_tree: BTreeSet<K> = BTreeSet::new();
                match &deps[0] {
                    Dependency::NarrowDependency(_nar) => {
                        for sub_data_enc in data_enc.0.clone() {
                            let data0 = self.op0.get_fd()(sub_data_enc);
                            match data0.last() {
                                Some(v) => min_max_k_tree.insert(v.0.clone()),
                                None => false,
                            };
                            for i in data0.into_iter() { 
                                let (k, v) = i;
                                agg.entry(k)
                                    .or_insert_with(|| (Vec::new(), Vec::new())).0
                                    .push(v);
                            }
                        }
                    },
                    Dependency::ShuffleDependency(shuf) => {
                        //TODO need revision if fe & fd of group_by is passed 
                        let s = shuf.downcast_ref::<ShuffleDependency<K, V, Vec<V>, KE, Vec<u8>>>().unwrap();
                        for sub_data_enc in data_enc.1.clone() {  
                            let now = Instant::now();
                            let mut pt = Vec::with_capacity(sub_data_enc.len());
                            for block in sub_data_enc {
                                let mut pt_b = (s.fd)(block);
                                pt.append(&mut pt_b); //need to check security
                            }

                            let data0 = pt;
                            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                            dur_sum += dur;  
                            match data0.last() {
                                Some(v) => min_max_k_tree.insert(v.0.clone()),
                                None => false,
                            };
                            for (k, c) in data0.into_iter() { 
                                let temp = agg.entry(k)
                                    .or_insert_with(|| (Vec::new(), Vec::new()));
                                for v in c {
                                    temp.0.push(v);
                                }
                            }
                        }
                    },
                };
        
                match &deps[1] {
                    Dependency::NarrowDependency(_nar) => {
                        for sub_data_enc in data_enc.2.clone() {
                            let data1 = self.op1.get_fd()(sub_data_enc); //need to check security
                            match data1.last() {
                                Some(v) => min_max_k_tree.insert(v.0.clone()),
                                None => false,
                            };
                            for i in data1.into_iter() {
                                let (k, w) = i;
                                agg.entry(k)
                                    .or_insert_with(|| (Vec::new(), Vec::new())).1
                                    .push(w);
                            }
                        }
                    },
                    Dependency::ShuffleDependency(shuf) => {
                        //TODO need revision if fe & fd of group_by is passed 
                        let s = shuf.downcast_ref::<ShuffleDependency<K, W, Vec<W>, KE, Vec<u8>>>().unwrap();
                        for sub_data_enc in data_enc.3.clone() { 
                            let now = Instant::now();
                            let mut pt = Vec::with_capacity(sub_data_enc.len());
                            for block in sub_data_enc {
                                let mut pt_b = (s.fd)(block);
                                pt.append(&mut pt_b); //need to check security
                            }

                            let data1 = pt;
                            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                            dur_sum += dur;
                            match data1.last() {
                                Some(v) => min_max_k_tree.insert(v.0.clone()),
                                None => false,
                            };
                            for (k, c) in data1.into_iter() { 
                                let temp = agg.entry(k)
                                    .or_insert_with(|| (Vec::new(), Vec::new()));
                                for w in c {
                                    temp.1.push(w);
                                }
                            }
                        }
                    },
                };
                forget(data_enc);
                println!("in enclave decrypt {:?}", dur_sum);  
                let min_max_k = min_max_k_tree.first();
                if min_max_k.is_some() {
                    let remained = agg.split_off(min_max_k.unwrap());
                    CAVE.lock().unwrap().insert(tid, Box::into_raw(Box::new(remained)) as *mut u8 as usize);
                }
                let result = agg.into_iter()
                    .filter(|(k, (v, w))| v.len() != 0 && w.len() != 0)
                    .collect::<Vec<_>>();
                Box::into_raw(Box::new(result)) as *mut u8
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_triplet();
            let val = self.get_and_remove_cached_data(key);
            return (Box::new(val.into_iter()), None); 
        }
        
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<(K, (Vec<V>, Vec<W>))>) };
        let res_iter = Box::new(data.into_iter());
        
        if need_cache {
            let key = call_seq.get_caching_triplet();
            if CACHE.get(key).is_none() { 
                return self.set_cached_data(
                    call_seq.is_survivor(),
                    call_seq.is_caching_final_rdd(),
                    key,
                    res_iter
                );
            }
        }

        (res_iter, None)
    }
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> OpE for CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>,
{
    type ItemE = (KE, (CE, DE));
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