use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;

use crate::CAVE;
use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Data, Func, SerFunc };
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::op::*;
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};

#[derive(Clone)]
pub struct CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    WE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))> + Clone, 
    FD: Func(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    pub(crate) op0: Arc<dyn OpE<Item = (K, V), ItemE = (KE, VE)>>,
    pub(crate) op1: Arc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
    pub(crate) part: Box<dyn Partitioner>,
    fe: FE,
    fd: FD,
}

impl<K, V, W, KE, VE, WE, FE, FD> CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    WE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))> + Clone, 
    FD: Func(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone, 
{
    pub fn new(op0: Arc<dyn OpE<Item = (K, V), ItemE = (KE, VE)>>,
               op1: Arc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
               fe: FE,
               fd: FD,
               part: Box<dyn Partitioner>) -> Self 
    {
        let context = op1.get_context();
        let mut vals = OpVals::new(context.clone());
        let mut deps = Vec::new();
             
        let mut prev_ids = op0.get_prev_ids();
        prev_ids.insert(op0.get_id()); 
        if op0
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids.clone())) as Arc<dyn NarrowDependencyTrait>,
            ));
            op0.get_next_deps().lock().unwrap().push(
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(prev_ids.clone()))
                )
            );
        } else {
            let fe = op0.get_fe();
            let fe_wrapper = move |v: Vec<(K, Vec<V>)>| {
                let mut ct = Vec::with_capacity(v.len());
                for (x, vy) in v {
                    let (ct_x, ct_y): (Vec<KE>, Vec<VE>) = (fe)(vy
                        .into_iter()
                        .map(|y| (x.clone(), y))
                        .collect::<Vec<_>>())
                        .into_iter()
                        .unzip();
                    ct.push((ct_x[0].clone(), ct_y));
                } 
                ct
            };

            let fd = op0.get_fd();
            let fd_wrapper = move |v: Vec<(KE, Vec<VE>)>| {
                let mut pt = Vec::with_capacity(v.len());
                for (x, vy) in v {
                    let (pt_x, pt_y): (Vec<K>, Vec<V>) = (fd)(vy
                        .into_iter()
                        .map(|y| (x.clone(), y))
                        .collect::<Vec<_>>())
                        .into_iter()
                        .unzip();
                    pt.push((pt_x[0].clone(), pt_y));
                }
                pt
            };

            let aggr = Arc::new(Aggregator::<K, V, _>::default());
            deps.push(Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr.clone(),
                    part.clone(),
                    prev_ids.clone(),
                    fe_wrapper.clone(),
                    fd_wrapper.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
            op0.get_next_deps().lock().unwrap().push(
                Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        true,
                        aggr,
                        part.clone(),
                        prev_ids,
                        fe_wrapper,
                        fd_wrapper,
                    ))
                )
            );
        }


        let mut prev_ids = op1.get_prev_ids();
        prev_ids.insert(op1.get_id()); 
        if op1
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids.clone())) as Arc<dyn NarrowDependencyTrait>,
            ));
            op1.get_next_deps().lock().unwrap().push(
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(prev_ids.clone()))
                )
            ); 
        } else {
            let fe = op1.get_fe();
            let fe_wrapper = move |v: Vec<(K, Vec<W>)>| {
                let mut ct = Vec::with_capacity(v.len());
                for (x, vy) in v {
                    let (ct_x, ct_y): (Vec<KE>, Vec<WE>) = (fe)(vy
                        .into_iter()
                        .map(|y| (x.clone(), y))
                        .collect::<Vec<_>>())
                        .into_iter()
                        .unzip();
                    ct.push((ct_x[0].clone(), ct_y));
                } 
                ct
            };

            let fd = op1.get_fd();
            let fd_wrapper = move |v: Vec<(KE, Vec<WE>)>| {
                let mut pt = Vec::with_capacity(v.len());
                for (x, vy) in v {
                    let (pt_x, pt_y): (Vec<K>, Vec<W>) = (fd)(vy
                        .into_iter()
                        .map(|y| (x.clone(), y))
                        .collect::<Vec<_>>())
                        .into_iter()
                        .unzip();
                    pt.push((pt_x[0].clone(), pt_y));
                }
                pt
            };

            let aggr = Arc::new(Aggregator::<K, W, _>::default());
            deps.push(Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr.clone(),
                    part.clone(),
                    prev_ids.clone(),
                    fe_wrapper.clone(),
                    fd_wrapper.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
            op1.get_next_deps().lock().unwrap().push(
                Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        true,
                        aggr,
                        part.clone(),
                        prev_ids,
                        fe_wrapper,
                        fd_wrapper,
                    ))
                )
            );
        }
        
        vals.deps = deps;
        let vals = Arc::new(vals);
        CoGrouped {
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            op0,
            op1,
            fe,
            fd,
            part,
        }
    }
}

impl<K, V, W, KE, VE, WE, FE, FD> OpBase for CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>, 
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
        match is_shuffle {
            0 | 2  => {
                let encrypted = self.get_next_deps().lock().unwrap().is_empty();
                if encrypted {
                    let mut idx = Idx::new();
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, (Vec<VE>, Vec<WE>))>) };
                    data_enc.send(&mut buf, &mut idx);
                    forget(data_enc);
                } else {
                    let mut idx = Idx::new();
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(K, (Vec<V>, Vec<W>))>) };
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
            20 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, Vec<VE>)>) };
                data_enc.send(&mut buf, &mut idx);
                println!("buf = {:?}", buf);
                forget(data_enc);
            },
            21 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE,  Vec<WE>)>) };
                data_enc.send(&mut buf, &mut idx);
                println!("buf = {:?}", buf);
                forget(data_enc);
            },
            22 => {
                let mut idx = Idx::new();
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Vec<usize>>) };
                data_enc.send(&mut buf, &mut idx);
                forget(data_enc);
            },
            _ => panic!("invalid is_shuffle"),
        } 

        forget(buf);
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 2 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<(KE, (Vec<VE>, Vec<WE>))>) };
                let encrypted = self.get_next_deps().lock().unwrap().is_empty();
                if encrypted {
                    let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, (Vec<VE>, Vec<WE>))>) };
                    v_out.clone_in_place(&data_enc);
                } else {
                    let data = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(K, (Vec<V>, Vec<W>))>) };
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
            20 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<(KE, Vec<VE>)>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, Vec<VE>)>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            },
            21 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<(KE, Vec<WE>)>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<(KE, Vec<WE>)>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
            },
            22 => {
                let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Vec<usize>>) };
                let data_enc = unsafe{ Box::from_raw(p_data_enc as *mut Vec<Vec<usize>>) };
                v_out.clone_in_place(&data_enc);
                forget(v_out);
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
        let part = self.part.clone() as Box<dyn Partitioner>;
        Some(part)
    }
    
    fn iterator(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8{
        println!("co_grouped iterator");
        self.compute_start(tid, data_ptr, is_shuffle)
    }

}

impl<K, V, W, KE, VE, WE, FE, FD> Op for CoGrouped<K, V, W, KE, VE, WE, FE, FD>
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>, 
{
    type Item = (K, (Vec<V>, Vec<W>));  
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            },
            2 => {      //shuffle read
                let mut dur_sum = 0.0;
                let data_enc = unsafe{ 
                    Box::from_raw(data_ptr as *mut (
                        Vec<Vec<(KE, VE)>>, 
                        Vec<Vec<(KE, Vec<VE>)>>, 
                        Vec<Vec<(KE, WE)>>, 
                        Vec<Vec<(KE, Vec<WE>)>>
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
                    Dependency::ShuffleDependency(_shuf) => {
                        for sub_data_enc in data_enc.1.clone() {  
                            let now = Instant::now();
                            let mut pt = Vec::with_capacity(sub_data_enc.len());
                            for (x, vy) in sub_data_enc {
                                let (pt_x, pt_y): (Vec<K>, Vec<V>) = self.op0.get_fd()(vy
                                    .into_iter()
                                    .map(|y| (x.clone(), y))
                                    .collect::<Vec<_>>())
                                    .into_iter()
                                    .unzip();
                                pt.push((pt_x[0].clone(), pt_y));
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
                    Dependency::ShuffleDependency(_shuf) => {
                        for sub_data_enc in data_enc.3.clone() { 
                            let now = Instant::now();
                            let mut pt = Vec::with_capacity(sub_data_enc.len());
                            for (x, vy) in sub_data_enc {
                                let (pt_x, pt_y): (Vec<K>, Vec<W>) = self.op1.get_fd()(vy
                                    .into_iter()
                                    .map(|y| (x.clone(), y))
                                    .collect::<Vec<_>>())
                                    .into_iter()
                                    .unzip();
                                pt.push((pt_x[0].clone(), pt_y));
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

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        //encryption block size: 1
        /*
        let now = Instant::now();
        let agg = unsafe{ Box::from_raw(data_ptr as *mut Vec<(KE, (Vec<Vec<VE>>, Vec<Vec<WE>>))>) };
        let data = agg.iter()
            .map(|(ke, (vve, vwe))| {
                let (k, _v) = self.op0.get_fd()(vec![(ke.clone(), vve[0][0].clone())]).pop().unwrap();
                let v = vve.iter().map( |ve| {
                    let (pt_k, pt_v): (Vec<K>, Vec<V>) = self.op0.get_fd()(ve
                            .iter()
                            .map(|i| (ke.clone(), i.clone()))
                            .collect::<Vec<_>>()
                        ).into_iter()
                        .unzip();
                    pt_v
                }).flatten()
                .collect::<Vec<_>>();
                let w = vwe.iter().map( |we| {
                    let (pt_k, pt_w): (Vec<K>, Vec<W>) = self.op1.get_fd()(we
                            .iter()
                            .map(|i| (ke.clone(), i.clone()))
                            .collect::<Vec<_>>()
                        ).into_iter()
                        .unzip();
                    pt_w
                }).flatten()
                .collect::<Vec<_>>();
                (k, (v, w))
            }).collect::<Vec<_>>();
        forget(agg);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave decrypt {:?} s", dur);
        */
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<(K, (Vec<V>, Vec<W>))>) };
        Box::new(data.into_iter())
    }
}

impl<K, V, W, KE, VE, WE, FE, FD> OpE for CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>,
{
    type ItemE = (KE, (Vec<VE>, Vec<WE>));
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