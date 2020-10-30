use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::{drop, forget};
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;

use crate::aggregator::Aggregator;
use crate::basic::{Arc as SerArc, AnyData, Data};
use crate::op::*;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::partitioner::Partitioner;

#[derive(Clone)]
pub struct CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
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
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
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
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>, 
{
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
    
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }

}

impl<K, V, W, KE, VE, WE, FE, FD> Op for CoGrouped<K, V, W, KE, VE, WE, FE, FD>
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
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

    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        let next_deps = self.next_deps.lock().unwrap();
        match is_shuffle == 0 {
            true => {       //No shuffle later
                self.narrow(data_ptr)
            },
            false => {      //Shuffle later
                self.shuffle(data_ptr)
            },
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let mut agg: HashMap<K, (Vec<V>, Vec<W>)> = HashMap::new();
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<usize>>) };
        let deps = self.get_deps();
        match &deps[0] {
            Dependency::NarrowDependency(nar) => {
                for block_ptr in &data[0] {
                    let block_ptr = *block_ptr as *mut u8 as *mut Vec<(KE, VE)>;
                    let data0_enc  = unsafe{ Box::from_raw(block_ptr) };
                    let data0 = self.op0.get_fd()(*(data0_enc.clone()));
                    for i in data0.into_iter() { 
                        let (k, v) = i;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new())).0
                            .push(v);
                    }
                    forget(data0_enc);
                }
            },
            Dependency::ShuffleDependency(shuf) => {
                for data_ptr in &data[0] {   //cycle == 1
                    let data_ptr = *data_ptr as *mut u8 as *mut Vec<(KE, Vec<VE>)>;
                    let data0_enc = unsafe{ Box::from_raw(data_ptr) };
                    //decrypt
                    let mut pt = Vec::with_capacity(data0_enc.len());
                    for (x, vy) in *(data0_enc.clone()) {
                        let (pt_x, pt_y): (Vec<K>, Vec<V>) = self.op0.get_fd()(vy
                            .into_iter()
                            .map(|y| (x.clone(), y))
                            .collect::<Vec<_>>())
                            .into_iter()
                            .unzip();
                        pt.push((pt_x[0].clone(), pt_y));
                    }
                    let data0 = pt;
                    for (k, c) in data0.into_iter() { 
                        let temp = agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()));
                        for v in c {
                            temp.0.push(v);
                        }
                    }
                    forget(data0_enc);
                }
            },
        };

        match &deps[1] {
            Dependency::NarrowDependency(nar) => {
                for block_ptr in &data[1] {
                    let block_ptr = *block_ptr as *mut u8 as *mut Vec<(KE, WE)>;
                    let data1_enc = unsafe{ Box::from_raw(block_ptr) };
                    let data1 = self.op1.get_fd()(*(data1_enc.clone())); //need to check security
                    for i in data1.into_iter() {
                        let (k, w) = i;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new())).1
                            .push(w);
                    }
                    forget(data1_enc);
                }
            },
            Dependency::ShuffleDependency(shuf) => {
                for data_ptr in &data[1] {   //cycle == 1
                    let data_ptr = *data_ptr as *mut u8 as *mut Vec<(KE, Vec<WE>)>;
                    let data1_enc = unsafe{ Box::from_raw(data_ptr) };
                    //decrypt
                    let mut pt = Vec::with_capacity(data1_enc.len());
                    for (x, vy) in *(data1_enc.clone()) {
                        let (pt_x, pt_y): (Vec<K>, Vec<W>) = self.op1.get_fd()(vy
                            .into_iter()
                            .map(|y| (x.clone(), y))
                            .collect::<Vec<_>>())
                            .into_iter()
                            .unzip();
                        pt.push((pt_x[0].clone(), pt_y));
                    }
                    let data1 = pt;
                    for (k, c) in data1.into_iter() { 
                        let temp = agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()));
                        for w in c {
                            temp.1.push(w);
                        }
                    }
                    forget(data1_enc);
                }
            },
        };
        forget(data);
        Box::new(agg.into_iter())
    }
}

impl<K, V, W, KE, VE, WE, FE, FD> OpE for CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
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