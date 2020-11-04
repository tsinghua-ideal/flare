use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::forget;
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
    KE: Data + Eq + Hash,
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
    KE: Data + Eq + Hash,
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
    KE: Data + Eq + Hash,
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
    
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8{
        println!("co_grouped iterator");
        self.compute_start(data_ptr, is_shuffle)
    }

}

impl<K, V, W, KE, VE, WE, FE, FD> Op for CoGrouped<K, V, W, KE, VE, WE, FE, FD>
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
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
        match is_shuffle {
            0 => {       //No shuffle
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            },
            2 => {      //shuffle read
                let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<usize>>) };
                let mut data_t_ptr = Vec::new();
                let deps = self.get_deps();
                match &deps[0] {
                    Dependency::NarrowDependency(_nar) => {
                        let mut ptr_per_part = Vec::new();
                        for block_ptr in &data[0] {
                            let ptr_per_subpart = *block_ptr as *mut u8 as *mut Vec<(KE, VE)>;
                            let data0_enc  = unsafe{ Box::from_raw(ptr_per_subpart) };
                            // batch decrypt
                            let data0 = self.op0.batch_decrypt(&data0_enc);
                            // individual encrypt
                            let mut data0_t = Vec::with_capacity(data0.len()); 
                            for i in data0 {
                                data0_t.append(
                                    &mut self.op0.get_fe()(vec![i])
                                        .into_iter()
                                        .map(|(ke, ve)| (ke, vec![ve]))
                                        .collect::<Vec<_>>()
                                );
                            }
                            //copy out
                            crate::ALLOCATOR.lock().set_switch(true);
                            let result = data0_t.clone(); 
                            let result_ptr = Box::into_raw(Box::new(result)) as *mut u8 as usize;
                            crate::ALLOCATOR.lock().set_switch(false);
                            forget(data0_enc);
                            ptr_per_part.push(result_ptr);
                        }
                        data_t_ptr.push(ptr_per_part);
                    },
                    Dependency::ShuffleDependency(_shuf) => {
                        data_t_ptr.push(data[0].clone());
                    },
                };
                match &deps[1] {
                    Dependency::NarrowDependency(_nar) => {
                        let mut ptr_per_part = Vec::new();
                        for block_ptr in &data[1] {
                            let ptr_per_subpart = *block_ptr as *mut u8 as *mut Vec<(KE, WE)>;
                            let data1_enc  = unsafe{ Box::from_raw(ptr_per_subpart) };
                            // batch decrypt
                            let data1 = self.op1.batch_decrypt(&data1_enc);
                            // individual encrypt
                            let mut data1_t = Vec::with_capacity(data1.len()); 
                            for i in data1 {
                                data1_t.append(
                                    &mut self.op1.get_fe()(vec![i])
                                    .into_iter()
                                    .map(|(ke, we)| (ke, vec![we]))
                                    .collect::<Vec<_>>()
                                );
                            }
                            //copy out
                            crate::ALLOCATOR.lock().set_switch(true);
                            let result = data1_t.clone(); 
                            let result_ptr = Box::into_raw(Box::new(result)) as *mut u8 as usize;
                            crate::ALLOCATOR.lock().set_switch(false);
                            forget(data1_enc);
                            ptr_per_part.push(result_ptr);
                        }
                        data_t_ptr.push(ptr_per_part);
                    },
                    Dependency::ShuffleDependency(_shuf) => {
                        data_t_ptr.push(data[1].clone());
                    },
                };
                forget(data);
                crate::ALLOCATOR.lock().set_switch(true);
                let result = data_t_ptr.clone(); 
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8;
                crate::ALLOCATOR.lock().set_switch(false);
                result_ptr
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        //encryption block size: 1
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
        //println!("data = {:?}", data);
        Box::new(data.into_iter())
    }
}

impl<K, V, W, KE, VE, WE, FE, FD> OpE for CoGrouped<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
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