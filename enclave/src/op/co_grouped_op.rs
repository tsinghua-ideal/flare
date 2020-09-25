use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::{drop, forget};
use std::sync::{Arc, SgxMutex, Weak};
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
pub struct CoGrouped<K: Data, V: Data, W: Data> {
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    pub(crate) op0: SerArc<dyn Op<Item = (K, V)>>,
    pub(crate) op1: SerArc<dyn Op<Item = (K, W)>>,
    pub(crate) part: Box<dyn Partitioner>,
    _marker_k: PhantomData<K>,
    _marker_v: PhantomData<V>,
    _marker_w: PhantomData<W>,
}

impl<K: Data + Eq + Hash, V: Data, W: Data> CoGrouped<K, V, W> {
    pub fn new(op0: SerArc<dyn Op<Item = (K, V)>>,
               op1: SerArc<dyn Op<Item = (K, W)>>,
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
            let aggr = Arc::new(Aggregator::<K, V, _>::default());
            deps.push(Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr.clone(),
                    part.clone(),
                    prev_ids.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
            op0.get_next_deps().lock().unwrap().push(
                Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        true,
                        aggr,
                        part.clone(),
                        prev_ids,
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
            let aggr = Arc::new(Aggregator::<K, W, _>::default());
            deps.push(Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr.clone(),
                    part.clone(),
                    prev_ids.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
            op1.get_next_deps().lock().unwrap().push(
                Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        true,
                        aggr,
                        part.clone(),
                        prev_ids,
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
            part,
            _marker_k: PhantomData,
            _marker_v: PhantomData,
            _marker_w: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, W: Data> OpBase for CoGrouped<K, V, W> {
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

impl<K: Data + Eq + Hash, V: Data, W: Data> Op for CoGrouped<K, V, W>{
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
                let result = self.compute(data_ptr)
                    .collect::<Vec<Self::Item>>();
                crate::ALLOCATOR.lock().set_switch(true);
                let result_enc = result.clone(); // encrypt
                let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8;
                crate::ALLOCATOR.lock().set_switch(false);
                result_ptr
            },
            false => {      //Shuffle later
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) };
                let data = data_enc.clone();
                crate::ALLOCATOR.lock().set_switch(true);
                drop(data_enc);
                crate::ALLOCATOR.lock().set_switch(false);
                let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.do_shuffle_task(iter)
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
                    let block_ptr = *block_ptr as *mut u8 as *mut Vec<(K, V)>;
                    let data0_enc  = unsafe{ Box::from_raw(block_ptr) };
                    let data0 = data0_enc.clone();
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
                    let data_ptr = *data_ptr as *mut u8 as *mut Vec<(K, Vec<V>)>;
                    let data0_enc = unsafe{ Box::from_raw(data_ptr) };
                    let data0 = data0_enc.clone();
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
                    let block_ptr = *block_ptr as *mut u8 as *mut Vec<(K, W)>;
                    let data1_enc = unsafe{ Box::from_raw(block_ptr) };
                    let data1 = data1_enc.clone();
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
                    let data_ptr = *data_ptr as *mut u8 as *mut Vec<(K, Vec<W>)>;
                    let data1_enc = unsafe{ Box::from_raw(data_ptr) };
                    let data1 = data1_enc.clone();
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
