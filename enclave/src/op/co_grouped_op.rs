use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
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
    
    fn iterator(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        self.compute_start(ser_data, ser_data_idx, is_shuffle)
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

    fn compute_start(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        let next_deps = self.next_deps.lock().unwrap();
        match is_shuffle == 0 {
            true => {       //No shuffle later
                assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
                let result = self.compute(ser_data, ser_data_idx)
                    .collect::<Vec<Self::Item>>();
                let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                (ser_result, ser_result_idx)
            },
            false => {      //Shuffle later
                let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();                           
                let iter = Box::new(data.into_iter().map(|x| Box::new(x) as Box<dyn AnyData>));
                let shuf_dep = match &next_deps[0] {
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                let ser_result_set = shuf_dep.do_shuffle_task(iter);                    
                let mut ser_result = Vec::<u8>::with_capacity(std::mem::size_of_val(&ser_result_set));
                let mut ser_result_idx = Vec::<usize>::with_capacity(ser_result.len());
                let mut idx: usize = 0;
                for (i, mut ser_result_bl) in ser_result_set.into_iter().enumerate() {
                    idx += ser_result_bl.len();
                    ser_result.append(&mut ser_result_bl);
                    ser_result_idx[i] = idx;
                }
                (ser_result, ser_result_idx)                
            },
        }
    }

    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        let mut agg: HashMap<K, (Vec<V>, Vec<W>)> = HashMap::new();
        let mut array: [u8; 8] = [0; 8];
        array.clone_from_slice(&ser_data[0..8]);  //get byte len of first rdd
        let first_len = usize::from_le_bytes(array);
        let mut first_i = 0;
        let mut pre_idx = 8;
        let idx_len = ser_data_idx.len();
        let deps = self.get_deps();
        for (i, idx) in ser_data_idx.iter().enumerate() {
            if *idx == 8 + first_len {
                first_i = i;
                break;
            }
        }
        match &deps[0] {
            Dependency::NarrowDependency(nar) => {
                let data0: Vec<(K, V)> = bincode::deserialize(&ser_data[pre_idx..ser_data_idx[first_i]]).unwrap();
                for i in data0 { 
                    let (k, v) = i;
                    agg.entry(k)
                        .or_insert_with(|| (Vec::new(), Vec::new())).0
                        .push(v);
                }
            },
            Dependency::ShuffleDependency(shuf) => {
                for idx in &ser_data_idx[1..=first_i] {                            
                    let data_bl: Vec<Vec<(K, Vec<V>)>>= bincode::deserialize(&ser_data[pre_idx..*idx]).unwrap();
                    for (k, c) in data_bl.into_iter().flatten() { 
                        let temp = agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()));
                        for v in c {
                            temp.0.push(v);
                        }
                    }
                    pre_idx = *idx;
                }
            },
        };

        match &deps[1] {
            Dependency::NarrowDependency(nar) => {
                let data1: Vec<(K, W)> = bincode::deserialize(&ser_data[pre_idx..]).unwrap();
                for i in data1 {
                    let (k, w) = i;
                    agg.entry(k)
                        .or_insert_with(|| (Vec::new(), Vec::new())).1
                        .push(w);
                }
            },
            Dependency::ShuffleDependency(shuf) => {
                for idx in &ser_data_idx[first_i+1..] {                            
                    let data_bl: Vec<Vec<(K, Vec<W>)>>= bincode::deserialize(&ser_data[pre_idx..*idx]).unwrap();
                    for (k, c) in data_bl.into_iter().flatten() { 
                        let temp = agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()));
                        for w in c {
                            temp.1.push(w);
                        }
                    }
                    pre_idx = *idx;
                }
            },
        };

        Box::new(agg.into_iter())
    }
}
