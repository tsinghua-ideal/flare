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
    pub(crate) op1: SerArc<dyn Op<Item = (K, V)>>,
    pub(crate) op2: SerArc<dyn Op<Item = (K, W)>>,
    pub(crate) part: Box<dyn Partitioner>,
    _marker_k: PhantomData<K>,
    _marker_v: PhantomData<V>,
    _marker_w: PhantomData<W>,
}

impl<K: Data + Eq + Hash, V: Data, W: Data> CoGrouped<K, V, W> {
    pub fn new(op1: SerArc<dyn Op<Item = (K, V)>>,
               op2: SerArc<dyn Op<Item = (K, W)>>,
               part: Box<dyn Partitioner>) -> Self 
    {
        let context = op1.get_context();
        let id = context.new_op_id();
        let mut vals = OpVals::new(context.clone());
        let mut deps = Vec::new();
        
        if op1
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new()) as Arc<dyn NarrowDependencyTrait>,
            ))
        } else {
            let aggr = Aggregator::<K, V, _>::default();
            deps.push(Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ))
        }

        if op2
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new()) as Arc<dyn NarrowDependencyTrait>,
            ))  
        } else {
            let aggr = Aggregator::<K, W, _>::default();
            deps.push(Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ))
        }
        
        vals.deps = deps;
        let vals = Arc::new(vals);
        CoGrouped {
            vals,
            next_deps: Arc::new(SgxMutex::new(deps)),
            op1,
            op2,
            part,
            _marker_k: PhantomData,
            _marker_v: PhantomData,
            _marker_w: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, W: Data> Op for CoGrouped<K, V, W> {
    type Item = (K, Vec<Vec<>>);  //may need a tuple

    fn get_id(&self) -> usize {
        self.vals.id
    }

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
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

    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        if id == self.get_id() {
            let next_deps = self.next_deps.lock().unwrap();
            match is_shuffle == 0 {
                true => {       //No shuffle later
                    assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
                    let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
                    let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                    let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                    (ser_result, ser_result_idx)
                },
                false => {      //Shuffle later
                    (ser_data.to_vec(), ser_data_idx.to_vec())
                },
            }
        /*
        }
        else if id < self.get_id() {
            self.parent.compute_by_id(ser_data, ser_data_idx, id, is_shuffle)
        */
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        //if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut agg: HashMap<K, Vec<Vec<Box<dyn AnyData>>>> = HashMap::new();
            /*
            for (dep_num, dep) in split.clone().deps.into_iter().enumerate() {
                match dep {
                    CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                        for i in rdd.iterator_any(split)? {                            
                            let b = i
                                .into_any()
                                .downcast::<(Box<dyn AnyData>, Box<dyn AnyData>)>()
                                .unwrap();
                            let (k, v) = *b;
                            let k = *(k.into_any().downcast::<K>().unwrap());
                            agg.entry(k)
                                .or_insert_with(|| vec![Vec::new(); self.rdds.len()])[dep_num]
                                .push(v)
                        }
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                        let num_rdds = self.rdds.len();
                        let fut = ShuffleFetcher::fetch::<K, Vec<Box<dyn AnyData>>>(
                            shuffle_id,
                            split.get_index(),
                        );
                        for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                            let temp = agg.entry(k).or_insert_with(|| vec![Vec::new(); num_rdds]);
                            for v in c {
                                temp[dep_num].push(v);
                            }
                        }
                    }
                }
            }
            */
            Box::new(agg.into_iter())
        //} else {
        //    panic!("Got split object from different concrete type other than CoGroupSplit")
        //}
    }
}
