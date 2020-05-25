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
pub struct CoGrouped<K: Data> {
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    pub(crate) ops: Vec<SerArc<dyn Op<Item = (K, Vec<Vec<Box<dyn AnyData>>>)>>>,
    pub(crate) part: Box<dyn Partitioner>,
    _marker: PhantomData<K>,
}

impl<K: Data + Eq + Hash> CoGrouped<K> {
    pub fn new(ops: Vec<SerArc<dyn Op<Item = (K, Vec<Vec<Box<dyn AnyData>>>)>>>, 
               part: Box<dyn Partitioner>) -> Self 
    {
        let context = ops[0].get_context();
        let id = context.new_op_id();
        let mut vals = OpVals::new(context.clone());
        let create_combiner = Box::new(|v: Box<dyn AnyData>| vec![v]);
        fn merge_value(
            mut buf: Vec<Box<dyn AnyData>>,
            v: Box<dyn AnyData>,
        ) -> Vec<Box<dyn AnyData>> {
            buf.push(v);
            buf
        }
        let merge_value = Box::new(|(buf, v)| merge_value(buf, v));
        fn merge_combiners(
            mut b1: Vec<Box<dyn AnyData>>,
            mut b2: Vec<Box<dyn AnyData>>,
        ) -> Vec<Box<dyn AnyData>> {
            b1.append(&mut b2);
            b1
        }
        let merge_combiners = Box::new(|(b1, b2)| merge_combiners(b1, b2));
        let aggr = Arc::new(
            Aggregator::<K, Box<dyn AnyData>, Vec<Box<dyn AnyData>>>::new(
                create_combiner,
                merge_value,
                merge_combiners,
            ),
        );
        let mut deps = Vec::new();
        
        for (_index, op) in ops.iter().enumerate() {
            let part = part.clone();
            if op     //反了？
                .partitioner()
                .map_or(false, |p| p.equals(&part as &dyn Any))
            {
                deps.push(Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new()) as Arc<dyn NarrowDependencyTrait>,
                ))
            } else {
                deps.push(Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        true,
                        aggr.clone(),
                        part,
                    )) as Arc<dyn ShuffleDependencyTrait>,
                ))
            }
        }
        vals.deps = deps;
        let vals = Arc::new(vals);
        CoGrouped {
            vals,
            next_deps: Arc::new(SgxMutex::new(deps)),
            ops,
            part,
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash> Op for CoGrouped<K> {
    type Item = (K, Vec<Vec<Box<dyn AnyData>>>);

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
        let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();
        Box::new(data.into_iter())
    }
}
