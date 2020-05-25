use std::boxed::Box;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex, Weak};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{Data};
use crate::op::{Context, Op, OpVals};
use crate::dependency::{Dependency, ShuffleDependency, ShuffleDependencyTrait};
use crate::partitioner::Partitioner;

pub struct Shuffled<K: Data + Eq + Hash, V: Data, C: Data> {
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    parent: Arc<dyn Op<Item = (K, V)>>,
    aggregator: Arc<Aggregator<K, V, C>>,
    part: Box<dyn Partitioner>,
    _marker_t: PhantomData<C>
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Clone for Shuffled<K, V, C> {
    fn clone(&self) -> Self {
        Shuffled {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            part: self.part.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Shuffled<K, V, C> {
    pub(crate) fn new(
        parent: Arc<dyn Op<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let dep = Dependency::ShuffleDependency(Arc::new(
            ShuffleDependency::new(
                false,
                aggregator.clone(),
                part.clone(),
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
            _marker_t: PhantomData
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Op for Shuffled<K, V, C> {
    type Item = (K, C);

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
        Some(self.part.clone())
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
                    let mut combiners: HashMap<K, Option<C>> = HashMap::new();
                    let aggregator = self.aggregator; 
                    let mut data = Vec::<(K, C)>::with_capacity(std::mem::size_of_val(ser_data));
                    let mut pre_idx: usize = 0;
                    let mut i = 0;
                    for idx in ser_data_idx {
                        let mut data_bl = bincode::deserialize::<Vec<(K, C)>>(&ser_data[pre_idx..*idx]).unwrap(); 
                        data.append(&mut data_bl);
                        pre_idx = *idx;
                        i += 1;
                    }
                    for (k, c) in data.into_iter() {
                        if let Some(old_c) = combiners.get_mut(&k) {
                            let old = old_c.take().unwrap();
                            let input = ((old, c),);
                            let output = aggregator.merge_combiners.call(input);
                            *old_c = Some(output);
                        } else {
                            combiners.insert(k, Some(c));
                        }
                    }
                    let result = combiners.into_iter().map(|(k, v)| (k, v.unwrap())).collect::<Vec<Self::Item>>();
                    let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                    let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                    (ser_result, ser_result_idx)
                },
            }
        }
        else if id < self.get_id() {
            self.parent.compute_by_id(ser_data, ser_data_idx, id, is_shuffle) 
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        let data: Vec<Self::Item> = bincode::deserialize(ser_data).unwrap();
        Box::new(data.into_iter())
    }
}
