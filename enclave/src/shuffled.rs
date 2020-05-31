use std::boxed::Box;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, SgxMutex, Weak};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{Data};
use crate::common::{Common, Context};
use crate::dependency::{Dependency, ShuffleDependency, ShuffleDependencyTrait};
use crate::partitioner::Partitioner;

pub struct Shuffled<K: Data + Eq + Hash, V: Data, C: Data> {
    context: Weak<Context>,
    id: usize,
    dependencies: Arc<SgxMutex<Vec<Dependency>>>,
    parent: Arc<dyn Common<Item = (K, V)>>,
    _marker_t: PhantomData<C>
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Clone for Shuffled<K, V, C> {
    fn clone(&self) -> Self {
        Shuffled {
            context: self.context.clone(),
            id: self.id,
            dependencies: self.dependencies.clone(),
            parent: self.parent.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Shuffled<K, V, C> {
    pub(crate) fn new(
        parent: Arc<dyn Common<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let ctx = parent.get_context();

        parent.get_dependencies().lock().unwrap().push(
            Dependency::ShuffleDependency(Arc::new(
                ShuffleDependency::new(
                    false,
                    aggregator.clone(),
                    part.clone(),
                ),
            ))
        );
        Shuffled {
            context: Arc::downgrade(&ctx),
            id: ctx.new_op_id(),
            dependencies: parent.get_dependencies(),
            parent,
            _marker_t: PhantomData
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Common for Shuffled<K, V, C> {
    type Item = (K, C);

    fn get_id(&self) -> usize {
        self.id
    }

    fn get_op(&self) -> Arc<dyn Common<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_context(&self) -> Arc<Context> {
        self.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        self.dependencies.clone()
    }

    fn compute_by_id(&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        if id == self.id {
            let dep = self.dependencies.lock().unwrap();
            match self.id > dep.len() || is_shuffle == 0 {
                true => {       //No shuffle later
                    assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
                    let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
                    let ser_result: Vec<u8> = bincode::serialize(&result).unwrap();
                    let ser_result_idx: Vec<usize> = vec![ser_result.len()];
                    (ser_result, ser_result_idx)
                },
                false => {      //Shuffle later
                    let mut combiners: HashMap<K, Option<C>> = HashMap::new();
                    let shuf_dep_r = match &dep[self.id-1] {
                        Dependency::ShuffleDependency(shuf_dep) => shuf_dep.clone().downcast_arc::<ShuffleDependency<K, V, C>>(),
                        Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                    };
                    let aggregator = match shuf_dep_r {
                        Ok(shuf_dep) => shuf_dep.aggregator.clone(),
                        Err(_) => panic!("get aggregator error!"),
                    };

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
                    //sub-partition
                    let len = result.len(); 
                    //println!("shuffled.rs : result.len() = {:?}", len);
                    let data_size = std::mem::size_of::<Self::Item>(); //need revising
                    let block_len = (1 << (5+10+10))/ data_size;
                    let mut cur = 0;
                    let mut ser_result: Vec<u8> = Vec::with_capacity(len * data_size);
                    let mut ser_result_idx: Vec<usize> = Vec::new();
                    let mut idx: usize = 0;

                    while cur < len {
                        let next = match cur + block_len > len {
                            true => len,
                            false => cur + block_len,
                        };
                        
                        
                        let ser_result_bl = bincode::serialize(&result[cur..next]).unwrap();
                        idx += ser_result_bl.len();
                        ser_result.extend_from_slice(&ser_result_bl);
                        ser_result_idx.push(idx);

                        cur = next;
                    }
                    if len == 0 {
                        ser_result = vec![0; 8];
                        ser_result_idx = vec![8];
                    }
                    (ser_result, ser_result_idx)
                },
            }
        }
        else if id < self.id {
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
