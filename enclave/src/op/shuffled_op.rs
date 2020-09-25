use std::boxed::Box;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{drop, forget};
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{Data};
use crate::op::{Context, Op, OpBase, OpVals};
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
        let mut prev_ids = parent.get_prev_ids();
        prev_ids.insert(parent.get_id());
        let dep = Dependency::ShuffleDependency(Arc::new(
            ShuffleDependency::new(
                false,
                aggregator.clone(),
                part.clone(),
                prev_ids,
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

impl<K: Data + Eq + Hash, V: Data, C: Data> OpBase for Shuffled<K, V, C> {
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
        Some(self.part.clone())
    }
    
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Op for Shuffled<K, V, C> {
    type Item = (K, C);

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
                let mut combiners: HashMap<K, Option<C>> = HashMap::new();
                let aggregator = self.aggregator.clone(); 
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) };
                let data = data_enc.clone();
                forget(data_enc);
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
                crate::ALLOCATOR.lock().set_switch(true);
                let result_enc = result.clone(); // encrypt
                let result_ptr = Box::into_raw(Box::new(result_enc)) as *mut u8; 
                crate::ALLOCATOR.lock().set_switch(false);
                result_ptr
            },
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) };
        let data = data_enc.clone();
        forget(data_enc);
        Box::new(data.into_iter())
    }
}
