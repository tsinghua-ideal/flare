use std::boxed::Box;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{Data, Func, SerFunc};
use crate::op::{Context, Op, OpE, OpBase, OpVals};
use crate::dependency::{Dependency, ShuffleDependency, ShuffleDependencyTrait};
use crate::partitioner::Partitioner;

pub struct Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> Vec<(KE, CE)> + Clone,
    FD: Func(Vec<(KE, CE)>) -> Vec<(K, C)> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    parent: Arc<dyn Op<Item = (K, V)>>,
    aggregator: Arc<Aggregator<K, V, C>>,
    part: Box<dyn Partitioner>,
    fe: FE,
    fd: FD,
}

impl<K, V, C, KE, CE, FE, FD> Clone for Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> Vec<(KE, CE)> + Clone,
    FD: Func(Vec<(KE, CE)>) -> Vec<(K, C)> + Clone,
{
    fn clone(&self) -> Self {
        Shuffled {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            part: self.part.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> Vec<(KE, CE)> + Clone,
    FD: Func(Vec<(KE, CE)>) -> Vec<(K, C)> + Clone,
{
    pub(crate) fn new(
        parent: Arc<dyn Op<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let mut prev_ids = parent.get_prev_ids();
        prev_ids.insert(parent.get_id());
        let dep = Dependency::ShuffleDependency(Arc::new(
            ShuffleDependency::new(
                false,
                aggregator.clone(),
                part.clone(),
                prev_ids,
                fe.clone(),
                fd.clone(),
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
            fe,
            fd,
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> OpBase for Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> Vec<(KE, CE)>,
    FD: SerFunc(Vec<(KE, CE)>) -> Vec<(K, C)>,
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
        Some(self.part.clone())
    }
    
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }
}

impl<K, V, C, KE, CE, FE, FD> Op for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> Vec<(KE, CE)>,
    FD: SerFunc(Vec<(KE, CE)>) -> Vec<(K, C)>, 
{
    type Item = (K, C);

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {      //No shuffle
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            }
            2 => {      //Shuffle read
                let mut combiners: HashMap<K, Option<C>> = HashMap::new();
                let aggregator = self.aggregator.clone(); 
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<(KE, CE)>) };
                // encryption block size: 1
                let data = self.get_fd()(*(data_enc.clone())); 
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
                // encryption block size: 1
                let result_enc = self.get_fe()(result); 
                crate::ALLOCATOR.lock().set_switch(true);
                let result = result_enc.clone(); // encrypt
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8; 
                crate::ALLOCATOR.lock().set_switch(false);
                result_ptr
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<(KE, CE)>) };
        // for group_by, self.fd naturally encrypt/decrypt per row, that is, encryption block size = 1
        let data = self.get_fd()(*(data_enc.clone()));
        forget(data_enc);
        Box::new(data.into_iter())
    }
}

impl<K, V, C, KE, CE, FE, FD> OpE for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> Vec<(KE, CE)>,
    FD: SerFunc(Vec<(KE, CE)>) -> Vec<(K, C)>, 
{
    type ItemE = (KE, CE);
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
