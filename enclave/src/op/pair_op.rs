use std::boxed::Box;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{drop, forget};
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::op::*;
use crate::basic::{Arc as SerArc, AnyData, Data};

pub trait Pair<K: Data + Eq + Hash, V: Data>: Op<Item = (K, V)> + Send + Sync {
    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, C)>>
    where
        Self: Sized + 'static,
    {
        SerArc::new(Shuffled::new(
            self.get_op(),
            Arc::new(aggregator),
            partitioner,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn Op<Item = (K, Vec<V>)>>
    where
        Self: Sized + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, Vec<V>)>>
    where
        Self: Sized + 'static,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }

    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> SerArc<dyn Op<Item = (K, V)>>
    where
        F: Fn((V, V)) -> V + Clone + Send + Sync + 'static,
        Self: Sized + 'static,
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
    }

    fn reduce_by_key_using_partitioner<F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, V)>>
    where
        F: Fn((V, V)) -> V + Clone + Send + Sync + 'static,
        Self: Sized + 'static,
    {
        let create_combiner = Box::new(|v: V| v);
        let f_clone = func.clone();
        let merge_value = Box::new(move |(buf, v)| { (f_clone)((buf, v)) });
        let merge_combiners = Box::new(move |(b1, b2)| { (func)((b1, b2)) });
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    fn map_values<U: Data, F>(
        &self,
        f: F,
    ) -> SerArc<dyn Op<Item = (K, U)>>
    where
        F: Fn(V) -> U + Clone + Send + Sync + 'static,
        Self: Sized,
    {
        SerArc::new(MappedValues::new(self.get_op(), f))
    }

    fn flat_map_values<U: Data, F>(
        &self,
        f: F,
    ) -> SerArc<dyn Op<Item = (K, U)>>
    where
        F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + Send + Sync + 'static,
        Self: Sized,
    {
        SerArc::new(FlatMappedValues::new(self.get_op(), f))
    }

    fn join<W: Data>(
        &self,
        other: SerArc<dyn Op<Item = (K, W)>>,
        num_splits: usize,
    ) -> SerArc<dyn Op<Item = (K, (V, W))>> {
        let f = |v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        };
        self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Box::new(f))
    }

    fn cogroup<W: Data>(
        &self,
        other: SerArc<dyn Op<Item = (K, W)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, (Vec<V>, Vec<W>))>> {
        let op0 = SerArc::from(self.get_op());
        let op1 = SerArc::from(other.get_op());
        let cg_op = CoGrouped::<K, V, W>::new(op0, op1, partitioner);
        let f = |v: (Vec<V>, Vec<W>)| -> (Vec<V>, Vec<W>) {
            v
        };
        cg_op.map_values(Box::new(f))
    }

}

// Implementing the Pair trait for all types which implements Op
impl<K: Data + Eq + Hash, V: Data, T: Op<Item = (K, V)>> Pair<K, V> for T  {}
impl<K: Data + Eq + Hash, V: Data, T: Op<Item = (K, V)>> Pair<K, V> for SerArc<T> {}

pub struct MappedValues<K: Data, V: Data, U: Data, F>
where
    F: Fn(V) -> U + Clone + 'static,
{ 
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for MappedValues<K, V, U, F>
where
    F: Fn(V) -> U + Clone + 'static,
{
    fn clone(&self) -> Self {
        MappedValues { 
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> MappedValues<K, V, U, F>
where
    F: Fn(V) -> U + Clone + 'static,
{
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F) -> Self {
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id());
        let mut vals = OpVals::new(prev.get_context());
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_ids.clone()),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        ); 
        MappedValues {
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            prev,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> OpBase for MappedValues<K, V, U, F>
where
    F: Fn(V) -> U + Clone + Send + Sync + 'static,
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
    
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }
}


impl<K: Data, V: Data, U: Data, F> Op for MappedValues<K, V, U, F>
where
    F: Fn(V) -> U + Clone + Send + Sync + 'static,
{   
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
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
        let f = self.f.clone();
        Box::new(
            self.prev.compute(data_ptr).map(move |(k, v)| (k, f(v))),
        )
    }
}

pub struct FlatMappedValues<K: Data, V: Data, U: Data, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + 'static,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    f: F,    
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for FlatMappedValues<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + 'static,
{
    fn clone(&self) -> Self {
        FlatMappedValues {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> FlatMappedValues<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + 'static,
{
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F) -> Self {
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id());
        let mut vals = OpVals::new(prev.get_context());
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_ids.clone()),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        );
        FlatMappedValues {
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            prev,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> OpBase for FlatMappedValues<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + Send + Sync + 'static,
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
    
    fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        self.compute_start(data_ptr, is_shuffle)
    }
}

impl<K: Data, V: Data, U: Data, F> Op for FlatMappedValues<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + Send + Sync + 'static,
{
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
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
        let f = self.f.clone();
        Box::new(
            self.prev
                .compute(data_ptr)
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        )
    }
}
