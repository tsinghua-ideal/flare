use std::boxed::Box;
use std::hash::Hash;
use std::marker::PhantomData;
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

    fn map_values<U: Data, F>(
        &self,
        f: F,
    ) -> SerArc<dyn Op<Item = (K, U)>>
    where
        F: Fn(V) -> U + Clone,
        Self: Sized,
    {
        SerArc::new(MappedValues::new(self.get_op(), f))
    }

    fn flat_map_values<U: Data, F>(
        &self,
        f: F,
    ) -> SerArc<dyn Op<Item = (K, U)>>
    where
        F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
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
        let op1 = SerArc::from(self.get_op());
        let op2 = SerArc::from(other.get_op());
        let cg_op = CoGrouped::<K>::new(op1, op2, partitioner);
        let f = |v: Vec<Vec<Box<dyn AnyData>>>| -> (Vec<V>, Vec<W>) {
            let mut count = 0;
            let mut vs: Vec<V> = Vec::new();
            let mut ws: Vec<W> = Vec::new();
            for v in v.into_iter() {
                if count >= 2 {
                    break;
                }
                if count == 0 {
                    for i in v {
                        vs.push(*(i.into_any().downcast::<V>().unwrap()))
                    }
                } else if count == 1 {
                    for i in v {
                        ws.push(*(i.into_any().downcast::<W>().unwrap()))
                    }
                }
                count += 1;
            }
            (vs, ws)
        };
        cg_op.map_values(Box::new(f))
    }

}

// Implementing the Pair trait for all types which implements Op
impl<K: Data + Eq + Hash, V: Data, T: Op<Item = (K, V)>> Pair<K, V> for T  {}
impl<K: Data + Eq + Hash, V: Data, T: Op<Item = (K, V)>> Pair<K, V> for SerArc<T> {}

pub struct MappedValues<K: Data, V: Data, U: Data, F>
where
    F: Fn(V) -> U + Clone,
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
    F: Fn(V) -> U + Clone,
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
    F: Func(V) -> U + Clone,
{
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F) -> Self {
        let mut vals = OpVals::new(prev.get_context());
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new())
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

impl<K: Data, V: Data, U: Data, F> Op for MappedValues<K, V, U, F>
where
    F: Fn(V) -> U,
{
    type Item = (K, U);
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
    
    fn compute_by_id (&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        if id == self.get_id() {
            assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
            let next_deps = self.next_deps.lock().unwrap();
            match is_shuffle == 0 {
                true => {       //No shuffle later
                    let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
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
        else if id < self.get_id() {
            self.prev.compute_by_id(ser_data, ser_data_idx, id, is_shuffle)
        } else {
            panic!("Invalid id")
        }
    }
    
    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        let f = self.f.clone();
        Box::new(
            self.prev.compute(ser_data).map(move |(k, v)| (k, f(v))),
        )
    }
}

pub struct FlatMappedValues<K: Data, V: Data, U: Data, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
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
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
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
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F) -> Self {
        let mut vals = OpVals::new(prev.get_context());
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new())
            )
        );
        FlatMappedValues {
            vals,
            next_deps,
            prev,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> Op for FlatMappedValues<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>>,
{
    type Item = (K, U);
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

    fn compute_by_id (&self, ser_data: &[u8], ser_data_idx: &[usize], id: usize, is_shuffle: u8) -> (Vec<u8>, Vec<usize>){
        if id == self.get_id() {
            assert!(ser_data_idx.len()==1 && ser_data.len()==*ser_data_idx.last().unwrap());
            let next_deps = self.next_deps.lock().unwrap();
            match is_shuffle == 0 {
                true => {       //No shuffle later
                    let result = self.compute(ser_data).collect::<Vec<Self::Item>>();
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
        else if id < self.get_id() {
            self.prev.compute_by_id(ser_data, ser_data_idx, id, is_shuffle)
        } else {
            panic!("Invalid id")
        }
    }

    fn compute(&self, ser_data: &[u8]) -> Box<dyn Iterator<Item = Self::Item>> {
        let f = self.f.clone();
        Box::new(
            self.prev
                .compute(ser_data)
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        )
    }
}
