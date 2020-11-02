use std::boxed::Box;
use std::hash::Hash;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::op::*;
use crate::basic::{Arc as SerArc, AnyData, Data, Func, SerFunc};

pub trait Pair<K, V, KE, VE>: OpE<Item = (K, V), ItemE = (KE, VE)> + Send + Sync 
where 
    K: Data + Eq + Hash, 
    V: Data, 
    KE: Data, 
    VE: Data,
{
    fn combine_by_key<KE2: Data, C: Data, CE: Data, FE, FD>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = (K, C), ItemE = (KE2, CE)>>
    where
        Self: Sized + 'static,
        FE: SerFunc(Vec<(K, C)>) -> Vec<(KE2, CE)>, 
        FD: SerFunc(Vec<(KE2, CE)>) -> Vec<(K, C)>,
    {
        let new_op = SerArc::new(Shuffled::new(
            self.get_op(),
            Arc::new(aggregator),
            partitioner,
            fe,
            fd,
        ));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn OpE<Item = (K, Vec<V>), ItemE = (KE, Vec<VE>)>>
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
    ) -> SerArc<dyn OpE<Item = (K, Vec<V>), ItemE = (KE, Vec<VE>)>>
    where
        Self: Sized + 'static,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let fe_wrapper = move |v: Vec<(K, Vec<V>)>| {
            let mut ct = Vec::with_capacity(v.len());
            for (x, vy) in v {
                let (ct_x, ct_y): (Vec<KE>, Vec<VE>) = (fe)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                ct.push((ct_x[0].clone(), ct_y));
            } 
            ct
        };
        let fd_wrapper = move |v: Vec<(KE, Vec<VE>)>| {
            let mut pt = Vec::with_capacity(v.len());
            for (x, vy) in v {
                let (pt_x, pt_y): (Vec<K>, Vec<V>) = (fd)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                pt.push((pt_x[0].clone(), pt_y));
            }
            pt
        };
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner, fe_wrapper, fd_wrapper)
    }

    fn reduce_by_key<KE2, VE2, F, FE, FD>(&self, func: F, num_splits: usize, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
        FE: SerFunc(Vec<(K, V)>) -> Vec<(KE2, VE2)>, 
        FD: SerFunc(Vec<(KE2, VE2)>) -> Vec<(K, V)>,  
        Self: Sized + 'static,
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
            fe,
            fd,
        )
    }

    fn reduce_by_key_using_partitioner<KE2, VE2, F, FE, FD>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
        FE: SerFunc(Vec<(K, V)>) -> Vec<(KE2, VE2)>, 
        FD: SerFunc(Vec<(KE2, VE2)>) -> Vec<(K, V)>,  
        Self: Sized + 'static,
    {
        let create_combiner = Box::new(|v: V| v);
        let f_clone = func.clone();
        let merge_value = Box::new(move |(buf, v)| { (f_clone)((buf, v)) });
        let merge_combiners = Box::new(move |(b1, b2)| { (func)((b1, b2)) });
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner, fe, fd)
    }

    fn map_values<U, UE, F, FE, FD>(
        &self,
        f: F,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = (K, U), ItemE = (KE, UE)>>
    where
        Self: Sized,
        F: SerFunc(V) -> U + Clone,
        U: Data,
        UE: Data,
        FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>, 
        FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
    {
        let new_op = SerArc::new(MappedValues::new(self.get_op(), f, fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn flat_map_values<U, UE, F, FE, FD>(
        &self,
        f: F,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn OpE<Item = (K, U), ItemE = (KE, UE)>>
    where
        Self: Sized,
        F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
        U: Data,
        UE: Data,
        FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>, 
        FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
    {
        let new_op = SerArc::new(FlatMappedValues::new(self.get_op(), f, fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn join<W: Data, WE: Data>(
        &self,
        other: SerArc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
        num_splits: usize,
    ) -> SerArc<dyn OpE<Item = (K, (V, W)), ItemE = (KE, (VE, WE))>> {
        let f = |v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        };

        let fe0 = self.get_fe();
        let fd0 = self.get_fd();
        let fe1 = other.get_fe();
        let fd1 = other.get_fd();
        let fe = move |v: Vec<(K, (V, W))>| {
            let (vx, vy): (Vec<K>, Vec<(V, W)>) = v.into_iter().unzip();
            let (vy, vz): (Vec<V>, Vec<W>) = vy.into_iter().unzip();
            let ct_xy: Vec<(KE, VE)> = (fe0)(vx
                .clone()
                .into_iter()
                .zip(vy.into_iter())
                .collect::<Vec<_>>());
            let ct_xz: Vec<(KE, WE)> = (fe1)(vx
                .into_iter()
                .zip(vz.into_iter())
                .collect::<Vec<_>>());
            ct_xy.into_iter()
                .zip(ct_xz.into_iter())
                .map(|((ct_k, ct_v), (_, ct_w))| (ct_k, (ct_v, ct_w)))
                .collect::<Vec<_>>()
        };
        let fd = move |v: Vec<(KE, (VE, WE))>| {
            let (vx, vy): (Vec<KE>, Vec<(VE, WE)>) = v.into_iter().unzip();
            let (vy, vz): (Vec<VE>, Vec<WE>) = vy.into_iter().unzip();
            let pt_xy: Vec<(K, V)> = (fd0)(vx
                .clone()
                .into_iter()
                .zip(vy.into_iter())
                .collect::<Vec<_>>());
            let pt_xz: Vec<(K, W)> = (fd1)(vx
                .into_iter()
                .zip(vz.into_iter())
                .collect::<Vec<_>>());
            pt_xy.into_iter()
                .zip(pt_xz.into_iter())
                .map(|((pt_k, pt_v), (_, pt_w))| (pt_k, (pt_v, pt_w)))
                .collect::<Vec<_>>()
        };
        let new_op = self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Box::new(f), fe, fd);
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn cogroup<W: Data, WE: Data>(
        &self,
        other: SerArc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn OpE<Item = (K, (Vec<V>, Vec<W>)), ItemE = (KE, (Vec<VE>, Vec<WE>))>> {
        let fe0 = self.get_fe();
        let fd0 = self.get_fd();
        let fe1 = other.get_fe();
        let fd1 = other.get_fd();
        let fe = move |v: Vec<(K, (Vec<V>, Vec<W>))>| {
            let mut ct = Vec::with_capacity(v.len());
            for (x, (vy, vz)) in v {
                let (ct_x, ct_y): (Vec<KE>, Vec<VE>) = (fe0)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                let (_, ct_z): (Vec<KE>, Vec<WE>) = (fe1)(vz
                    .into_iter()
                    .map(|z| (x.clone(), z))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                ct.push((ct_x[0].clone(), (ct_y, ct_z)));
            } 
            ct
        };
        let fd = move |v: Vec<(KE, (Vec<VE>, Vec<WE>))>| {
            let mut pt = Vec::with_capacity(v.len());
            for (x, (vy, vz)) in v {
                let (pt_x, pt_y): (Vec<K>, Vec<V>) = (fd0)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                let (_, pt_z): (Vec<K>, Vec<W>) = (fd1)(vz
                    .into_iter()
                    .map(|z| (x.clone(), z))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                pt.push((pt_x[0].clone(), (pt_y, pt_z)));
            }
            pt
        };
        let new_op = SerArc::new(CoGrouped::new(self.get_ope(), other.get_ope(), fe, fd, partitioner));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

}

// Implementing the Pair trait for all types which implements Op
impl<K, V, KE, VE, T> Pair<K, V, KE, VE> for T
where
    T: OpE<Item = (K, V), ItemE = (KE, VE)>,
    K: Data + Eq + Hash, 
    V: Data, 
    KE: Data, 
    VE: Data,
{}

impl<K, V, KE, VE, T> Pair<K, V, KE, VE> for SerArc<T>
where
    T: OpE<Item = (K, V), ItemE = (KE, VE)>,
    K: Data + Eq + Hash, 
    V: Data, 
    KE: Data, 
    VE: Data,
{}

pub struct MappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{ 
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<K, V, U, KE, UE, F, FE, FD> Clone for MappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn clone(&self) -> Self {
        MappedValues { 
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> MappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
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
            fe,
            fd,
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> OpBase for MappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> U + Clone,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
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


impl<K, V, U, KE, UE, F, FE, FD> Op for MappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> U + Clone,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{   
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }
    
    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        let f = self.f.clone();
        Box::new(
            self.prev.compute(data_ptr).map(move |(k, v)| (k, f(v))),
        )
    }
}

impl<K, V, U, KE, UE, F, FE, FD> OpE for MappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> U + Clone,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
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

pub struct FlatMappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    f: F,    
    fe: FE,
    fd: FD,
}

impl<K, V, U, KE, UE, F, FE, FD> Clone for FlatMappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValues {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> FlatMappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
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
            fe,
            fd,
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> OpBase for FlatMappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
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

impl<K, V, U, KE, UE, F, FE, FD> Op for FlatMappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(data_ptr)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr)
            },
            _ => panic!("Invalid is_shuffle")
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

impl<K, V, U, KE, UE, F, FE, FD> OpE for FlatMappedValues<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
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
