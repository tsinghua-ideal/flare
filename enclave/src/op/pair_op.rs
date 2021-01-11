use std::boxed::Box;
use std::hash::Hash;
use std::mem::forget;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::aggregator::Aggregator;
use crate::basic::{Arc as SerArc, AnyData, Data, Func, SerFunc};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::op::*;
use crate::serialization_free::{Construct, Idx, SizeBuf};
pub trait Pair<K, V, KE, VE>: OpE<Item = (K, V), ItemE = (KE, VE)> + Send + Sync 
where 
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    KE: Data + Eq + Hash + Ord, 
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
        FE: SerFunc(Vec<(K, C)>) -> (KE2, CE), 
        FD: SerFunc((KE2, CE)) -> Vec<(K, C)>,
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

    fn group_by_key<CE, FE, FD>(&self, fe: FE, fd: FD, num_splits: usize) -> SerArc<dyn OpE<Item = (K, Vec<V>), ItemE = (KE, CE)>>
    where
        Self: Sized + 'static,
        CE: Data,
        FE: SerFunc(Vec<(K, Vec<V>)>) -> (KE, CE),
        FD: SerFunc((KE, CE)) -> Vec<(K, Vec<V>)>,
    {
        self.group_by_key_using_partitioner(
            fe,
            fd,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner<CE, FE, FD>(
        &self,
        fe: FE,
        fd: FD,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn OpE<Item = (K, Vec<V>), ItemE = (KE, CE)>>
    where
        Self: Sized + 'static,
        CE: Data,
        FE: SerFunc(Vec<(K, Vec<V>)>) -> (KE, CE),
        FD: SerFunc((KE, CE)) -> Vec<(K, Vec<V>)>,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner, fe, fd)
    }

    fn reduce_by_key<KE2, VE2, F, FE, FD>(&self, func: F, num_splits: usize, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
        Self: Sized + 'static,
        FE: SerFunc(Vec<(K, V)>) -> (KE2, VE2), 
        FD: SerFunc((KE2, VE2)) -> Vec<(K, V)>,        
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
            fe,
            fd
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
        FE: SerFunc(Vec<(K, V)>) -> (KE2, VE2), 
        FD: SerFunc((KE2, VE2)) -> Vec<(K, V)>,  
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
        FE: SerFunc(Vec<(K, U)>) -> (KE, UE), 
        FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
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
        FE: SerFunc(Vec<(K, U)>) -> (KE, UE), 
        FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
    {
        let new_op = SerArc::new(FlatMappedValues::new(self.get_op(), f, fe, fd));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

    fn join<W, WE, FE, FD>(
        &self,
        other: SerArc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
        fe: FE,
        fd: FD,
        num_splits: usize,
    ) -> SerArc<dyn OpE<Item = (K, (V, W)), ItemE = (KE, (VE, WE))>> 
    where
        W: Data,
        WE: Data,
        FE: SerFunc(Vec<(K, (V, W))>) -> (KE, (VE, WE)), 
        FD: SerFunc((KE, (VE, WE))) -> Vec<(K, (V, W))>, 
    {
        let f = Box::new(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });

        let fe0 = self.get_fe();
        let fd0 = self.get_fd();
        let fe1 = other.get_fe();
        let fd1 = other.get_fd();
        //temporarily built
        let fe_cg = Box::new(move |v: Vec<(K, (Vec<V>, Vec<W>))>| {
            let (k, vw): (Vec<K>, Vec<(Vec<V>, Vec<W>)>) = v.into_iter().unzip();
            let (v, w): (Vec<Vec<V>>, Vec<Vec<W>>) = vw.into_iter().unzip();
            let w_default = Default::default();
            let w_padding: Vec<W> = vec![w_default; k.len()];
            let (ct_x, _) = (fe1)(k.into_iter()
                .zip(w_padding.into_iter())
                .collect::<Vec<_>>()
            );
            (ct_x, (ser_encrypt(v), ser_encrypt(w)))
        });
        let fd_cg = Box::new(move |v: (KE, (Vec<u8>, Vec<u8>))| {
            let (ct_x, (ct_y, ct_z)) = v;
            let w_padding: WE = Default::default();
            let (x, _): (Vec<K>, Vec<W>)= (fd1)((ct_x, w_padding)).into_iter().unzip();
            let y = ser_decrypt(ct_y);
            let z = ser_decrypt(ct_z);
            x.into_iter()
                .zip(y.into_iter()
                    .zip(z.into_iter())
                ).collect::<Vec<_>>()
        });

        self.cogroup(
            other,
            fe_cg,
            fd_cg,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Box::new(f), fe, fd)
    }

    fn cogroup<W, WE, CE, DE, FE, FD>(
        &self,
        other: SerArc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
        fe: FE,
        fd: FD,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn OpE<Item = (K, (Vec<V>, Vec<W>)), ItemE = (KE, (CE, DE))>> 
    where
        W: Data,
        WE: Data,
        CE: Data,
        DE: Data,
        FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
        FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>, 
    {
        let new_op = SerArc::new(CoGrouped::new(self.get_ope(), other.get_ope(), fe, fd, partitioner));
        insert_opmap(new_op.get_id(), new_op.get_op_base());
        new_op
    }

}

// Implementing the Pair trait for all types which implements Op
impl<K, V, KE, VE, T> Pair<K, V, KE, VE> for T
where
    T: OpE<Item = (K, V), ItemE = (KE, VE)>,
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    KE: Data + Eq + Hash + Ord, 
    VE: Data,
{}

impl<K, V, KE, VE, T> Pair<K, V, KE, VE> for SerArc<T>
where
    T: OpE<Item = (K, V), ItemE = (KE, VE)>,
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    KE: Data + Eq + Hash + Ord, 
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, is_shuffle),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, is_shuffle), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 => self.free_res_enc(res_ptr),
            1 => {
                let next_deps = self.get_next_deps().lock().unwrap().clone();
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.free_res_enc(res_ptr);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

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
    
    fn iterator(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        self.compute_start(tid, data_ptr, is_shuffle, cache_meta)
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{   
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(data_ptr, cache_meta)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr, cache_meta)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }
    
    fn compute(&self, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let have_cache = cache_meta.count_cached_down();
        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = (cache_meta.cached_rdd_id, cache_meta.part_id, cache_meta.sub_part_id);
            let val = self.get_and_remove_cached_data(key);
            return (Box::new(val.into_iter()), None); 
        }
        
        let f = self.f.clone();
        let (res_iter, handle) = self.prev.compute(data_ptr, cache_meta);
        let res_iter = Box::new(res_iter.map(move |(k, v)| (k, f(v))));

        let need_cache = cache_meta.count_caching_down();
        if need_cache {
            assert!(handle.is_none());
            let key = (cache_meta.caching_rdd_id, cache_meta.part_id, cache_meta.sub_part_id);
            if CACHE.get(key).is_none() { 
                return self.set_cached_data(key, res_iter);
            }
        }

        (res_iter, handle)
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Self::ItemE>
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Self::ItemE)->Vec<Self::Item>>
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, is_shuffle),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, is_shuffle), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 => self.free_res_enc(res_ptr),
            1 => {
                let next_deps = self.get_next_deps().lock().unwrap().clone();
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.free_res_enc(res_ptr);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

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
    
    fn iterator(&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        self.compute_start(tid, data_ptr, is_shuffle, cache_meta)
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, tid: u64, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(data_ptr, cache_meta)
            },
            1 => {      //Shuffle write
                self.shuffle(data_ptr, cache_meta)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let have_cache = cache_meta.count_cached_down();
        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = (cache_meta.cached_rdd_id, cache_meta.part_id, cache_meta.sub_part_id);
            let val = self.get_and_remove_cached_data(key);
            return (Box::new(val.into_iter()), None); 
        }

        let f = self.f.clone();
        let (res_iter, handle) = self.prev.compute(data_ptr, cache_meta);
        let res_iter = Box::new(res_iter.flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))));
        
        let need_cache = cache_meta.count_caching_down();
        if need_cache {
            assert!(handle.is_none());
            let key = (cache_meta.caching_rdd_id, cache_meta.part_id, cache_meta.sub_part_id);
            if CACHE.get(key).is_none() { 
                return self.set_cached_data(key, res_iter);
            }
        }

        (res_iter, handle)
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Self::ItemE>
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Self::ItemE)->Vec<Self::Item>>
    }
}
