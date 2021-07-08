use std::hash::Hash;
use crate::aggregator::Aggregator;
use crate::partitioner::HashPartitioner;
use crate::op::*;

pub trait Pair<K, V, KE, VE>: OpE<Item = (K, V), ItemE = (KE, VE)> + Send + Sync 
where 
    K: Data + Eq + Hash + Ord, 
    V: Data, 
    KE: Data + Eq + Hash + Ord, 
    VE: Data,
{
    #[track_caller]
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
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
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

    #[track_caller]
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

    #[track_caller]
    fn reduce_by_key<KE2, VE2, F, FE, FD>(&self, func: F, num_splits: usize, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        Self: Sized + 'static,
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
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

    #[track_caller]
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

    #[track_caller]
    fn values<FE, FD>(
        &self,
        fe: FE,  //the type corresponding to the encryption form is fixed
        fd: FD,
    ) -> SerArc<dyn OpE<Item = V, ItemE = VE>>
    where
        Self: Sized,
        FE: SerFunc(Vec<V>) -> VE, 
        FD: SerFunc(VE) -> Vec<V>,
    {
        let new_op = SerArc::new(Values::new(self.get_op(), fe, fd));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
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
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
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
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
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

        let cogrouped = self.cogroup(
            other,
            fe_cg,
            fd_cg,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        );
        self.get_context().add_num(1);
        cogrouped.flat_map_values(Box::new(f), fe, fd)
    }

    #[track_caller]
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
        let mut cogrouped = CoGrouped::new(self.get_ope(), other.get_ope(), fe, fd, partitioner);
        cogrouped.is_for_join = true;
        let new_op = SerArc::new(cogrouped);
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
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

pub struct Values<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: Func(Vec<V>) -> VE + Clone,
    FD: Func(VE) -> Vec<V> + Clone,
{ 
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    fe: FE,
    fd: FD,
}

impl<K, V, VE, FE, FD> Clone for Values<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: Func(Vec<V>) -> VE + Clone,
    FD: Func(VE) -> Vec<V> + Clone,
{
    fn clone(&self) -> Self {
        Values { 
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, VE, FE, FD> Values<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: Func(Vec<V>) -> VE + Clone,
    FD: Func(VE) -> Vec<V> + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, fe: FE, fd: FD) -> Self {
        let mut vals = OpVals::new(prev.get_context(), prev.number_of_splits());
        let cur_id = vals.id;
        let prev_id = prev.get_op_id();
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_id, cur_id),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().write().unwrap().insert(
            (prev_id, cur_id),
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_id, cur_id))
            )
        ); 
        Values {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            prev,
            fe,
            fd,
        }
    }
}

impl<K, V, VE, FE, FD> OpBase for Values<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: SerFunc(Vec<V>) -> VE,
    FD: SerFunc(VE) -> Vec<V>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr, is_enc),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr, is_enc);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn fix_split_num(&self, split_num: usize) {
        self.vals.split_num.store(split_num, atomic::Ordering::SeqCst);
    }

    fn get_op_id(&self) -> OpId {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }

    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        self.next_deps.clone()
    }    

    fn has_spec_oppty(&self) -> bool {
        true
    }

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }
    
    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = V>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = V>;
            let vtable = unsafe {
                std::mem::transmute::<_, TraitObject>(x).vtable
            };
            let data = Arc::into_raw(self);
            Some(TraitObject {
                data: data as *mut (),
                vtable: vtable,
            })
        } else {
            None
        }
    }

}


impl<K, V, VE, FE, FD> Op for Values<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: SerFunc(Vec<V>) -> VE,
    FD: SerFunc(VE) -> Vec<V>,
{   
    type Item = V;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {
                self.narrow(call_seq, input, dep_info)
            },
            1 => {
                self.shuffle(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }
    
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let fe = self.get_fe();
        let fd = self.get_fd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_doublet();
            let is_spec = call_seq.is_spec;
            return self.get_and_remove_cached_data(key, is_spec);
        }
        
        let opb = call_seq.get_next_op().clone();
        let res_iter= if opb.get_op_id() == self.prev.get_op_id() {
            self.prev.compute(call_seq, input)
        } else {
            let op = opb.to_arc_op::<dyn Op<Item = (K, V)>>().unwrap();
            op.compute(call_seq, input)
        };
        let res_iter = Box::new(res_iter.map(move |res_iter| {
            let mut block = res_iter.map(move |(k, v)| v).collect::<Vec<_>>();
            let ser_block = bincode::serialize(&block).unwrap();
            block = bincode::deserialize(&ser_block).unwrap();
            Box::new(block.into_iter()) as Box<dyn Iterator<Item = _>>
        }));

        let key = call_seq.get_caching_doublet();
        if need_cache && CACHE.get(key).is_none() {
            return self.set_cached_data(
                call_seq,
                res_iter,
            )
        }
        res_iter
    }
}

impl<K, V, VE, FE, FD> OpE for Values<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: SerFunc(Vec<V>) -> VE,
    FD: SerFunc(VE) -> Vec<V>,
{
    type ItemE = VE;
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
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
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
    #[track_caller]
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = OpVals::new(prev.get_context(), prev.number_of_splits());
        let cur_id = vals.id;
        let prev_id = prev.get_op_id();
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_id, cur_id),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().write().unwrap().insert(
            (prev_id, cur_id),
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_id, cur_id))
            )
        ); 
        MappedValues {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
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
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr, is_enc),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr, is_enc);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn fix_split_num(&self, split_num: usize) {
        self.vals.split_num.store(split_num, atomic::Ordering::SeqCst);
    }

    fn get_op_id(&self) -> OpId {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }

    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        self.next_deps.clone()
    }    

    fn has_spec_oppty(&self) -> bool {
        !self.f.has_captured_var()
    }

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }
    
    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }

    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = (K, U)>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (K, U)>;
            let vtable = unsafe {
                std::mem::transmute::<_, TraitObject>(x).vtable
            };
            let data = Arc::into_raw(self);
            Some(TraitObject {
                data: data as *mut (),
                vtable: vtable,
            })
        } else {
            None
        }
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

    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {
                self.narrow(call_seq, input, dep_info)
            },
            1 => {
                self.shuffle(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }
    
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let fe = self.get_fe();
        let fd = self.get_fd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_doublet();
            let is_spec = call_seq.is_spec;
            return self.get_and_remove_cached_data(key, is_spec);
        }
        
        let mut f = self.f.clone();
        match call_seq.get_ser_captured_var() {
            Some(ser) => f.deser_captured_var(ser),
            None  => (),
        }
        let opb = call_seq.get_next_op().clone();
        let res_iter = if opb.get_op_id() == self.prev.get_op_id() {
            self.prev.compute(call_seq, input)
        } else {
            let op = opb.to_arc_op::<dyn Op<Item = (K, V)>>().unwrap();
            op.compute(call_seq, input)
        };
        let res_iter = Box::new(res_iter.map(move |res_iter| {
            let f = f.clone();
            let mut block = res_iter.map(move |(k, v)| (k, f(v))).collect::<Vec<_>>();
            let ser_block = bincode::serialize(&block).unwrap();
            block = bincode::deserialize(&ser_block).unwrap();
            Box::new(block.into_iter()) as Box<dyn Iterator<Item = _>>
        }));

        let key = call_seq.get_caching_doublet();
        if need_cache && CACHE.get(key).is_none() {
            return self.set_cached_data(
                call_seq,
                res_iter,
            )
        }
        res_iter
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
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
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
    #[track_caller]
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = OpVals::new(prev.get_context(), prev.number_of_splits());
        let cur_id = vals.id;
        let prev_id = prev.get_op_id();
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_id, cur_id),
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().write().unwrap().insert(
            (prev_id, cur_id),
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_id, cur_id))
            )
        );
        FlatMappedValues {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
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
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr, is_enc),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr, is_enc);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn fix_split_num(&self, split_num: usize) {
        self.vals.split_num.store(split_num, atomic::Ordering::SeqCst);
    }

    fn get_op_id(&self) -> OpId {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }

    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        self.next_deps.clone()
    }

    fn has_spec_oppty(&self) -> bool {
        !self.f.has_captured_var()
    }

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }
    
    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = (K, U)>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (K, U)>;
            let vtable = unsafe {
                std::mem::transmute::<_, TraitObject>(x).vtable
            };
            let data = Arc::into_raw(self);
            Some(TraitObject {
                data: data as *mut (),
                vtable: vtable,
            })
        } else {
            None
        }
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

    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {       //narrow
                self.narrow(call_seq, input, dep_info)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let fe = self.get_fe();
        let fd = self.get_fd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_doublet();
            let is_spec = call_seq.is_spec;
            return self.get_and_remove_cached_data(key, is_spec);
        }

        let mut f = self.f.clone();
        match call_seq.get_ser_captured_var() {
            Some(ser) => f.deser_captured_var(ser),
            None  => (),
        }
        let opb = call_seq.get_next_op().clone();
        let res_iter = if opb.get_op_id() == self.prev.get_op_id() {
            self.prev.compute(call_seq, input)
        } else {
            let op = opb.to_arc_op::<dyn Op<Item = (K, V)>>().unwrap();
            op.compute(call_seq, input)
        };

        let res_iter = Box::new(res_iter.map(move |res_iter| {
            let f = f.clone();
            let mut block = res_iter.flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))).collect::<Vec<_>>();
            let ser_block = bincode::serialize(&block).unwrap();
            block = bincode::deserialize(&ser_block).unwrap();
            Box::new(block.into_iter()) as Box<dyn Iterator<Item = _>>
        }));

        let key = call_seq.get_caching_doublet();
        if need_cache && CACHE.get(key).is_none() {
            return self.set_cached_data(
                call_seq,
                res_iter,
            )
        }
        res_iter
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
