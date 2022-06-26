use std::hash::Hash;
use crate::aggregator::Aggregator;
use crate::partitioner::HashPartitioner;
use crate::op::*;

pub trait Pair<K, V>: Op<Item = (K, V)> + Send + Sync 
where 
    K: Data + Eq + Hash + Ord, 
    V: Data,
{
    #[track_caller]
    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, C)>>
    where
        Self: Sized + 'static,
    {
        let new_op = SerArc::new(Shuffled::new(
            self.get_op(),
            Arc::new(aggregator),
            partitioner,
        ));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn Op<Item = (K, Vec<V>)>>
    where
        Self: Sized + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    #[track_caller]
    fn group_by_key_using_partitioner<>(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, Vec<V>)>>
    where
        Self: Sized + 'static,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }

    #[track_caller]
    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> SerArc<dyn Op<Item = (K, V)>>
    where
        Self: Sized + 'static,
        F: SerFunc((V, V)) -> V,     
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
    }

    #[track_caller]
    fn reduce_by_key_using_partitioner<F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V,
        Self: Sized + 'static,
    {
        let create_combiner = Box::new(|v: V| v);
        let f_clone = func.clone();
        let merge_value = Box::new(move |(buf, v)| { (f_clone)((buf, v)) });
        let merge_combiners = Box::new(move |(b1, b2)| { (func)((b1, b2)) });
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    #[track_caller]
    fn values(
        &self,
    ) -> SerArc<dyn Op<Item = V>>
    where
        Self: Sized,
    {
        let new_op = SerArc::new(Values::new(self.get_op()));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn map_values<U, F>(
        &self,
        f: F,
    ) -> SerArc<dyn Op<Item = (K, U)>>
    where
        Self: Sized,
        F: SerFunc(V) -> U + Clone,
        U: Data,
    {
        let new_op = SerArc::new(MappedValues::new(self.get_op(), f));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn flat_map_values<U, F>(
        &self,
        f: F,
    ) -> SerArc<dyn Op<Item = (K, U)>>
    where
        Self: Sized,
        F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
        U: Data,
    {
        let new_op = SerArc::new(FlatMappedValues::new(self.get_op(), f));
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

    #[track_caller]
    fn join<W>(
        &self,
        other: SerArc<dyn Op<Item = (K, W)>>,
        num_splits: usize,
    ) -> SerArc<dyn Op<Item = (K, (V, W))>> 
    where
        W: Data,
    {
        let f = Box::new(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });

        let cogrouped = self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        );
        self.get_context().add_num(1);
        cogrouped.flat_map_values(Box::new(f))
    }

    #[track_caller]
    fn cogroup<W>(
        &self,
        other: SerArc<dyn Op<Item = (K, W)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Op<Item = (K, (Vec<V>, Vec<W>))>> 
    where
        W: Data,
    {
        let mut cogrouped = CoGrouped::new(self.get_op(), other.get_op(), partitioner);
        cogrouped.is_for_join = true;
        let new_op = SerArc::new(cogrouped);
        if !self.get_context().get_is_tail_comp() {
            insert_opmap(new_op.get_op_id(), new_op.get_op_base());
        }
        new_op
    }

}

// Implementing the Pair trait for all types which implements Op
impl<K, V, T> Pair<K, V> for T
where
    T: Op<Item = (K, V)>,
    K: Data + Eq + Hash + Ord, 
    V: Data, 
{}

impl<K, V, T> Pair<K, V> for SerArc<T>
where
    T: Op<Item = (K, V)>,
    K: Data + Eq + Hash + Ord, 
    V: Data, 
{}

pub struct Values<K, V>
where
    K: Data,
    V: Data,
{ 
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<V>>>>>,
}

impl<K, V> Clone for Values<K, V>
where
    K: Data,
    V: Data,
{
    fn clone(&self) -> Self {
        Values { 
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            cache_space: self.cache_space.clone(),
        }
    }
}

impl<K, V> Values<K, V>
where
    K: Data,
    V: Data,
{
    #[track_caller]
    fn new(prev: Arc<dyn Op<Item = (K, V)>>) -> Self {
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
            cache_space: Arc::new(Mutex::new(HashMap::new()))
        }
    }
}

impl<K, V> OpBase for Values<K, V>
where
    K: Data,
    V: Data,
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

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }
    
    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
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


impl<K, V> Op for Values<K, V>
where
    K: Data,
    V: Data,
{   
    type Item = V;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {
                self.narrow(call_seq, input, true)
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
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
        }
        
        let opb = call_seq.get_next_op().clone();
        let res_iter= if opb.get_op_id() == self.prev.get_op_id() {
            self.prev.compute(call_seq, input)
        } else {
            let op = opb.to_arc_op::<dyn Op<Item = (K, V)>>().unwrap();
            op.compute(call_seq, input)
        };
        let res_iter = Box::new(res_iter.map(|res_iter| 
            Box::new(res_iter.map(move |(k, v)| v)) as Box<dyn Iterator<Item = _>>
        ));

        let key = call_seq.get_caching_doublet();
        if need_cache {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }
}


pub struct MappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> U + Clone,
{ 
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    f: F,
    cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<(K, U)>>>>>,
}

impl<K, V, U, F> Clone for MappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> U + Clone,
{
    fn clone(&self) -> Self {
        MappedValues { 
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            cache_space: self.cache_space.clone(),
        }
    }
}

impl<K, V, U, F> MappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> U + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F) -> Self {
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
            cache_space: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, V, U, F> OpBase for MappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> U + Clone,
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

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }
    
    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
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


impl<K, V, U, F> Op for MappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> U + Clone,
{   
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {
                self.narrow(call_seq, input, true)
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
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
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
            Box::new(res_iter.map(move |(k, v)| (k, f(v)))) as Box<dyn Iterator<Item = _>>
        }));

        let key = call_seq.get_caching_doublet();
        if need_cache {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }
}

pub struct FlatMappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    prev: Arc<dyn Op<Item = (K, V)>>,
    f: F,    
    cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<(K, U)>>>>>,
}

impl<K, V, U, F> Clone for FlatMappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValues {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            cache_space: self.cache_space.clone(),
        }
    }
}

impl<K, V, U, F> FlatMappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Op<Item = (K, V)>>, f: F) -> Self {
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
            cache_space: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, V, U, F> OpBase for FlatMappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
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

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }
    
    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
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

impl<K, V, U, F> Op for FlatMappedValues<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
{
    type Item = (K, U);
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {       //narrow
                self.narrow(call_seq, input, true)
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
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
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
            Box::new(res_iter.flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x)))) as Box<dyn Iterator<Item = _>>
        }));

        let key = call_seq.get_caching_doublet();
        if need_cache {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }
}
