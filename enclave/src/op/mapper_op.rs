use crate::op::*;

pub struct Mapper<T: Data, U: Data, F>
where
    F: Func(T) -> U + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    cache_space: Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<U>>, Vec<Vec<bool>>)>>>,
}

impl<T: Data, U: Data, F> Clone for Mapper<T, U, F>
where
    F: Func(T) -> U + Clone,
{
    fn clone(&self) -> Self {
        Mapper {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            cache_space: self.cache_space.clone(),
        }
    }
}

impl<T: Data, U: Data, F> Mapper<T, U, F>
where
    F: Func(T) -> U + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F) -> Self {
        let mut vals = OpVals::new(prev.get_context(), prev.number_of_splits());
        let cur_id = vals.id;
        let prev_id = prev.get_op_id();
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_id, cur_id)
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().write().unwrap().insert(
            (prev_id, cur_id),
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_id, cur_id))
            )
        );
        Mapper {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            prev,
            f,
            cache_space: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T: Data, U: Data, F> OpBase for Mapper<T, U, F>
where
    F: SerFunc(T) -> U,
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

    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 12 | 14 => self.free_res_enc(data, marks, is_enc),
            1 | 11 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(data, dep_info, is_enc);
            },
            13 => {
                assert_eq!(marks as usize, 0usize);
                crate::ALLOCATOR.set_switch(true);
                let res = unsafe { Box::from_raw(data as *mut Vec<Vec<ItemE>>) };
                drop(res);
                crate::ALLOCATOR.set_switch(false);
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

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        
		self.compute_start(call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *mut u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn etake(&self, input: *mut u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }
    
    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = U>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = U>;
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

impl<T: Data, U: Data, F> Op for Mapper<T, U, F>
where
    F: SerFunc(T) -> U,
{
    type Item = U;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<Self::Item>>, Vec<Vec<bool>>)>>> {
        self.cache_space.clone()
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0usize);
            return self.get_and_remove_cached_data(call_seq);
        }
        
        let mut f = self.f.clone();
        match call_seq.get_ser_captured_var() {
            Some(ser) => {
                f.deser_captured_var(ser)
            },
            None  => {
                ()
            },
        }
        let opb = call_seq.get_next_op().clone();
        let res_iter = if opb.get_op_id() == self.prev.get_op_id() {
            self.prev.compute(call_seq, input)
        } else {
            let op = opb.to_arc_op::<dyn Op<Item = T>>().unwrap();
            op.compute(call_seq, input)
        };
        let res_iter = Box::new(res_iter.map(move |(bl_iter, blmarks)| {
            (Box::new(bl_iter.map(f.clone())) as Box<dyn Iterator<Item = _>>, blmarks)
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

