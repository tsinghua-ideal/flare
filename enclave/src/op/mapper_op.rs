use crate::dependency::OneToOneDependency;
use crate::op::*;

pub struct Mapper<T: Data, U: Data, UE: Data, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<Mutex<Vec<Dependency>>>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> Clone for Mapper<T, U, UE, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        Mapper {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> Mapper<T, U, UE, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = OpVals::new(prev.get_context());
        let mut prev_ids = prev.get_prev_ids();
        prev_ids.insert(prev.get_id()); 
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev_ids.clone())
            )));
        let vals = Arc::new(vals);
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(prev_ids))
            )
        );
        Mapper {
            vals,
            next_deps: Arc::new(Mutex::new(Vec::<Dependency>::new())),
            prev,
            f,
            fe,
            fd,
        }
    }
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> OpBase for Mapper<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
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
    
    fn get_next_deps(&self) -> Arc<Mutex<Vec<Dependency>>> {
        self.next_deps.clone()
    }
    
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        
		self.compute_start(tid, call_seq, data_ptr, is_shuffle, cache_meta)
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

impl<T: Data, U: Data, UE: Data, F, FE, FD> Op for Mapper<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    type Item = U;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(call_seq, data_ptr, cache_meta)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, data_ptr, cache_meta)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let have_cache = cache_meta.count_cached_down();
        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = (cache_meta.cached_rdd_id, cache_meta.part_id, cache_meta.sub_part_id);
            let val = self.get_and_remove_cached_data(key);
            return (Box::new(val.into_iter()), None); 
        }
        
        let opb = call_seq.get_next_op().clone();
        let (res_iter, handle) = if opb.get_id() == self.prev.get_id() {
            self.prev.compute(call_seq, data_ptr, cache_meta)
        } else {
            let op = opb.to_arc_op::<dyn Op<Item = T>>().unwrap();
            op.compute(call_seq, data_ptr, cache_meta)
        };
        let res_iter = Box::new(res_iter.map(self.f.clone()));
        
        //println!("In mapper_op, memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
        let need_cache = cache_meta.count_caching_down();
        if need_cache {
            assert!(handle.is_none());
            let key = (cache_meta.caching_rdd_id, cache_meta.part_id, cache_meta.sub_part_id);
            if CACHE.get(key).is_none() { 
                
                return self.set_cached_data(key, res_iter);
                
                /*
                let (res_iter, handle) = self.set_cached_data(key, res_iter);
                println!("In mapper_op (before join), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                if let Some(handle) = handle {
                    handle.join();
                }
                return (res_iter, None);
                */
            }
        }

        (res_iter, handle)
    }

}

impl<T: Data, U: Data, UE: Data, F, FE, FD> OpE for Mapper<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    type ItemE = UE;
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


