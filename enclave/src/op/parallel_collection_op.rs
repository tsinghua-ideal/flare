use std::marker::PhantomData;
use crate::op::*;

pub struct ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    vals: Arc<OpVals>, 
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    fe: FE,
    fd: FD,
    _marker_t: PhantomData<T>,
    _marker_te: PhantomData<TE>,
}

impl<T, TE, FE, FD> Clone for ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        ParallelCollection {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
            _marker_t: PhantomData,
            _marker_te: PhantomData,
        }
    }
} 

impl<T, TE, FE, FD> ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    #[track_caller]
    pub fn new(context: Arc<Context>, fe: FE, fd: FD, num_splits: usize) -> Self {
        let vals = OpVals::new(context.clone(), num_splits);
        ParallelCollection {
            vals: Arc::new(vals),
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            fe,
            fd,
            _marker_t: PhantomData,
            _marker_te: PhantomData,
        }
    }
}

impl<T, TE, FE, FD> OpBase for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
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

    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr);
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
        if id == TypeId::of::<dyn Op<Item = T>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = T>;
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

impl<T, TE, FE, FD> Op for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
{
    type Item = T;

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
        let now = Instant::now();
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let fd = self.get_fd();

        //In this case, the data is either cached outside enclave or inside enclave
        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_doublet();
            return self.get_and_remove_cached_data(key)
        }

        let len = input.get_enc_data::<Vec<TE>>().len();
        let res_iter = Box::new((0..len).map(move|i| {
            let data = input.get_enc_data::<Vec<TE>>();
            Box::new((fd)(data[i].clone()).into_iter()) as Box<dyn Iterator<Item = _>>
        }));

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in enclave decrypt {:?} s", dur);  
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

impl<T, TE, FE, FD> OpE for ParallelCollection<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>,
{
    type ItemE = TE;
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
