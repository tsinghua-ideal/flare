use crate::op::*;

pub struct Reduced<T, TE, UE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    UE: Data,
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> UE + Clone,
    FD: Func(UE) -> Vec<T> + Clone,
{
    prev: Arc<dyn OpE<Item = T, ItemE = TE>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<T, TE, UE, F, FE, FD> Clone for Reduced<T, TE, UE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    UE: Data,
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> UE + Clone,
    FD: Func(UE) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        Reduced {
            prev: self.prev.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, TE, UE, F, FE, FD> Reduced<T, TE, UE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    UE: Data,
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(Vec<T>) -> UE + Clone,
    FD: Func(UE) -> Vec<T> + Clone,
{
    pub(crate) fn new(prev: Arc<dyn OpE<Item = T, ItemE = TE>>, f: F, fe: FE, fd: FD) -> Self {
        /*         
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(true))
            )
        ); 
        */
        Reduced {
            prev,
            f,
            fe,
            fd,
        }
    }
}

impl<T, TE, UE, F, FE, FD> OpBase for Reduced<T, TE, UE, F, FE, FD>
where
    T: Data,
    TE: Data,
    UE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> UE,
    FD: SerFunc(UE) -> Vec<T>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 => self.step0_of_clone(p_buf, p_data_enc, dep_info), 
            _ => self.prev.build_enc_data_sketch(p_buf, p_data_enc, dep_info),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => self.prev.clone_enc_data_out(p_out, p_data_enc, dep_info),
        } 
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 => self.free_res_enc(res_ptr),
            _ => self.prev.call_free_res_enc(res_ptr, dep_info),
        };
    }

    fn fix_split_num(&self, split_num: usize) {
        self.prev.fix_split_num(split_num)
    }

    fn get_op_id(&self) -> OpId {
        self.prev.get_op_id()
    }
    
    fn get_context(&self) -> Arc<Context> {
        self.prev.get_context()
    }
    
    fn get_deps(&self) -> Vec<Dependency> {
        self.prev.get_deps()
    }
    
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        self.prev.get_next_deps()
    }

    fn has_spec_oppty(&self) -> bool {
        self.prev.has_spec_oppty()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
		self.compute_start(tid, call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.prev.randomize_in_place(input, seed, num)
    }

    fn set_sampler(&self, with_replacement: bool, fraction: f64) {
        self.prev.set_sampler(with_replacement, fraction)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.prev.etake(input ,should_take, have_take)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        self.prev.clone().__to_arc_op(id)
    }
}

impl<T, TE, UE, F, FE, FD> Op for Reduced<T, TE, UE, F, FE, FD>
where
    T: Data,
    TE: Data,
    UE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> UE,
    FD: SerFunc(UE) -> Vec<T>,
{
    type Item = T;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start (&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        //3 is only for reduce & fold
        if dep_info.dep_type() == 3 {
            let data_enc = input.get_enc_data::<Vec<TE>>();
            let data = self.prev.batch_decrypt(data_enc.clone()); //need to check security
            let result = (self.f)(Box::new(data.into_iter()));
            let now = Instant::now();
            let result_enc = self.batch_encrypt(result); 
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("in enclave encrypt {:?} s", dur); 
            res_enc_to_ptr(result_enc) 
        }
        else {
            if call_seq.need_cache() {
                self.prev.compute_start(tid, call_seq, input, dep_info)
            } else {
                let (result_iter, handle) = self.compute(call_seq, input);
                let result = result_iter.collect::<Vec<Self::Item>>();
                //println!("In narrow(before encryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                let result_ptr = match dep_info.need_encryption() {
                    true => {
                        let now = Instant::now();
                        let result_enc = self.prev.batch_encrypt(result); 
                        //println!("In narrow(after encryption), memroy usage: {:?} B", crate::ALLOCATOR.lock().get_memory_usage());
                        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                        println!("in enclave encrypt {:?} s", dur); 
                        res_enc_to_ptr(result_enc) 
                    },
                    false => {
                        let result_ptr = Box::into_raw(Box::new(result)) as *mut u8;
                        result_ptr
                    },
                };
                if let Some(handle) = handle {
                    handle.join();
                }   
                result_ptr
            }
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        //move some parts in compute start to this part, this part is originally used to reduce ahead to shrink size
        let (res_iter, handle) = self.prev.compute(call_seq, input);
        (Box::new((self.f)(res_iter).into_iter()), handle)        
    }

}

impl<T, TE, UE, F, FE, FD> OpE for Reduced<T, TE, UE, F, FE, FD>
where
    T: Data,
    TE: Data,   
    UE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(Vec<T>) -> UE,
    FD: SerFunc(UE) -> Vec<T>,
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