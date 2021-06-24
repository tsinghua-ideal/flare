use crate::op::*;

pub struct Reduced<T, TE, UE, F, FE, FD>
where
    T: Data, 
    TE: Data, 
    UE: Data,
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
    FE: Func(T) -> UE + Clone,
    FD: Func(UE) -> T + Clone,
{
    vals: Arc<OpVals>,
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
    FE: Func(T) -> UE + Clone,
    FD: Func(UE) -> T + Clone,
{
    fn clone(&self) -> Self {
        Reduced {
            vals: self.vals.clone(),
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
    FE: Func(T) -> UE + Clone,
    FD: Func(UE) -> T + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn OpE<Item = T, ItemE = TE>>, f: F, fe: FE, fd: FD) -> Self {
        let vals = Arc::new(OpVals::new(prev.get_context(), usize::MAX));
        /*         
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(true))
            )
        ); 
        */
        Reduced {
            vals,
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
    FE: SerFunc(T) -> UE,
    FD: SerFunc(UE) -> T,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 => self.step0_of_clone(p_buf, p_data_enc, dep_info), 
            _ => unreachable!(),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => unreachable!(),
        }
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 => self.free_res_enc(res_ptr, is_enc),
            _ => unreachable!(),
        };
    }
    
    fn get_op_id(&self) -> OpId {
        self.vals.id
    }
    
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(call_seq, input, dep_info)
    }

    fn pre_merge(&self, dep_info: DepInfo, tid: u64, input: Input) -> usize {
        unreachable!()
    }
}

impl<T, TE, UE, F, FE, FD> Op for Reduced<T, TE, UE, F, FE, FD>
where
    T: Data,
    TE: Data,
    UE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(T) -> UE,
    FD: SerFunc(UE) -> T,
{
    type Item = T;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        //3 is only for reduce & fold
        if dep_info.dep_type() == 3 {
            let data_enc = input.get_enc_data::<Vec<TE>>();
            let data = self.prev.batch_decrypt(data_enc.clone()); //need to check security
            let result = (self.f)(Box::new(data.into_iter()));
            let now = Instant::now();
            let result_enc = result.into_iter()
                .map(|x| (self.fe)(x))
                .collect::<Vec<_>>();
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            res_enc_to_ptr(result_enc) 
        } else {
            let op = call_seq.get_next_op();
            assert!(op.get_op_id() == self.prev.get_op_id());
            if call_seq.need_cache() {
                self.prev.compute_start(call_seq, input, dep_info)
            } else {
                let result_iter = self.compute(call_seq, input);
                let mut acc = create_enc();
                for result in result_iter {
                    for block in self.prev.batch_encrypt(result.collect::<Vec<_>>()) {
                        merge_enc(&mut acc, &block)
                    }
                }
                to_ptr(acc)
            }
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        //move some parts in compute start to this part, this part is originally used to reduce ahead to shrink size
        let res_iter = self.prev.compute(call_seq, input);
        let f = self.f.clone();
        Box::new(res_iter.map(move |res_iter|
            Box::new((f)(res_iter).into_iter()) as Box<dyn Iterator<Item = _>>))
    }

}

impl<T, TE, UE, F, FE, FD> OpE for Reduced<T, TE, UE, F, FE, FD>
where
    T: Data,
    TE: Data,   
    UE: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
    FE: SerFunc(T) -> UE,
    FD: SerFunc(UE) -> T,
{
    type ItemE = UE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        unreachable!()
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        unreachable!()
    }
}