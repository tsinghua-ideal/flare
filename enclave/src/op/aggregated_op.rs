use crate::op::*;

pub struct Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    UE: Data, 
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> Vec<U> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    vals: Arc<OpVals>,
    prev: Arc<dyn OpE<Item = T, ItemE = TE>>,
    sf: SF,
    cf: CF,
    fe: FE,
    fd: FD,
}

impl<T, U, TE, UE, SF, CF, FE, FD> Clone for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> Vec<U> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        Aggregated {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
            sf: self.sf.clone(),
            cf: self.cf.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, U, TE, UE, SF, CF, FE, FD> Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> Vec<U> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn OpE<Item = T, ItemE = TE>>, sf: SF, cf: CF, fe: FE, fd: FD) -> Self {
        let vals = Arc::new(OpVals::new(prev.get_context(), usize::MAX));
        /*
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(true))
            )
        );
        */
        Aggregated {
            vals,
            prev,
            sf,
            cf,
            fe,
            fd,
        }
    }
}

impl<T, U, TE, UE, SF, CF, FE, FD> OpBase for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> Vec<U>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
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

impl<T, U, TE, UE, SF, CF, FE, FD> Op for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> Vec<U>,
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
  
    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8{
        //3 is only for reduce & fold
        if dep_info.dep_type() == 3 {
            self.narrow(call_seq, input, dep_info)
        }
        else {
            let opb = call_seq.get_next_op().clone();
            if opb.get_op_id() == self.prev.get_op_id() {
                self.prev.compute_start(call_seq, input, dep_info)
            } else {
                let op = opb.to_arc_op::<dyn Op<Item = T>>().unwrap();
                op.compute_start(call_seq, input, dep_info)
            }
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_enc = input.get_enc_data::<Vec<TE>>();
        let len = data_enc.len();
        let mut reduced = Vec::new();
        for i in 0..len {
            let block = self.prev.get_fd()(data_enc[i].clone());
            reduced.push((self.sf)(Box::new(block.into_iter())));  
        }
        Box::new(vec![Box::new((self.cf)(Box::new(reduced.into_iter())).into_iter())]
            .into_iter()
            .map(|x| x as Box<dyn Iterator<Item = _>>)
        )
    }

}

impl<T, U, TE, UE, SF, CF, FE, FD> OpE for Aggregated<T, U, TE, UE, SF, CF, FE, FD>
where
    T: Data, 
    U: Data,
    TE: Data,
    UE: Data, 
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> Vec<U>,
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