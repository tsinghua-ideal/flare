use crate::op::*;

pub struct Aggregated<T, U, SF, CF>
where
    T: Data, 
    U: Data,
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> U + Clone,
{
    vals: Arc<OpVals>,
    prev: Arc<dyn Op<Item = T>>,
    sf: SF,
    cf: CF,
}

impl<T, U, SF, CF> Clone for Aggregated<T, U, SF, CF>
where
    T: Data, 
    U: Data,
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> U + Clone,
{
    fn clone(&self) -> Self {
        Aggregated {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
            sf: self.sf.clone(),
            cf: self.cf.clone(),
        }
    }
}

impl<T, U, SF, CF> Aggregated<T, U, SF, CF>
where
    T: Data, 
    U: Data,
    SF: Func(Box<dyn Iterator<Item = T>>) -> U + Clone,
    CF: Func(Box<dyn Iterator<Item = U>>) -> U + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, sf: SF, cf: CF) -> Self {
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
        }
    }
}

impl<T, U, SF, CF> OpBase for Aggregated<T, U, SF, CF>
where
    T: Data, 
    U: Data,
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> U,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 | 4 => self.step0_of_clone(p_buf, p_data_enc, dep_info), 
            _ => unreachable!(),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 | 4 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => unreachable!(),
        }
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 | 4 => self.free_res_enc(res_ptr, is_enc),
            _ => unreachable!(),
        };
    }
    
    fn get_op_id(&self) -> OpId {
        self.vals.id
    }
    
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(call_seq, input, dep_info)
    }
}

impl<T, U, SF, CF> Op for Aggregated<T, U, SF, CF>
where
    T: Data, 
    U: Data,
    SF: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    CF: SerFunc(Box<dyn Iterator<Item = U>>) -> U,
{
    type Item = U;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8{
        //3 is only for global reduce & fold (cf)
        //4 is only for local reduce & fold (sf + cf)
        if dep_info.dep_type() == 3 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let u = (self.cf)(Box::new(data_enc.clone()
                .into_iter()
                .map(|x| ser_decrypt::<U>(&x))
                .collect::<Vec<_>>()
                .into_iter()));
            let ue = vec![ser_encrypt(&u)];
            res_enc_to_ptr(ue)
        } else if dep_info.dep_type() == 4 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let len = data_enc.len();
            let mut reduced = Vec::new();
            for i in 0..len {
                let block = ser_decrypt::<Vec<T>>(&data_enc[i].clone());
                reduced.push((self.sf)(Box::new(block.into_iter())));  
            }
            let u = (self.cf)(Box::new(reduced.into_iter()));
            let ue = vec![ser_encrypt(&u)];
            res_enc_to_ptr(ue) 
        } else {
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
        unreachable!()
    }

}