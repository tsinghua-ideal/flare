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

    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            3 | 4 => self.free_res_enc(data, marks, is_enc),
            _ => unreachable!(),
        };
    }
    
    fn get_op_id(&self) -> OpId {
        self.vals.id
    }
    
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        
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
  
    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        //3 is only for global reduce & fold (cf)
        //4 is only for local reduce & fold (sf + cf)
        if dep_info.dep_type() == 3 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let marks_enc = input.get_enc_marks::<Vec<ItemE>>();
            assert!(marks_enc.is_empty());

            let iter = data_enc.iter().map(|bl_enc| ser_decrypt::<U>(&bl_enc.clone())).collect::<Vec<_>>().into_iter();
            let u = (self.cf)(Box::new(iter));
            let ue = vec![ser_encrypt(&u)];
            (res_enc_to_ptr(ue), 0usize as *mut u8)
        } else if dep_info.dep_type() == 4 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let marks_enc = input.get_enc_marks::<Vec<ItemE>>();
            let reduced = if marks_enc.is_empty() {
                data_enc.iter().map(|bl_enc| {
                    let bl = ser_decrypt::<Vec<T>>(&bl_enc.clone());
                    (self.sf)(Box::new(bl.into_iter()))
                }).collect::<Vec<_>>()
            } else {
                assert_eq!(data_enc.len(), marks_enc.len());
                data_enc.iter().zip(marks_enc.iter())
                    .map(|(bl_enc, blmarks_enc)| {
                        let mut bl = ser_decrypt::<Vec<T>>(&bl_enc.clone());
                        let blmarks = ser_decrypt::<Vec<bool>>(&blmarks_enc.clone());
                        if !blmarks.is_empty() {
                            assert_eq!(blmarks.len(), bl.len());
                            bl = bl.into_iter()
                                .zip(blmarks.into_iter())
                                .filter(|(_, m)| *m)
                                .map(|(x, _)| x)
                                .collect::<Vec<_>>();
                        }
                        (self.sf)(Box::new(bl.into_iter()))
                    }).collect::<Vec<_>>()
            };

            let u = (self.cf)(Box::new(reduced.into_iter()));
            let ue = vec![ser_encrypt(&u)];
            (res_enc_to_ptr(ue), 0usize as *mut u8)
        } else {
            call_seq.should_filter.1 = false;
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