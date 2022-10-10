use crate::op::*;

pub struct Fold<T, F>
where
    T: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> T + Clone,
{
    vals: Arc<OpVals>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
}

impl<T, F> Clone for Fold<T, F>
where
    T: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> T + Clone,
{
    fn clone(&self) -> Self {
        Fold {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
        }
    }
}

impl<T, F> Fold<T, F>
where
    T: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> T + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>, f: F) -> Self {
        let vals = Arc::new(OpVals::new(prev.get_context(), usize::MAX));
        /*
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(true))
            )
        );
        */
        Fold {
            vals,
            prev,
            f,
        }
    }
}

impl<T, F> OpBase for Fold<T, F>
where
    T: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> T,
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

impl<T, F> Op for Fold<T, F>
where
    T: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> T,
{
    type Item = T;
    
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

            let t = (self.f)(Box::new(data_enc.clone()
                .into_iter()
                .map(|x| ser_decrypt::<T>(&x))
                .collect::<Vec<_>>()
                .into_iter()));
            let ue = vec![ser_encrypt(&t)];
            (res_enc_to_ptr(ue), 0usize as *mut u8)
        } else if dep_info.dep_type() == 4 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let marks_enc = input.get_enc_marks::<Vec<ItemE>>();
            assert!(data_enc.len() == marks_enc.len() || marks_enc.is_empty());

            let data = batch_decrypt(data_enc, true);
            let marks = batch_decrypt(marks_enc, true);
            let t = if marks.is_empty() {
                (self.f)(Box::new(data.into_iter()))
            } else {
                assert_eq!(data.len(), marks.len());
                (self.f)(Box::new(data.into_iter().zip(marks.into_iter()).filter(|(_, m)| *m).map(|(x, _)| x)))
            };
            let ue = vec![ser_encrypt(&t)];
            (res_enc_to_ptr(ue), 0usize as *mut u8)
        } else {
            call_seq.should_filter.1 = false;
            let opb = call_seq.get_next_op().clone();
            if call_seq.need_cache() {
                if opb.get_op_id() == self.prev.get_op_id() {
                    self.prev.compute_start(call_seq, input, dep_info)
                } else {
                    let op = opb.to_arc_op::<dyn Op<Item = T>>().unwrap();
                    op.compute_start(call_seq, input, dep_info)
                }
            } else {
                self.narrow(call_seq, input, true)
            }
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        //move some parts in compute start to this part
        let res_iter = self.prev.compute(call_seq, input);
        let f = self.f.clone();
        Box::new(res_iter.map(move |(bl_iter, blmarks)| {
            if blmarks.is_empty() {
                (Box::new(vec![(f)(bl_iter)].into_iter()) as Box<dyn Iterator<Item = _>>, blmarks)
            } else {
                (Box::new(vec![(f)(Box::new(bl_iter.zip(blmarks.into_iter()).filter(|(_, m)| *m).map(|(x, _)| x)))].into_iter()) as Box<dyn Iterator<Item = _>>, Vec::new())
            }
        }))
    }

}