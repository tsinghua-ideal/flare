use crate::op::*;

pub struct Reduced<T, F>
where
    T: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
{
    vals: Arc<OpVals>,
    prev: Arc<dyn Op<Item = T>>,
    f: F,
}

impl<T, F> Clone for Reduced<T, F>
where
    T: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        Reduced {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
            f: self.f.clone(),
        }
    }
}

impl<T, F> Reduced<T, F>
where
    T: Data, 
    F: Func(Box<dyn Iterator<Item = T>>) -> Vec<T> + Clone,
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
        Reduced {
            vals,
            prev,
            f,
        }
    }
}

impl<T, F> OpBase for Reduced<T, F>
where
    T: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
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

impl<T, F> Op for Reduced<T, F>
where
    T: Data,
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Vec<T>,
{
    type Item = T;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        //3 is only for global reduce & fold
        //4 is only for local reduce & fold
        if dep_info.dep_type() == 3 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let data = data_enc.clone()
                .into_iter()
                .map(|x| ser_decrypt(&x))
                .collect::<Vec<_>>();
            let result = (self.f)(Box::new(data.into_iter()));
            let result_enc = result.into_iter()
                .map(|x| ser_encrypt(&x))
                .collect::<Vec<_>>();
            res_enc_to_ptr(result_enc) 
        } else if dep_info.dep_type() == 4 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let data = batch_decrypt(data_enc, true);
            let result = (self.f)(Box::new(data.into_iter()));
            let result_enc = result.into_iter()
                .map(|x| ser_encrypt(&x))
                .collect::<Vec<_>>();
            res_enc_to_ptr(result_enc)
        } else {
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
                // let result_iter = self.compute(&mut call_seq, input);
                // let mut acc = create_enc();
                // for result in result_iter {
                //     let block_enc = batch_encrypt(&result.collect::<Vec<_>>(), true);
                //     combine_enc(&mut acc, block_enc);
                // }
                // to_ptr(acc)
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