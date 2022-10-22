use crate::op::*;

pub struct Count<T>
where
    T: Data, 
{
    vals: Arc<OpVals>,
    prev: Arc<dyn Op<Item = T>>,
}

impl<T> Clone for Count<T>
where
    T: Data, 
{
    fn clone(&self) -> Self {
        Count {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
        }
    }
}

impl<T> Count<T>
where
    T: Data, 
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Op<Item = T>>) -> Self {
        let vals = Arc::new(OpVals::new(prev.get_context(), usize::MAX));
        /*
        prev.get_next_deps().lock().unwrap().push(
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(true))
            )
        );
        */
        Count {
            vals,
            prev,
        }
    }
}

impl<T> OpBase for Count<T>
where
    T: Data, 
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            4 => self.step0_of_clone(p_buf, p_data_enc, dep_info), 
            _ => unreachable!(),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            4 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => unreachable!(),
        }
    }

    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            4 => {
                assert_eq!(marks as usize, 0usize);
                crate::ALLOCATOR.set_switch(true);
                let res = unsafe { Box::from_raw(data as *mut Vec<u64>) };
                drop(res);
                crate::ALLOCATOR.set_switch(false);
            },
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

impl<T> Op for Count<T>
where
    T: Data, 
{
    type Item = u64;
    
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
            //because it does not compute on ciphertext
            unreachable!()
        } else if dep_info.dep_type() == 4 {
            let data_enc = input.get_enc_data::<Vec<ItemE>>();
            let marks_enc = input.get_enc_marks::<Vec<ItemE>>();

            let count = if marks_enc.is_empty() {
                data_enc.iter().map(|bl_enc| ser_decrypt::<Vec<T>>(&bl_enc.clone()).len()).sum::<usize>()
            } else {
                assert_eq!(data_enc.len(), marks_enc.len());
                data_enc.iter().zip(marks_enc.iter())
                    .map(|(bl_enc, blmarks_enc)| {
                        let bl = ser_decrypt::<Vec<T>>(&bl_enc.clone());
                        let blmarks = ser_decrypt::<Vec<bool>>(&blmarks_enc.clone());
                        if blmarks.is_empty() {
                            bl.len()
                        } else {
                            assert_eq!(blmarks.len(), bl.len());
                            blmarks.into_iter().filter(|m| *m).count()
                        }
                    }).sum::<usize>()
            };
            let res = vec![count as u64];
            (res_enc_to_ptr(res), 0usize as *mut u8)
        } else {
            call_seq.should_filter.1 = true;
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