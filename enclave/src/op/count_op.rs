use crate::op::*;

pub struct Count<T, TE>
where
    T: Data, 
    TE: Data, 
{
    vals: Arc<OpVals>,
    prev: Arc<dyn OpE<Item = T, ItemE = TE>>,
}

impl<T, TE> Clone for Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    fn clone(&self) -> Self {
        Count {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
        }
    }
}

impl<T, TE> Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn OpE<Item = T, ItemE = TE>>) -> Self {
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

impl<T, TE> OpBase for Count<T, TE>
where
    T: Data, 
    TE: Data,
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

impl<T, TE> Op for Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    type Item = u64;
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }
  
    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8{
        //3 is only for reduce & fold
        if dep_info.dep_type() == 3 {
            //self.narrow(call_seq, input, dep_info)
            let result_iter = self.compute(call_seq, input);
            let result = result_iter.flat_map(|x| x.collect::<Vec<_>>())
                .collect::<Vec<Self::Item>>()[0];
            result as *mut u8
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
        let mut count = 0;
        for i in 0..len {
            let block = self.prev.get_fd()(data_enc[i].clone());
            count += block.len(); 
        }

        Box::new(vec![Box::new(vec![count as u64].into_iter())]
            .into_iter()
            .map(|x| x as Box<dyn Iterator<Item = _>>))
    }

}

impl<T, TE> OpE for Count<T, TE>
where
    T: Data, 
    TE: Data,
{
    type ItemE = Vec<u64>;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        Box::new(|v: Vec<u64>| v)
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        Box::new(|v: Vec<u64>| v)
    }
}