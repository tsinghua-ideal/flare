use crate::dependency::OneToOneDependency;
use crate::op::*;

pub struct Union<T: Data, TE: Data>
{
    ops: Vec<Arc<dyn OpE<Item = T, ItemE = TE>>>,
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(usize, usize), Dependency>>>,
    part: Option<Box<dyn Partitioner>>,
}

impl<T: Data, TE: Data> Clone for Union<T, TE>
{
    fn clone(&self) -> Self {
        Union {
            ops: self.ops.clone(),
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            part: self.part.clone(),
        }
    }
}

impl<T: Data, TE: Data> Union<T, TE>
{
    pub(crate) fn new(ops: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> Self {
        let mut vals = OpVals::new(ops[0].get_context());
        let cur_id = vals.id;

        for prev in ops {
            let prev_id = prev.get_id();
            vals.deps
                .push(Dependency::NarrowDependency(Arc::new(
                    OneToOneDependency::new(prev_id, cur_id)
                )));
            prev.get_next_deps().write().unwrap().insert(
                (prev_id, cur_id),
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(prev_id, cur_id))
                )
            );
        } 

        let vals = Arc::new(vals);
        let part = match Union::has_unique_partitioner(ops) {
            true => ops[0].partitioner(),
            false => None,
        };
        let ops: Vec<_> = ops.iter().map(|op| op.clone().into()).collect();

        Union {
            ops,
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            part,
        }
    }

    fn has_unique_partitioner(ops: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> bool {
        ops.iter()
            .map(|p| p.partitioner())
            .try_fold(None, |prev: Option<Box<dyn Partitioner>>, p| {
                if let Some(partitioner) = p {
                    if let Some(prev_partitioner) = prev {
                        if prev_partitioner.equals((&*partitioner).as_any()) {
                            // only continue in case both partitioners are the same
                            Ok(Some(partitioner))
                        } else {
                            Err(())
                        }
                    } else {
                        // first element
                        Ok(Some(partitioner))
                    }
                } else {
                    Err(())
                }
            })
            .is_ok()
    }

}

impl<T: Data, TE: Data> OpBase for Union<T, TE>
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

    fn get_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    
    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }
    
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(usize, usize), Dependency>>> {
        self.next_deps.clone()
    }
    
    fn has_spec_oppty(&self, matching_id: usize) -> bool {
        todo!()
    }

    fn number_of_splits(&self) -> usize {
        todo!()
    }

    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(tid, call_seq, data_ptr, dep_info)
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

impl<T: Data, TE: Data> Op for Union<T, TE>
{
    type Item = T;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {       //No shuffle later
                self.narrow(call_seq, data_ptr, dep_info)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, data_ptr, dep_info)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) };
        (Box::new(data.into_iter()), None)
    }

}

impl<T: Data, TE: Data> OpE for Union<T, TE>
{
    type ItemE = TE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        self.ops[0].get_fe()
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        self.ops[0].get_fd()
    }
}


