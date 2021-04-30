use crate::dependency::RangeDependency;
use crate::op::*;

pub struct Union<T: Data, TE: Data>
{
    ops: Vec<Arc<dyn OpE<Item = T, ItemE = TE>>>,
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
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
    #[track_caller]
    pub(crate) fn new(ops: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> Self {
        let mut vals_ = OpVals::new(ops[0].get_context(), 0);
        let cur_id = vals_.id;
        
        let part = match Union::has_unique_partitioner(ops) {
            true => {
                for prev in ops {
                    let prev_id = prev.get_op_id();
                    let new_dep = Dependency::NarrowDependency(Arc::new(
                        OneToOneDependency::new(prev_id, cur_id)
                    ));
                    vals_.deps.push(new_dep.clone());
                    prev.get_next_deps().write().unwrap().insert(
                        (prev_id, cur_id),
                        new_dep
                    );
                }
                Some(ops[0].partitioner().unwrap())
            },
            false => {
                let mut pos = 0;
                for prev in ops {
                    let prev_id = prev.get_op_id();
                    let num_parts = prev.number_of_splits();
                    let new_dep = Dependency::NarrowDependency(Arc::new(
                        RangeDependency::new(0, pos, num_parts, prev_id, cur_id)
                    ));
                    vals_.deps.push(new_dep.clone());
                    prev.get_next_deps().write().unwrap().insert(
                        (prev_id, cur_id),
                        new_dep,
                    );
                    pos += num_parts;
                } 
                None
            },
        };
        let split_num = match &part {
            None => {
                ops.iter().fold(0, |acc, op| acc + op.number_of_splits())
            },
            Some(part) => {
                part.get_num_of_partitions()
            }
        };
        vals_.split_num.store(split_num, atomic::Ordering::SeqCst);
        let vals = Arc::new(vals_);
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
            0 | 1 | 2 => self.step0_of_clone(p_buf, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 | 2 => self.step1_of_clone(p_out, p_data_enc, dep_info), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 => self.free_res_enc(res_ptr),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn fix_split_num(&self, split_num: usize) {
        self.vals.split_num.store(split_num, atomic::Ordering::SeqCst);
    }

    fn get_op_id(&self) -> OpId {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    
    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }
    
    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        self.next_deps.clone()
    }
    
    fn has_spec_oppty(&self) -> bool {
        //There is no item in branch_op_history for union op and its prev op
        //Thus block the speculative execution across union op
        true
    }

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
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

    fn compute_start (&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 | 2 => {    
                self.narrow(call_seq, input, dep_info)
            },
            1 => { 
                self.shuffle(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_triplet();
            let val = self.get_and_remove_cached_data(key);
            return (Box::new(val.into_iter()), None); 
        }
        
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) };
        let res_iter = Box::new(data.into_iter());

        if need_cache {
            let key = call_seq.get_caching_triplet();
            if CACHE.get(key).is_none() { 
                return self.set_cached_data(
                    call_seq.is_survivor(),
                    call_seq.is_caching_final_rdd(),
                    key,
                    res_iter
                );
            }
        }

        (res_iter, None)
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


