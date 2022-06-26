use crate::dependency::RangeDependency;
use crate::op::*;

pub struct Union<T: Data>
{
    ops: Vec<Arc<dyn Op<Item = T>>>,
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    part: Option<Box<dyn Partitioner>>,
    cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<T>>>>>,
}

impl<T: Data> Clone for Union<T>
{
    fn clone(&self) -> Self {
        Union {
            ops: self.ops.clone(),
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            part: self.part.clone(),
            cache_space: self.cache_space.clone(),
        }
    }
}

impl<T: Data> Union<T>
{
    #[track_caller]
    pub(crate) fn new(ops: &[Arc<dyn Op<Item = T>>]) -> Self {
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
            cache_space: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn has_unique_partitioner(ops: &[Arc<dyn Op<Item = T>>]) -> bool {
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

impl<T: Data> OpBase for Union<T>
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

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr, is_enc),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr, is_enc);
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
    
    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
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

impl<T: Data> Op for Union<T>
{
    type Item = T;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {    
                self.narrow(call_seq, input, true)
            },
            1 => { 
                self.shuffle(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
        }
        
        let opb = call_seq.get_next_op().clone();
        let op = opb.to_arc_op::<dyn Op<Item = T>>().unwrap();
        let res_iter = op.compute(call_seq, input);

        let key = call_seq.get_caching_doublet();
        if need_cache {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }

}


