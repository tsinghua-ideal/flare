use std::collections::BTreeMap;
use std::hash::Hash;

use crate::CAVE;
use crate::aggregator::Aggregator;
use crate::dependency::ShuffleDependency;
use crate::op::*;

pub struct Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    parent: Arc<dyn Op<Item = (K, V)>>,
    aggregator: Arc<Aggregator<K, V, C>>,
    part: Box<dyn Partitioner>,
    fe: FE,
    fd: FD,
}

impl<K, V, C, KE, CE, FE, FD> Clone for Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    fn clone(&self) -> Self {
        Shuffled {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            part: self.part.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    #[track_caller]
    pub(crate) fn new(
        parent: Arc<dyn Op<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let ctx = parent.get_context();
        let mut vals = OpVals::new(ctx);
        let cur_id = vals.id;
        let prev_id = parent.get_op_id();
        let dep = Dependency::ShuffleDependency(Arc::new(
            ShuffleDependency::new(
                false,
                aggregator.clone(),
                part.clone(),
                0,
                prev_id,
                cur_id,
                Box::new(fe.clone()),
                Box::new(fd.clone()),
            ),
        ));

        vals.deps.push(dep.clone());
        let vals = Arc::new(vals);
        parent.get_next_deps().write().unwrap().insert((prev_id, cur_id), dep);
        Shuffled {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            parent,
            aggregator,
            part,
            fe,
            fd,
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> OpBase for Shuffled<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 | 2  => self.step0_of_clone(p_buf, p_data_enc, dep_info),
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
        true
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
    }
    
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(tid, call_seq, data_ptr, dep_info)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = (K, C)>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (K, C)>;
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

impl<K, V, C, KE, CE, FE, FD> Op for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
    type Item = (K, C);

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {      //No shuffle
                self.narrow(call_seq, data_ptr, dep_info)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, data_ptr, dep_info)
            }
            2 => {      //Shuffle read
                let aggregator = self.aggregator.clone(); 
                let data_enc = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<(KE, CE)>>) };
                println!("cur mem before decryption: {:?}", crate::ALLOCATOR.lock().get_memory_usage());
                let now = Instant::now(); 
                let data = data_enc.clone()
                    .into_iter()
                    .map(|v | {
                        //batch_decrypt
                        let mut data = Vec::new();
                        for block in v {
                            let mut pt = self.get_fd()(block);
                            data.append(&mut pt); //need to check security
                        }
                        data
                    }).collect::<Vec<_>>();
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                println!("cur mem after decryption: {:?}, in enclave decrypt: {:?} s", crate::ALLOCATOR.lock().get_memory_usage(), dur);
                forget(data_enc);
                let remained_ptr = CAVE.lock().unwrap().remove(&tid);
                let mut combiners: BTreeMap<K, Option<C>> = match remained_ptr {
                    Some(ptr) => *unsafe { Box::from_raw(ptr as *mut u8 as *mut BTreeMap<K, Option<C>>) },
                    None => BTreeMap::new(),
                };
                println!("cur mem before shuffle read: {:?}", crate::ALLOCATOR.lock().get_memory_usage()); 
                if data.len() > 0 {    
                    let mut min_max_kv = data[0].last().unwrap(); 
                    for idx in 1..data.len() {
                        let cand = data[idx].last().unwrap();
                        if min_max_kv.0 > cand.0 {
                            min_max_kv = cand;
                        }
                    }
                    let min_max_k = min_max_kv.0.clone();

                    for (k, c) in data.into_iter().flatten() {
                        if let Some(old_c) = combiners.get_mut(&k) {
                            let old = old_c.take().unwrap();
                            let input = ((old, c),);
                            let output = aggregator.merge_combiners.call(input);
                            *old_c = Some(output);
                        } else {
                            combiners.insert(k, Some(c));
                        }
                    }
                    let remained = combiners.split_off(&min_max_k);
                    //Temporary stored for next computation
                    CAVE.lock().unwrap().insert(tid, Box::into_raw(Box::new(remained)) as *mut u8 as usize);
                }
                println!("cur mem after shuffle read: {:?}", crate::ALLOCATOR.lock().get_memory_usage());
                let result = combiners.into_iter().map(|(k, v)| (k, v.unwrap())).collect::<Vec<Self::Item>>();
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8; 
                result_ptr
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<(K, C)>) };
        (Box::new(data.into_iter()), None)
    }
}

impl<K, V, C, KE, CE, FE, FD> OpE for Shuffled<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
    type ItemE = (KE, CE);
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
