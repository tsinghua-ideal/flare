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
    
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        
		self.compute_start(tid, call_seq, input, dep_info)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
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

    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {      //No shuffle
                self.narrow(call_seq, input, dep_info)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, input, dep_info)
            }
            2 => {      //Shuffle read
                let aggregator = self.aggregator.clone(); 
                let remained_ptr = CAVE.lock().unwrap().remove(&tid);
                let (mut combiners, mut sorted_max_key): (BTreeMap<K, Option<C>>, BTreeMap<K, usize>) = match remained_ptr {
                    Some((c_ptr, s_ptr)) => (
                        *unsafe { Box::from_raw(c_ptr as *mut u8 as *mut BTreeMap<K, Option<C>>) },
                        *unsafe { Box::from_raw(s_ptr as *mut u8 as *mut BTreeMap<K, usize>) }
                    ),
                    None => (BTreeMap::new(), BTreeMap::new()),
                };
                let buckets_enc = input.get_enc_data::<Vec<Vec<(KE, CE)>>>();
                let lower = input.get_lower();
                let upper = input.get_upper();
                let block_size = input.get_block_size();
                //println!("cur mem before decryption: {:?}", crate::ALLOCATOR.lock().get_memory_usage());
                let now = Instant::now(); 
                let upper_bound = buckets_enc.iter().map(|sub_part| sub_part.len()).collect::<Vec<_>>();
                let mut block = Vec::new();
                if sorted_max_key.is_empty() {
                    block = buckets_enc.iter()
                        .enumerate()
                        .map(|(idx, sub_part)| {
                            let l = &mut lower[idx];
                            let u = &mut upper[idx];
                            let ub = upper_bound[idx];
                            match *l < ub {
                                true => {
                                    let data_enc = sub_part[*l..*u].to_vec();
                                    *l += 1;
                                    *u += 1;
                                    self.batch_decrypt(data_enc)
                                },
                                false => Vec::new(),
                            }
                        }).collect::<Vec<_>>();
                    sorted_max_key = block.iter()
                        .enumerate()
                        .filter(|(idx, sub_part)| sub_part.last().is_some())
                        .map(|(idx, sub_part)| (sub_part.last().unwrap().0.clone(), idx))
                        .collect::<BTreeMap<_, _>>();
                } else {
                    block.resize(lower.len(), Vec::new());
                }

                let mut cur_size = block.get_aprox_size();
                while cur_size < block_size {
                    let entry = match sorted_max_key.first_entry() {
                        Some(entry) => entry,
                        None => break,
                    };
                    let idx = *entry.get();
                    entry.remove_entry();
                    if lower[idx] >= upper_bound[idx] {
                        continue;
                    }
                    let mut inc_block = self.batch_decrypt(buckets_enc[idx][lower[idx]..upper[idx]].to_vec());
                    cur_size += inc_block.get_aprox_size();
                    block[idx].append(&mut inc_block); 
                    sorted_max_key.insert(block[idx].last().unwrap().0.clone(), idx);
                    lower[idx] += 1;
                    upper[idx] += 1;
                }
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                println!("cur mem after decryption: {:?}, in enclave decrypt: {:?} s", crate::ALLOCATOR.lock().get_memory_usage(), dur);
                for (k, c) in block.into_iter().flatten() {
                    if let Some(old_c) = combiners.get_mut(&k) {
                        let old = old_c.take().unwrap();
                        let input = ((old, c),);
                        let output = aggregator.merge_combiners.call(input);
                        *old_c = Some(output);
                    } else {
                        combiners.insert(k, Some(c));
                    }
                }

                if let Some(min_max_k) = sorted_max_key.first_entry() {
                    let remained_c = combiners.split_off(min_max_k.key());
                    let remained_s = sorted_max_key;
                    //Temporary stored for next computation
                    CAVE.lock().unwrap().insert(tid, 
                        (Box::into_raw(Box::new(remained_c)) as *mut u8 as usize, 
                        Box::into_raw(Box::new(remained_s)) as *mut u8 as usize)
                    );
                }

                println!("cur mem after shuffle read: {:?}", crate::ALLOCATOR.lock().get_memory_usage());
                let result = combiners.into_iter().map(|(k, v)| (k, v.unwrap())).collect::<Vec<Self::Item>>();
                let result_ptr = Box::into_raw(Box::new(result)) as *mut u8; 
                result_ptr
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
        
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<(K, C)>) };
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
