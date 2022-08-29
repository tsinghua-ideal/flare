use std::collections::BTreeMap;
use std::hash::Hash;

use deepsize::DeepSizeOf;

use crate::aggregator::Aggregator;
use crate::dependency::ShuffleDependency;
use crate::op::*;
use itertools::Itertools;

pub struct Shuffled<K, V, C>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
{
    vals: Arc<OpVals>,
    next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    parent: Arc<dyn Op<Item = (K, V)>>,
    aggregator: Arc<Aggregator<K, V, C>>,
    part: Box<dyn Partitioner>,
    cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<(K, C)>>>>>,
}

impl<K, V, C> Clone for Shuffled<K, V, C> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
{
    fn clone(&self) -> Self {
        Shuffled {
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            part: self.part.clone(),
            cache_space: self.cache_space.clone(),
        }
    }
}

impl<K, V, C> Shuffled<K, V, C> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
{
    #[track_caller]
    pub(crate) fn new(
        parent: Arc<dyn Op<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let ctx = parent.get_context();
        let mut vals = OpVals::new(ctx, part.get_num_of_partitions());
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
            cache_space: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn compute_inner(&self, tid: u64, input: Input) -> Vec<ItemE> {
        fn merge_core<K: Ord + Data, V: Data, C: Data>(buckets: Vec<Vec<(K, C)>>, aggregator: &Arc<Aggregator<K, V, C>>) -> Vec<(K, C)> {
            let mut iter = buckets.into_iter().kmerge_by(|a, b| a.0 < b.0);
            let mut combiners = match iter.next() {
                Some(first) => vec![first],
                None => vec![],
            };
    
            for (k, c) in iter {
                if k == combiners.last().unwrap().0 {
                    let pair = combiners.last_mut().unwrap();
                    pair.1 = (aggregator.merge_combiners)((std::mem::take(&mut pair.1), c));
                } else {
                    combiners.push((k, c));
                }
            }
            combiners
        }
        
        let mut acc = create_enc();
        let data_enc = input.get_enc_data::<Vec<Vec<Vec<ItemE>>>>();
        assert_eq!(data_enc.len(), MAX_THREAD + 1);
        let (is_para_enc, is_para_mer) = {
            crate::ALLOCATOR.reset_alloc_cnt();
            let sample_data = data_enc[0].iter().map(|bucket_enc| batch_decrypt::<(K, C)>(bucket_enc, true)).collect::<Vec<_>>();
            let sample_len = sample_data.iter().map(|v| v.len()).sum::<usize>();
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = (alloc_cnt as f64)/(sample_len as f64);
            let is_para_enc = alloc_cnt_ratio < PARA_THRESHOLD;
            println!("for decryption, alloc_cnt per len = {:?}", alloc_cnt_ratio);

            crate::ALLOCATOR.reset_alloc_cnt();
            let combiners = merge_core(sample_data, &self.aggregator);
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = (alloc_cnt as f64)/(sample_len as f64);
            let is_para_merge = alloc_cnt_ratio < PARA_THRESHOLD;
            println!("for merge, alloc_cnt per len = {:?}", alloc_cnt_ratio);
            combine_enc(&mut acc, batch_encrypt(&combiners, true));
            (is_para_enc, is_para_merge)
        };

        let mut handlers = Vec::with_capacity(MAX_THREAD);
        if !is_para_enc {
            let mut data = data_enc[1..MAX_THREAD+1].iter().map(|buckets_enc| {
                buckets_enc.iter().map(|bucket_enc| batch_decrypt::<(K, C)>(bucket_enc, true)).collect::<Vec<_>>()
            }).collect::<Vec<_>>();
            if !is_para_mer {
                let mut combiners = data.into_iter().map(|buckets| merge_core(buckets, &self.aggregator)).collect::<Vec<_>>();
                for i in 0..MAX_THREAD {
                    let combiners = combiners.pop().unwrap();
                    let builder = thread::Builder::new();
                    let handler = builder
                        .spawn(move || {
                            batch_encrypt(&combiners, true)
                        }).unwrap();
                    handlers.push(handler);
                }
            } else {
                for i in 0..MAX_THREAD {
                    let aggregator = self.aggregator.clone();
                    let buckets = data.pop().unwrap();
                    let builder = thread::Builder::new();
                    let handler = builder
                        .spawn(move || {
                            let combiners = merge_core(buckets, &aggregator);
                            batch_encrypt(&combiners, true)
                        }).unwrap();
                    handlers.push(handler);
                }
            }
        } else {
            if !is_para_mer {
                let mut handlers_pt = Vec::with_capacity(MAX_THREAD);
                for i in 1..MAX_THREAD + 1 {
                    let handler = thread::Builder::new()
                        .spawn(move || {
                            let buckets_enc = input.get_enc_data::<Vec<Vec<Vec<ItemE>>>>();
                            buckets_enc[i].iter().map(|bucket_enc| batch_decrypt::<(K, C)>(bucket_enc, true)).collect::<Vec<_>>()
                        }).unwrap();
                    handlers_pt.push(handler);
                }
                for handler in handlers_pt {
                    let aggregator = self.aggregator.clone();
                    let buckets = handler.join().unwrap();
                    let combiners = merge_core(buckets, &aggregator);
                    let handler = thread::Builder::new()
                        .spawn(move || {
                            batch_encrypt(&combiners, true)
                        }).unwrap();
                    handlers.push(handler);
                }
            } else {
                for i in 1..MAX_THREAD + 1 {
                    let aggregator = self.aggregator.clone();
                    let handler = thread::Builder::new()
                        .spawn(move || {
                            let buckets_enc = input.get_enc_data::<Vec<Vec<Vec<ItemE>>>>();
                            let buckets = buckets_enc[i].iter().map(|bucket_enc| batch_decrypt::<(K, C)>(bucket_enc, true)).collect::<Vec<_>>();
                            let combiners = merge_core(buckets, &aggregator);
                            batch_encrypt(&combiners, true)
                        }).unwrap();
                    handlers.push(handler);
                }
            }
        }

        for handler in handlers {
            combine_enc(&mut acc, handler.join().unwrap());
        }

        acc
    }
}

impl<K, V, C> OpBase for Shuffled<K, V, C> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data,
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

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 => self.free_res_enc(res_ptr, is_enc),
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

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
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

impl<K, V, C> Op for Shuffled<K, V, C>
where
    K: Data + Eq + Hash + Ord,
    V: Data, 
    C: Data, 
{
    type Item = (K, C);

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
            1 => {      //Shuffle write
                self.shuffle(call_seq, input, dep_info)
            }
            2 => {
                let result = self.compute_inner(call_seq.tid, input);
                to_ptr(result)
            }
            _ => panic!("Invalid is_shuffle"),
        }
    }
}
