use std::collections::BTreeMap;
use std::hash::Hash;

use crate::aggregator::Aggregator;
use crate::dependency::ShuffleDependency;
use crate::op::*;

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
        }
    }

    pub fn compute_inner(&self, tid: u64, input: Input) -> Vec<ItemE> {
        let buckets_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
        let mut combiners: BTreeMap<K, Option<C>> = BTreeMap::new();
        let mut sorted_max_key: BTreeMap<(K, usize), usize> = BTreeMap::new();
        
        let num_sub_part = buckets_enc.len();
        let mut lower = vec![0; num_sub_part];
        let mut upper = vec![1; num_sub_part];
        let upper_bound = buckets_enc
            .iter()
            .map(|sub_part| sub_part.len())
            .collect::<Vec<_>>();
        
        let mut res = create_enc();
        while lower
            .iter()
            .zip(upper_bound.iter())
            .filter(|(l, ub)| l < ub)
            .count()
            > 0
        {
            upper = upper
                .iter()
                .zip(upper_bound.iter())
                .map(|(l, ub)| std::cmp::min(*l, *ub))
                .collect::<Vec<_>>();
            let (res_bl, remained_c) = self.compute_inner_core(input.get_parallel(), buckets_enc, &mut lower, &mut upper, &upper_bound, combiners, &mut sorted_max_key);
            combine_enc(&mut res, res_bl);
            combiners = remained_c;
            lower = lower
                .iter()
                .zip(upper_bound.iter())
                .map(|(l, ub)| std::cmp::min(*l, *ub))
                .collect::<Vec<_>>();
        }
        res
    }
    pub fn compute_inner_core(&self, parallel_num: usize, buckets_enc: &Vec<Vec<ItemE>>, lower: &mut Vec<usize>, upper: &mut Vec<usize>, upper_bound: &Vec<usize>, mut combiners: BTreeMap<K, Option<C>>, sorted_max_key: &mut BTreeMap<(K, usize), usize>) -> (Vec<ItemE>, BTreeMap<K, Option<C>>) {
        fn combine<K, V, C>(combiners: &mut BTreeMap<K, Option<C>>, aggregator: &Arc<Aggregator<K, V, C>>, block: Vec<(K, C)>)
        where
            K: Data + Eq + Hash + Ord,
            V: Data, 
            C: Data,
        {
            for (k, c) in block.into_iter() {
                if let Some(old_c) = combiners.get_mut(&k) {
                    let old = old_c.take().unwrap();
                    let input = ((old, c),);
                    let output = aggregator.merge_combiners.call(input);
                    *old_c = Some(output);
                } else {
                    combiners.insert(k, Some(c));
                }
            }
        }
        let aggregator = self.aggregator.clone(); 
        if sorted_max_key.is_empty() {
            *sorted_max_key = buckets_enc.iter()
                .enumerate()
                .filter_map(|(idx, sub_part)| {
                    let l = &mut lower[idx];
                    let u = &mut upper[idx];
                    let ub = upper_bound[idx];
                    match *l < ub {
                        true => {
                            let block = batch_decrypt::<(K, C)>(&sub_part[*l..*u], true);
                            *l += 1;
                            *u += 1;
                            let r = ((block.last().unwrap().0.clone(), idx), idx);
                            combine(&mut combiners, &aggregator, block);
                            Some(r)
                        },
                        false => None,
                    }
                }).collect::<BTreeMap<_, _>>();
        }

        let mut cur_memory = crate::ALLOCATOR.get_memory_usage().1;
        let mut cur_len = 0;
        while cur_memory < CACHE_LIMIT/parallel_num || cur_len == 0 {
            let entry = match sorted_max_key.first_entry() {
                Some(entry) => entry,
                None => break,
            };
            let idx = *entry.get();
            entry.remove_entry();
            if lower[idx] >= upper_bound[idx] {
                continue;
            }
            let block = batch_decrypt::<(K, C)>(&buckets_enc[idx][lower[idx]..upper[idx]], true);
            sorted_max_key.insert((block.last().unwrap().0.clone(), idx), idx);
            combine(&mut combiners, &aggregator, block);
            lower[idx] += 1;
            upper[idx] += 1;
            cur_memory = crate::ALLOCATOR.get_memory_usage().1;
            cur_len += 1;
        }

        let remained_c = if lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
            let min_max_k = sorted_max_key.first_entry().unwrap();
            combiners.split_off(&min_max_k.key().0)
        } else {
            BTreeMap::new()
        };

        let result = combiners.into_iter().map(|(k, v)| (k, v.unwrap())).collect::<Vec<_>>();
        (batch_encrypt(&result, true), remained_c)
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

    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {
                self.narrow(call_seq, input, dep_info)
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

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_doublet();
            return self.get_and_remove_cached_data(key);
        }

        let len = input.get_enc_data::<Vec<ItemE>>().len();
        let res_iter = Box::new((0..len).map(move|i| {
            let data = input.get_enc_data::<Vec<ItemE>>();
            Box::new(ser_decrypt::<Vec<Self::Item>>(&data[i].clone()).into_iter()) as Box<dyn Iterator<Item = _>>
        }));

        let key = call_seq.get_caching_doublet();
        if need_cache && CACHE.get(key).is_none() {
            return self.set_cached_data(
                call_seq,
                res_iter,
                is_caching_final_rdd,
            )
        }
        res_iter
    }
}
