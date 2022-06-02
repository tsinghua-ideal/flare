use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use crate::aggregator::Aggregator;
use crate::dependency::{
    NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::op::*;
use crate::partitioner::HashPartitioner;

#[derive(Clone)]
pub struct CoGrouped<K, V, W> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
{
    pub is_for_join: bool,
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    pub(crate) op0: Arc<dyn Op<Item = (K, V)>>,
    pub(crate) op1: Arc<dyn Op<Item = (K, W)>>,
    pub(crate) part: Box<dyn Partitioner>,
}

impl<K, V, W> CoGrouped<K, V, W> 
where
K: Data + Eq + Hash + Ord,
V: Data,
W: Data,
{
    #[track_caller]
    pub fn new(op0: Arc<dyn Op<Item = (K, V)>>,
               op1: Arc<dyn Op<Item = (K, W)>>,
               part: Box<dyn Partitioner>) -> Self 
    {
        let context = op1.get_context();
        let mut vals = OpVals::new(context.clone(), part.get_num_of_partitions());
        let mut deps = Vec::new();
        let cur_id = vals.id;
        let op0_id = op0.get_op_id();
        let op1_id = op1.get_op_id();
             
        if op0
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(op0_id, cur_id)) as Arc<dyn NarrowDependencyTrait>,
            ));
            op0.get_next_deps().write().unwrap().insert(
                (op0_id, cur_id),
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(op0_id, cur_id))
                )
            );
        } else {
            let aggr = Arc::new(Aggregator::<K, V, _>::default());
            let dep = Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                    0,
                    op0_id,
                    cur_id,
                )) as Arc<dyn ShuffleDependencyTrait>,
            );
            deps.push(dep.clone());
            op0.get_next_deps().write().unwrap().insert(
                (op0_id, cur_id),
                dep,
            );
        }

        if op1
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(op1_id, cur_id)) as Arc<dyn NarrowDependencyTrait>,
            ));
            op1.get_next_deps().write().unwrap().insert(
                (op1_id, cur_id),
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(op1_id, cur_id))
                )
            ); 
        } else {
            let aggr = Arc::new(Aggregator::<K, W, _>::default());
            let dep = Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                    1,
                    op1_id,
                    cur_id,
                )) as Arc<dyn ShuffleDependencyTrait>,
            );
            deps.push(dep.clone());
            op1.get_next_deps().write().unwrap().insert(
                (op1_id, cur_id),
                dep,
            );
        }
        
        vals.deps = deps;
        let vals = Arc::new(vals);
        CoGrouped {
            is_for_join: false,
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            op0,
            op1,
            part,
        }
    }

    pub fn compute_inner(&self, tid: u64, input: Input) -> Vec<ItemE> {
        //TODO need revision if fe & fd of group_by is passed 
        let data_enc = input.get_enc_data::<(
            Vec<Vec<ItemE>>, 
            Vec<Vec<ItemE>>, 
            Vec<Vec<ItemE>>, 
            Vec<Vec<ItemE>>
        )>();
        let mut agg: BTreeMap<K, (Vec<V>, Vec<W>)> = BTreeMap::new();
        let mut sorted_max_key: BTreeMap<(K, usize), usize> = BTreeMap::new();

        let num_sub_part = data_enc.0.len() + data_enc.1.len() + data_enc.2.len() + data_enc.3.len();
        let mut lower = vec![0; num_sub_part];
        let mut upper = vec![1; num_sub_part];
        let upper_bound = data_enc.0.iter().map(|x| x.len())
            .chain(data_enc.1.iter().map(|x| x.len()))
            .chain(data_enc.2.iter().map(|x| x.len()))
            .chain(data_enc.3.iter().map(|x| x.len()))
            .collect::<Vec<_>>();
        assert_eq!(upper_bound.len(), num_sub_part);
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
            let (res_bl, remained_a) = self.compute_inner_core(input.get_parallel(), data_enc, &mut lower, &mut upper, &upper_bound, agg, &mut sorted_max_key);
            combine_enc(&mut res, res_bl);
            agg = remained_a;
            lower = lower
                .iter()
                .zip(upper_bound.iter())
                .map(|(l, ub)| std::cmp::min(*l, *ub))
                .collect::<Vec<_>>();
        }
        res
    }

    pub fn compute_inner_core(&self, parallel_num: usize, data_enc: &(Vec<Vec<ItemE>>, Vec<Vec<ItemE>>, Vec<Vec<ItemE>>, Vec<Vec<ItemE>>), lower: &mut Vec<usize>, upper: &mut Vec<usize>, upper_bound: &Vec<usize>, mut agg: BTreeMap<K, (Vec<V>, Vec<W>)>, sorted_max_key: &mut BTreeMap<(K, usize), usize>) -> (Vec<ItemE>, BTreeMap<K, (Vec<V>, Vec<W>)>) {
        let mut num_sub_part = vec![0, 0];
        let mut block = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        num_sub_part[0] += data_enc.0.len();
        block.0.resize(data_enc.0.len(), Vec::new());
        num_sub_part[0] += data_enc.1.len();
        block.1.resize(data_enc.1.len(), Vec::new());
        num_sub_part[1] += data_enc.2.len();
        block.2.resize(data_enc.2.len(), Vec::new());
        num_sub_part[1] += data_enc.3.len();
        block.3.resize(data_enc.3.len(), Vec::new());
        let deps = self.get_deps();

        if sorted_max_key.is_empty() {
            for idx in 0..(num_sub_part[0] + num_sub_part[1]) {  //init
                if lower[idx] >= upper_bound[idx] {
                    continue;
                }
                get_block(&deps, idx, &num_sub_part,
                    lower, upper, data_enc, &mut block, sorted_max_key
                );
                lower[idx] += 1;
                upper[idx] += 1;
            }
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
            get_block(&deps, idx, &num_sub_part,
                lower, upper, data_enc, &mut block, sorted_max_key
            );
            lower[idx] += 1;
            upper[idx] += 1;
            cur_memory = crate::ALLOCATOR.get_memory_usage().1;
            cur_len += 1;
        }

        let (b0, b1, b2, b3) = block;
        for i in b0.into_iter().flatten() { 
            let (k, v) = i;
            agg.entry(k)
                .or_insert_with(|| (Vec::new(), Vec::new())).0
                .push(v);
        }
        for (k, c) in b1.into_iter().flatten() { 
            let temp = agg.entry(k)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            for v in c {
                temp.0.push(v);
            }
        }
        for i in b2.into_iter().flatten() {
            let (k, w) = i;
            agg.entry(k)
                .or_insert_with(|| (Vec::new(), Vec::new())).1
                .push(w);
        }
        for (k, c) in b3.into_iter().flatten() { 
            let temp = agg.entry(k)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            for w in c {
                temp.1.push(w);
            }
        }

        let remained_a = if lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
            let min_max_k = sorted_max_key.first_entry().unwrap();
            agg.split_off(&min_max_k.key().0)
        } else {
            BTreeMap::new()
        };

        //block reshape
        let res = if self.is_for_join {
            let mut len = 0;
            let agg = agg.into_iter()
                .filter(|(k, (v, w))| v.len() != 0 && w.len() != 0)
                .collect::<Vec<_>>()
                .split_inclusive(|(k, (v, w))| {
                    len += v.get_aprox_size() * w.get_aprox_size();
                    let res = len > MAX_ENC_BL * MAX_ENC_BL;
                    len = (!res as usize) * len;
                    res
                }).flat_map(|x| {
                    let mut x = x.to_vec();
                    let mut y = x.drain_filter(|(k, (v, w))| v.len() * w.len() > MAX_ENC_BL * 128)
                        .flat_map(|(k, (v, w))| {
                            let vlen = v.len();
                            let wlen = w.len();
                            {
                                if vlen > wlen {
                                    let chunk_size = (MAX_ENC_BL*128-1)/wlen+1;
                                    let chunk_num =  (vlen-1)/chunk_size+1;
                                    let kk = vec![k; chunk_num].into_iter();
                                    let vv = v.chunks(chunk_size).map(|x| x.to_vec()).collect::<Vec<_>>().into_iter();
                                    let ww = vec![w; chunk_num].into_iter();
                                    kk.zip(vv.zip(ww))
                                } else {
                                    let chunk_size = (MAX_ENC_BL*128-1)/vlen+1;
                                    let chunk_num =  (wlen-1)/chunk_size+1;
                                    let kk = vec![k; chunk_num].into_iter();
                                    let ww = w.chunks(chunk_size).map(|x| x.to_vec()).collect::<Vec<_>>().into_iter();
                                    let vv = vec![v; chunk_num].into_iter();
                                    kk.zip(vv.zip(ww))
                                }
                            }
                        }).map(|x| vec![x])
                        .collect::<Vec<_>>();
                    y.push(x);
                    y
                }).collect::<Vec<_>>();
            agg
        } else {
            let mut len = 0;
            let agg = agg.into_iter()
                .filter(|(k, (v, w))| v.len() != 0 && w.len() != 0)
                .collect::<Vec<_>>()
                .split_inclusive(|(k, (v, w))| {
                    len += v.len() * w.len();
                    let res = len > MAX_ENC_BL;
                    len = (!res as usize) * len;
                    res
                }).map(|x| x.to_vec())
                .collect::<Vec<_>>();
            agg
        };

        let mut res_enc = create_enc();
        for res_bl in res {
            let block_enc = batch_encrypt(&res_bl, true);
            combine_enc(&mut res_enc, block_enc);
        }

        (res_enc, remained_a)
    }
}

impl<K, V, W> OpBase for CoGrouped<K, V, W> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
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
    
    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        let part = self.part.clone() as Box<dyn Partitioner>;
        Some(part)
    }

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }
    
    fn iterator_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8{
        
		self.compute_start(call_seq, input, dep_info)
    }
    
    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = (K, (Vec<V>, Vec<W>))>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (K, (Vec<V>, Vec<W>))>;
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

impl<K, V, W> Op for CoGrouped<K, V, W>
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
{
    type Item = (K, (Vec<V>, Vec<W>));  
    
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    
    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start(&self, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {       //narrow
                self.narrow(call_seq, input, dep_info)
            },
            1 => {       //shuffle write
                self.shuffle(call_seq, input, dep_info)
            },
            2 => {       //shuffle read
                let res = self.compute_inner(call_seq.tid, input);
                to_ptr(res)
            }
            _ => panic!("Invalid is_shuffle")
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

fn get_block<K, V, W>(
    deps: &Vec<Dependency>, 
    idx: usize,
    num_sub_part: &Vec<usize>,
    lower: &Vec<usize>,
    upper: &Vec<usize>,
    data_enc: &(
        Vec<Vec<ItemE>>, 
        Vec<Vec<ItemE>>, 
        Vec<Vec<ItemE>>, 
        Vec<Vec<ItemE>>
    ),
    block: &mut (
        Vec<Vec<(K, V)>>,
        Vec<Vec<(K, Vec<V>)>>,
        Vec<Vec<(K, W)>>,
        Vec<Vec<(K, Vec<W>)>>
    ),
    sorted_max_key: &mut BTreeMap<(K, usize), usize>,
) -> usize   //incremental size
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
{
    let mut inc_len = 0;
    if idx < num_sub_part[0] {
        match &deps[0] {
            Dependency::NarrowDependency(_nar) => {
                let sub_data_enc = &data_enc.0[idx];
                let mut block0 = batch_decrypt(&sub_data_enc[lower[idx]..upper[idx]], true);
                inc_len += 1;
                block.0[idx].append(&mut block0);
                sorted_max_key.insert((block.0[idx].last().unwrap().0.clone(), idx), idx);
            },
            Dependency::ShuffleDependency(shuf) => {
                //TODO need revision if fe & fd of group_by is passed 
                let s = shuf.downcast_ref::<ShuffleDependency<K, V, Vec<V>>>().unwrap();
                let sub_data_enc = &data_enc.1[idx]; 
                let mut block1 = batch_decrypt(&sub_data_enc[lower[idx]..upper[idx]], true);
                inc_len += 1;
                block.1[idx].append(&mut block1);
                sorted_max_key.insert((block.1[idx].last().unwrap().0.clone(), idx), idx);
            },
        };
    } else {
        let idx1 = idx - num_sub_part[0];
        match &deps[1] {
            Dependency::NarrowDependency(_nar) => {
                let sub_data_enc = &data_enc.2[idx1];
                let mut block2 = batch_decrypt(&sub_data_enc[lower[idx]..upper[idx]], true);
                inc_len += 1;
                block.2[idx1].append(&mut block2);
                sorted_max_key.insert((block.2[idx1].last().unwrap().0.clone(), idx), idx);
            },
            Dependency::ShuffleDependency(shuf) => {
                //TODO need revision if fe & fd of group_by is passed 
                let s = shuf.downcast_ref::<ShuffleDependency<K, W, Vec<W>>>().unwrap();
                let sub_data_enc = &data_enc.3[idx1]; 
                let mut block3 = batch_decrypt(&sub_data_enc[lower[idx]..upper[idx]], true);
                inc_len += 1;
                block.3[idx1].append(&mut block3);
                sorted_max_key.insert((block.3[idx1].last().unwrap().0.clone(), idx), idx);
            },
        };
    }
    inc_len
}