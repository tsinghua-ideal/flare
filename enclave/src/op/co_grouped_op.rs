use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use crate::CNT_PER_PARTITION;
use crate::aggregator::Aggregator;
use crate::dependency::{
    NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::op::*;
use crate::partitioner::HashPartitioner;

#[derive(Clone)]
pub struct CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
    FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
    pub is_for_join: bool,
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    pub(crate) op0: Arc<dyn OpE<Item = (K, V), ItemE = (KE, VE)>>,
    pub(crate) op1: Arc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
    pub(crate) part: Box<dyn Partitioner>,
    fe: FE,
    fd: FD,
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where
K: Data + Eq + Hash + Ord,
V: Data,
W: Data,
KE: Data + Eq + Hash + Ord,
VE: Data,
CE: Data,
WE: Data,
DE: Data,
FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
    #[track_caller]
    pub fn new(op0: Arc<dyn OpE<Item = (K, V), ItemE = (KE, VE)>>,
               op1: Arc<dyn OpE<Item = (K, W), ItemE = (KE, WE)>>,
               fe: FE,
               fd: FD,
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
            //TODO need revision if fe & fd of group_by is passed 
            let fe = op0.get_fe();
            let fe_wrapper = Box::new(move |v: Vec<(K, Vec<V>)>| {
                let (x, y): (Vec<K>, Vec<Vec<V>>) = v.into_iter().unzip();
                let mut y_padding = Vec::new();
                y_padding.resize_with(x.len(), Default::default);
                let (ct_x, _) = (fe)(x.into_iter()
                    .zip(y_padding.into_iter())
                    .collect::<Vec<_>>()
                );
                (ct_x, ser_encrypt(y))
            });

            let fd = op0.get_fd();
            let fd_wrapper = Box::new(move |v: (KE, Vec<u8>)| {
                let (x, y) = v;
                let y_padding: VE = Default::default();
                let (pt_x, _): (Vec<K>, Vec<V>) = (fd)((x, y_padding)).into_iter().unzip();
                let pt_y: Vec<Vec<V>> = ser_decrypt(y);
                pt_x.into_iter()
                    .zip(pt_y.into_iter())
                    .collect::<Vec<_>>()
            });

            let aggr = Arc::new(Aggregator::<K, V, _>::default());
            let dep = Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                    0,
                    op0_id,
                    cur_id,
                    fe_wrapper,
                    fd_wrapper,
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
            //TODO need revision if fe & fd of group_by is passed 
            let fe = op1.get_fe();
            let fe_wrapper = Box::new(move |v: Vec<(K, Vec<W>)>| {
                let (x, y): (Vec<K>, Vec<Vec<W>>) = v.into_iter().unzip();
                let mut y_padding = Vec::new();
                y_padding.resize_with(x.len(), Default::default);
                let (ct_x, _) = (fe)(x.into_iter()
                    .zip(y_padding.into_iter())
                    .collect::<Vec<_>>()
                );
                (ct_x, ser_encrypt(y))
            });

            let fd = op1.get_fd();
            let fd_wrapper = Box::new(move |v: (KE, Vec<u8>)| {
                let (x, y) = v;
                let y_padding: WE = Default::default();
                let (pt_x, _): (Vec<K>, Vec<W>) = (fd)((x, y_padding)).into_iter().unzip();
                let pt_y: Vec<Vec<W>> = ser_decrypt(y);
                pt_x.into_iter()
                    .zip(pt_y.into_iter())
                    .collect::<Vec<_>>()
            });

            let aggr = Arc::new(Aggregator::<K, W, _>::default());
            let dep = Dependency::ShuffleDependency(
                Arc::new(ShuffleDependency::new(
                    true,
                    aggr,
                    part.clone(),
                    1,
                    op1_id,
                    cur_id,
                    fe_wrapper,
                    fd_wrapper,
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
            fe,
            fd,
            part,
        }
    }

    pub fn compute_inner(&self, tid: u64, input: Input) -> Vec<(KE, (CE, DE))> {



        let mut res = create_enc();

        res
    }

    pub fn compute_inner_core(&self, parallel_num: usize, data_enc: &(Vec<Vec<(KE, VE)>>, Vec<Vec<(KE, Vec<u8>)>>, Vec<Vec<(KE, WE)>>, Vec<Vec<(KE, Vec<u8>)>>), lower: &mut Vec<usize>, upper: &mut Vec<usize>, upper_bound: &Vec<usize>, mut agg: BTreeMap<K, (Vec<V>, Vec<W>)>, sorted_max_key: &mut BTreeMap<(K, usize), usize>) -> (Vec<(KE, (CE, DE))>, BTreeMap<K, (Vec<V>, Vec<W>)>) {
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
        let op0 = self.op0.clone();
        let op1 = self.op1.clone();

        if sorted_max_key.is_empty() {
            for idx in 0..(num_sub_part[0] + num_sub_part[1]) {  //init
                if lower[idx] >= upper_bound[idx] {
                    continue;
                }
                get_block(&deps, &op0, &op1, idx, &num_sub_part,
                    lower, upper, data_enc, &mut block, sorted_max_key
                );
                lower[idx] += 1;
                upper[idx] += 1;
            }
        }

        let mut cur_memory = crate::ALLOCATOR.get_memory_usage().1;
        while cur_memory < CACHE_LIMIT/parallel_num {
            let entry = match sorted_max_key.first_entry() {
                Some(entry) => entry,
                None => break,
            };
            let idx = *entry.get();
            entry.remove_entry();
            if lower[idx] >= upper_bound[idx] {
                continue;
            }
            get_block(&deps, &op0, &op1, idx, &num_sub_part,
                lower, upper, data_enc, &mut block, sorted_max_key
            );
            lower[idx] += 1;
            upper[idx] += 1;
            cur_memory = crate::ALLOCATOR.get_memory_usage().1;
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
            let block_enc = self.batch_encrypt(res_bl);
            combine_enc(&mut res_enc, block_enc);
        }

        (res_enc, remained_a)
    }
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> OpBase for CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
    FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
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
            20 | 21 | 22 | 23 => {
                crate::ALLOCATOR.set_switch(true);
                let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<(KE, (CE, DE))>>) };
                drop(res);
                crate::ALLOCATOR.set_switch(false);
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

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> Op for CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD>
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>, 
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
            },
            20 => {      //column sort, step 1 + step 2
                //TODO need revision if fe & fd of group_by is passed 
                let data_enc = input.get_enc_data::<(
                    Vec<(KE, VE)>, 
                    Vec<Vec<(KE, Vec<u8>)>>, 
                    Vec<(KE, WE)>, 
                    Vec<Vec<(KE, Vec<u8>)>>
                )>();
                let deps = self.get_deps();
                let op0 = self.op0.clone();
                let op1 = self.op1.clone();
                // sub_parts[i][j] < sub_parts[i][j+1] in sub_parts[i]
                let mut sub_parts = Vec::new();
                let mut num_prev_part = 0;

                let mut max_len = 0;
                let mut max_key = Default::default();

                match &deps[0] {
                    Dependency::NarrowDependency(_nar) => {
                        for sub_part in &data_enc.0 {
                            let mut sub_part = op0.batch_decrypt(vec![sub_part.clone()]);
                            max_len = std::cmp::max(max_len, sub_part.len());
                            unimplemented!();
                        }
                    },
                    Dependency::ShuffleDependency(shuf) => {
                        //TODO need revision if fe & fd of group_by is passed 
                        let s = shuf.downcast_ref::<ShuffleDependency<K, V, Vec<V>, KE, Vec<u8>>>().unwrap();
                        for part in &data_enc.1 {
                            sub_parts.push(Vec::new());
                            for sub_part in part {
                                let sub_part = (s.fd)(sub_part.clone());
                                max_len = std::cmp::max(max_len, sub_part.len());
                                max_key = std::cmp::max(max_key, sub_part.last().unwrap().0.clone());
                                let new_sub_part = sub_part.into_iter()
                                    .map(|(k, v)| (k, (v, Vec::<W>::new())))
                                    .collect::<Vec<_>>();
                                let new_sub_part_enc = (self.fe)(new_sub_part);
                                crate::ALLOCATOR.set_switch(true);
                                sub_parts[num_prev_part].push(new_sub_part_enc.clone());
                                crate::ALLOCATOR.set_switch(false);
                            }
                            num_prev_part += 1;
                        }
                    },
                };

                match &deps[1] {
                    Dependency::NarrowDependency(_nar) => {
                        for sub_part in &data_enc.2 {
                            let mut sub_part = op1.batch_decrypt(vec![sub_part.clone()]);
                            max_len = std::cmp::max(max_len, sub_part.len());
                            unimplemented!();
                        }
                    },
                    Dependency::ShuffleDependency(shuf) => {
                        //TODO need revision if fe & fd of group_by is passed 
                        let s = shuf.downcast_ref::<ShuffleDependency<K, W, Vec<W>, KE, Vec<u8>>>().unwrap();
                        for part in &data_enc.3 {
                            sub_parts.push(Vec::new());
                            for sub_part in part {
                                let sub_part = (s.fd)(sub_part.clone());
                                max_len = std::cmp::max(max_len, sub_part.len());
                                max_key = std::cmp::max(max_key, sub_part.last().unwrap().0.clone());
                                let new_sub_part = sub_part.into_iter()
                                    .map(|(k, w)| (k, (Vec::<V>::new(), w)))
                                    .collect::<Vec<_>>();
                                let new_sub_part_enc = (self.fe)(new_sub_part);
                                crate::ALLOCATOR.set_switch(true);
                                sub_parts[num_prev_part].push(new_sub_part_enc.clone());
                                crate::ALLOCATOR.set_switch(false);
                            }
                            num_prev_part += 1;
                        }
                    },
                };

                let sort_helper = SortHelper::new_with(sub_parts, max_len, max_key, true, self.get_fe(), self.get_fd());
                sort_helper.sort();
                let (sub_parts, num_real_elem) = sort_helper.take();
                CNT_PER_PARTITION.lock().unwrap().insert(self.get_op_id(), num_real_elem);
                let num_output_splits = self.number_of_splits();
                let buckets_enc = column_sort_step_2(sub_parts, max_len, num_output_splits, self.get_fe(), self.get_fd());
                to_ptr(buckets_enc)
            },
            21 | 22 | 23 => {     //column sort, remaining steps
                let data_enc_ref = input.get_enc_data::<Vec<Vec<(KE, (CE, DE))>>>();
                let mut max_len = 0;
                let mut max_key = Default::default();
                let mut data_enc = vec![Vec::new(); data_enc_ref.len()];
                for (i, part) in data_enc_ref.iter().enumerate() {
                    let last_j = part.len() - 1;
                    for (j, sub_part_enc) in part.iter().enumerate() {
                        if j == 0 {
                            //we can derive max_len from the first sub partition in each partition
                            let sub_part = (self.fd)(sub_part_enc.clone());
                            max_len = std::cmp::max(max_len, sub_part.len());

                        } else if j == last_j {
                            //we can derive max_key from the last sub partition in each partition
                            let sub_part = (self.fd)(sub_part_enc.clone());
                            sub_part.last().map(|x| max_key = std::cmp::max(max_key, x.0.clone()));
                        } 
                        crate::ALLOCATOR.set_switch(true);
                        data_enc[i].push(sub_part_enc.clone());
                        crate::ALLOCATOR.set_switch(false);
                    }
                }
                let sort_helper = SortHelper::new_with(data_enc, max_len, max_key, true, self.get_fe(), self.get_fd());
                sort_helper.sort();
                let (sub_parts, num_real_elem) = sort_helper.take();
                let cnt_per_partition = *CNT_PER_PARTITION.lock().unwrap().get(&self.get_op_id()).unwrap();
                let num_output_splits = self.number_of_splits();
                let buckets_enc = match dep_info.dep_type() {
                    21 => column_sort_step_4_6_8(sub_parts, cnt_per_partition, max_len, num_real_elem, num_output_splits, self.get_fe(), self.get_fd()),
                    22 => column_sort_step_4_6_8(sub_parts, cnt_per_partition, max_len, num_real_elem, 2, self.get_fe(), self.get_fd()),
                    23 => {
                        CNT_PER_PARTITION.lock().unwrap().remove(&self.get_op_id()).unwrap();
                        column_sort_step_4_6_8(sub_parts, cnt_per_partition, max_len, num_real_elem, 2, self.get_fe(), self.get_fd())
                    },
                    _ => unreachable!(),
                };
                to_ptr(buckets_enc)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();
        let fd = self.get_fd();

        if have_cache {
            assert_eq!(data_ptr as usize, 0 as usize);
            let key = call_seq.get_cached_doublet();
            return self.get_and_remove_cached_data(key);
        }
        
        let len = input.get_enc_data::<Vec<(KE, (CE, DE))>>().len();
        let res_iter = Box::new((0..len).map(move|i| {
            let data = input.get_enc_data::<Vec<(KE, (CE, DE))>>();
            Box::new((fd)(data[i].clone()).into_iter()) as Box<dyn Iterator<Item = _>>
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

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> OpE for CoGrouped<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash + Ord,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>,
{
    type ItemE = (KE, (CE, DE));
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