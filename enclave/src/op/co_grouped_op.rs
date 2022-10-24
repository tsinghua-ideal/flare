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
use deepsize::DeepSizeOf;
use itertools::Itertools;

#[derive(Clone)]
pub struct CoGrouped<K, V, W> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
{
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    pub(crate) op0: Arc<dyn Op<Item = (K, V)>>,
    pub(crate) op1: Arc<dyn Op<Item = (K, W)>>,
    pub(crate) part: Box<dyn Partitioner>,
    pub(crate) cache_space: Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<(K, (Vec<V>, Vec<W>))>>, Vec<Vec<bool>>)>>>
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
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            op0,
            op1,
            part,
            cache_space: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn compute_inner(&self, tid: u64, input: Input) -> (Vec<ItemE>, Vec<ItemE>) {
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let mut res = create_enc();
        let mut part_cnt = 0u64;

        let mut max_a_k = None;
        let mut max_a_cnt: (u64, u64) = (0, 0);  //(Ta, Tb)
        let mut max_b_k = None;
        let mut max_b_cnt: (u64, u64) = (0, 0);  //(Ta, Tb)
        let mut first_k = None;
        let mut first_cnt = (0, 0); // (Ta, Tb)
        let mut last_k = None;
        let mut last_cnt = (0, 0);  // (Ta, Tb)
        for sub_part in data_enc {
            let sub_part: Vec<(K, (Option<V>, Option<W>))> = ser_decrypt(&sub_part.clone());
            part_cnt += sub_part.len() as u64;
            for (k, v) in sub_part {
                if last_k.as_ref().map_or(false, |lk| lk == &k) {
                    last_cnt.0 += v.0.is_some() as u64;
                    last_cnt.1 += v.1.is_some() as u64;
                } else {
                    if max_a_cnt.0 < last_cnt.0 {
                        max_a_k = last_k.clone();
                        max_a_cnt = last_cnt;
                    } 
                    if max_b_cnt.1 < last_cnt.1 {
                        max_b_k = last_k.clone();
                        max_b_cnt = last_cnt;
                    }
                    if first_k.is_none() {
                        first_k = last_k;
                        first_cnt = last_cnt;
                    }
                    last_k = Some(k);
                    last_cnt = (v.0.is_some() as u64, v.1.is_some() as u64);
                }
            }
        }       
        let pairs = vec![(first_k, first_cnt), (last_k, last_cnt), (max_a_k, max_a_cnt), (max_b_k, max_b_cnt)];
        merge_enc(&mut res, &ser_encrypt(&pairs));
        let mut cnts = batch_encrypt(&vec![max_a_cnt.0, max_b_cnt.1], true);
        assert_eq!(cnts.len(), 1);
        merge_enc(&mut cnts, &part_cnt.to_le_bytes().to_vec());
        (res, cnts)
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

    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 | 28 | 29 => self.free_res_enc(data, marks, is_enc),
            1 => {
                assert_eq!(marks as usize, 0usize);
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(data, is_enc);
            },
            20 | 21 | 22 | 23 | 24 | 25 | 26 | 27 => {
                assert_eq!(marks as usize, 0usize);
                crate::ALLOCATOR.set_switch(true);
                let res = unsafe { Box::from_raw(data as *mut Vec<Vec<ItemE>>) };
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
    
    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        
		self.compute_start(call_seq, input, dep_info)
    }
    
    fn randomize_in_place(&self, input: *mut u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn etake(&self, input: *mut u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
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

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<Self::Item>>, Vec<Vec<bool>>)>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        let max_value = ((None, Default::default()), false);
        let cmp_f = |a: &((Option<K>, (Option<V>, Option<W>)), bool), b: &((Option<K>, (Option<V>, Option<W>)), bool)| {
            if a.0.0.is_some() && b.0.0.is_some() {
                (!a.1, (&a.0.0, a.0.1.1.is_some())).cmp(&(!b.1, (&b.0.0, b.0.1.1.is_some())))
            } else {
                a.0.0.cmp(&b.0.0).reverse()
            }
        };
        match dep_info.dep_type() {
            0 => {       //narrow
                self.narrow(call_seq, input, true)
            },
            1 => {       //shuffle write
                self.shuffle(call_seq, input, dep_info)
            },
            2 => {       //shuffle read
                let (data, cnt) = self.compute_inner(call_seq.tid, input);
                (to_ptr(data), to_ptr(cnt))
            },
            20 => {      //column sort, step 1 + step 2
                let data_enc_ref = input.get_enc_data::<(
                    (Vec<ItemE>, Vec<ItemE>), 
                    Vec<Vec<ItemE>>, 
                    (Vec<ItemE>, Vec<ItemE>), 
                    Vec<Vec<ItemE>>
                )>();
                // sub_parts[i][j] < sub_parts[i][j+1] in sub_parts[i]
                let mut data = Vec::new();
                let mut max_len = 0;

                // (Option<K>, V) -> (Option<K>, (Option<V>, Option<W>))
                for (sub_part_enc, sub_part_marks_enc) in data_enc_ref.0.0.iter().zip(data_enc_ref.0.1.iter()) {
                    //not sure about the type of the fed key, Option<K> or <K>
                    let mut sub_part: Vec<(K, V)> = ser_decrypt(&sub_part_enc.clone());
                    max_len = std::cmp::max(max_len, sub_part.len());
                    unimplemented!()
                }
                for part_enc in &data_enc_ref.1 {
                    let mut part = Vec::new();
                    let mut sub_part = Vec::new();
                    for block_enc in part_enc {
                        sub_part.append(&mut ser_decrypt::<Vec<((Option<K>, V), bool)>>(&block_enc.clone()).into_iter()
                            .map(|((k, v), m)| ((k, (Some(v), None::<W>)), m))
                            .collect::<Vec<_>>());
                        if sub_part.deep_size_of() > CACHE_LIMIT/input.get_parallel() {
                            max_len = std::cmp::max(max_len, sub_part.len());
                            part.push(sub_part);
                            sub_part = Vec::new();
                        } 
                    }
                    if !sub_part.is_empty() {
                        max_len = std::cmp::max(max_len, sub_part.len());
                        part.push(sub_part);
                    }
                    data.push(part);
                }

                // (Option<K>, W) -> (Option<K>, (Option<V>, Option<W>))
                for (sub_part_enc, sub_part_marks_enc) in data_enc_ref.2.0.iter().zip(data_enc_ref.2.1.iter()) {
                    let mut sub_part: Vec<(K, W)> = ser_decrypt(&sub_part_enc.clone());
                    max_len = std::cmp::max(max_len, sub_part.len());
                    unimplemented!();
                }

                for part_enc in &data_enc_ref.3 {
                    let mut part = Vec::new();
                    let mut sub_part = Vec::new();
                    for block_enc in part_enc {
                        sub_part.append(&mut ser_decrypt::<Vec<((Option<K>, W), bool)>>(&block_enc.clone()).into_iter()
                            .map(|((k, w), m)| ((k, (None::<V>, Some(w))), m))
                            .collect::<Vec<_>>());
                        if sub_part.deep_size_of() > CACHE_LIMIT/input.get_parallel() {
                            max_len = std::cmp::max(max_len, sub_part.len());
                            part.push(sub_part);
                            sub_part = Vec::new();
                        } 
                    }
                    if !sub_part.is_empty() {
                        max_len = std::cmp::max(max_len, sub_part.len());
                        part.push(sub_part);
                    }
                    data.push(part);
                }
                let op_id = self.get_op_id();
                if max_len == 0 {
                    assert!(CNT_PER_PARTITION.lock().unwrap().insert((op_id, call_seq.get_part_id()), 0).is_none());
                    (res_enc_to_ptr(Vec::<Vec<ItemE>>::new()), 0 as *mut u8)
                } else {
                    let mut sort_helper = SortHelper::new_with(data, max_len, max_value, true, cmp_f);
                    sort_helper.sort();
                    let (sorted_data, num_real_elem) = sort_helper.take();
                    assert!(CNT_PER_PARTITION.lock().unwrap().insert((op_id, call_seq.get_part_id()), num_real_elem).is_none());
                    let num_output_splits = self.number_of_splits();
                    let buckets_enc = column_sort_step_2(call_seq.tid, sorted_data, max_len, num_output_splits);
                    (to_ptr(buckets_enc), 0 as *mut u8)
                }
            },
            21 | 22 | 23 => {     //column sort, remaining steps
                let filter_f = |data: Vec<((Option<K>, (Option<V>, Option<W>)), bool)>| data.into_iter()
                    .filter(|((k, c), m)| *m && k.is_some())
                    .map(|((k, c), m)| (k.unwrap(), c))
                    .collect::<Vec<_>>();
                (combined_column_sort_step_4_6_8(call_seq.tid, input, dep_info, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits(), max_value, cmp_f, filter_f), 0 as *mut u8)
            },
            24 | 25 | 26 | 27 => {
                self.sort(call_seq, input, dep_info)
            },
            28 => { //aggregate again with the agg info from other servers
                let ser = call_seq.get_ser_captured_var().unwrap();
                let agg_data_ref = input.get_enc_data::<Vec<ItemE>>();
                let mut res = create_enc();
                let mut sup_data: Vec<(Option<K>, (u64, u64))> = batch_decrypt(ser, true);
                let mut agg_data: Vec<(Option<K>, (u64, u64))> = batch_decrypt(agg_data_ref, true);
                assert!(sup_data.len() == 4 && agg_data.len() == 4);
                if sup_data[1].0 == agg_data[0].0 {
                    agg_data[0].1.0 += sup_data[1].1.0;
                    agg_data[0].1.1 += sup_data[1].1.1;
                    if agg_data[0].0 == agg_data[1].0 {
                        agg_data[1] = agg_data[0].clone();
                    }
                    if agg_data[0].1.0 > agg_data[2].1.0 {
                        agg_data[2] = agg_data[0].clone();
                    } //update a maxw   
                    if agg_data[0].1.1 > agg_data[3].1.1 {
                        agg_data[3] = agg_data[0].clone();
                    } //update b max
                }
                if sup_data[2].1.0 > agg_data[2].1.0 {
                    agg_data[2] = sup_data[2].clone();
                }
                if sup_data[3].1.1 > agg_data[3].1.1 {
                    agg_data[3] = sup_data[3].clone();
                }
                merge_enc(&mut res, &ser_encrypt(&agg_data));
                let encrypted_cnt = batch_encrypt(&vec![agg_data[2].1.0, agg_data[3].1.1], true);
                (to_ptr(res), to_ptr(encrypted_cnt) as *mut u8)
            },
            29 => {
                fn co_group_by<K, V, W>(data_enc: &[ItemE], mut agg: Vec<(K, (Vec<V>, Vec<W>))>, mut marks: Vec<bool>, res_data: &mut Vec<ItemE>, res_marks: &mut Vec<ItemE>, common_mark: bool) -> (Vec<(K, (Vec<V>, Vec<W>))>, Vec<bool>) 
                where
                    K: Data + Eq + Hash + Ord,
                    V: Data,
                    W: Data,
                {
                    for sub_part in data_enc {
                        let sub_part: Vec<(K, (Option<V>, Option<W>))> = ser_decrypt(&sub_part.clone());
                        for (j, group) in sub_part.group_by(|a, b| a.0 == b.0).enumerate() {
                            let k = group[0].0.clone();
                            let mut vs = group.iter().filter_map(|(_, (v, _))| {
                                v.clone()
                            }).collect::<Vec<_>>();
                            let mut ws = group.iter().filter_map(|(_, (_, w))| {
                                w.clone()
                            }).collect::<Vec<_>>();
                            if j == 0 && !agg.is_empty() {
                                let (lk, (lv, lw)) = agg.last_mut().unwrap();
                                if *lk == k {
                                    lv.append(&mut vs);
                                    lw.append(&mut ws);
                                } else {
                                    agg.push((k, (vs, ws)));
                                    marks.push(common_mark);
                                }
                            } else {
                                agg.push((k, (vs, ws)));
                                marks.push(common_mark);
                            }
                        }
                        let last = agg.pop().unwrap();
                        let last_m = marks.pop().unwrap();
                        if !agg.is_empty() {
                            merge_enc(res_data, &ser_encrypt(&agg));
                            merge_enc(res_marks, &ser_encrypt(&marks));
                        }
                        agg = vec![last];
                        marks = vec![last_m];
                    }
                    (agg, marks)
                }

                let sup_data_enc = call_seq.get_ser_captured_var().unwrap();
                let mut res_data = create_enc();
                let mut res_marks = create_enc();
                //all groups except the last one are marked invalid
                let (agg, mut marks) = co_group_by::<K, V, W>(&sup_data_enc[..sup_data_enc.len()-2], Vec::new(), Vec::new(), &mut res_data, &mut res_marks, false);
                assert_eq!(agg.len(), marks.len());
                if let Some(m) = marks.last_mut() {
                    *m = true;
                }

                let data_enc = input.get_enc_data::<Vec<ItemE>>();
                let (agg, mut marks) = co_group_by(data_enc, agg, marks, &mut res_data, &mut res_marks, true);
                //invalid the last group of local partition (except for the last partition)
                if call_seq.get_part_id() < self.number_of_splits() - 1 {
                    if let Some(m) = marks.last_mut() {
                        *m = false;
                    }
                }

                if !agg.is_empty() {
                    merge_enc(&mut res_data, &ser_encrypt(&agg));
                    merge_enc(&mut res_marks, &ser_encrypt(&marks));
                }

                (to_ptr(res_data), to_ptr(res_marks))
            },
            _ => panic!("Invalid is_shuffle")
        }
    }
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let marks_ptr = input.marks;

        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();
        call_seq.should_filter.0 = true;

        if have_cache || (call_seq.get_cur_rdd_id() == call_seq.cache_meta.cached_rdd_id && data_ptr == 0) {
            assert_eq!(data_ptr as usize, 0 as usize);
            return self.get_and_remove_cached_data(call_seq);
        }
        
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let marks_enc = input.get_enc_marks::<Vec<ItemE>>();
        let res_iter = self.parallel_control(call_seq, data_enc, marks_enc);
        
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