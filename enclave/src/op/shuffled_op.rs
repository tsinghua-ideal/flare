use std::collections::BTreeMap;
use std::hash::Hash;

use crate::CNT_PER_PARTITION;
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
    cache_space: Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<(K, C)>>, Vec<Vec<bool>>)>>>,
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

    pub fn compute_inner(&self, tid: u64, input: Input) -> (Vec<ItemE>, Vec<ItemE>) {
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let mut res = create_enc();
        let aggregator = self.aggregator.clone(); 
        let mut part_cnt = 0u64;
        let encrypted_cnt = if aggregator.is_default {
            let mut max_k = None;
            let mut max_cnt: u64 = 0;
            let mut first_k = None;
            let mut first_cnt = 0;
            let mut last_k = None;
            let mut last_cnt = 0;
            for sub_part in data_enc {
                let sub_part: Vec<(K, V)> = ser_decrypt(&sub_part.clone());
                part_cnt += sub_part.len() as u64;
                for (k, v) in sub_part {
                    if last_k.as_ref().map_or(false, |lk| lk == &k) {
                        last_cnt += 1;
                    } else {
                        if max_cnt < last_cnt {
                            max_k = last_k.clone();
                            max_cnt = last_cnt;
                        }
                        if first_k.is_none() {
                            first_k = last_k;
                            first_cnt = last_cnt;
                        }
                        last_k = Some(k);
                        last_cnt = 1;
                    }
                }
            }
            let pairs = vec![(first_k, first_cnt), (last_k, last_cnt), (max_k, max_cnt)];
            merge_enc(&mut res, &ser_encrypt(&pairs));
            let mut cnts = batch_encrypt(&vec![max_cnt], true);
            assert_eq!(cnts.len(), 1);
            merge_enc(&mut cnts, &part_cnt.to_le_bytes().to_vec());
            cnts
        } else {
            //assume others are min, max, avg, add, etc.
            //they can be implemented obliviously
            let mut last_k = None;
            let mut last_c = Default::default();
            let mut last_kc: Option<((K, C), bool)> = None;
            for sub_part in data_enc {
                let sub_part = ser_decrypt::<Vec<(K, V)>>(&sub_part.clone());
                part_cnt += sub_part.len() as u64;
                let mut sub_part_kc: Vec<((K, C), bool)> = Vec::with_capacity(sub_part.len());
                if let Some(last_kc) = last_kc {
                    sub_part_kc.push(last_kc);
                }
                for (k, v) in sub_part {
                    if last_k.as_ref().map_or(false, |lk| lk == &k) {
                        last_c = (aggregator.merge_value)((last_c, v));
                        if let Some(last_kc) = sub_part_kc.last_mut() {
                            last_kc.1 = false;
                        }
                    } else {
                        last_k = Some(k.clone());
                        last_c = (aggregator.create_combiner)(v);
                    }
                    sub_part_kc.push(((k, last_c.clone()), true));
                }
                last_kc = sub_part_kc.pop();
                if !sub_part_kc.is_empty() {
                    let res_bl = ser_encrypt(&sub_part_kc);
                    merge_enc(&mut res, &res_bl);
                }
            }
            if last_kc.is_some() {
                let res_bl = ser_encrypt(&vec![last_kc.unwrap()]);
                merge_enc(&mut res, &res_bl);
            }
            let mut cnts = batch_encrypt(&vec![0u64], true);
            assert_eq!(cnts.len(), 1);
            merge_enc(&mut cnts, &part_cnt.to_le_bytes().to_vec());
            cnts

        };
        (res, encrypted_cnt)
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

    fn call_free_res_enc(&self, data: *mut u8, marks: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 2 | 28 | 29 => self.free_res_enc(data, marks, is_enc),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(data, is_enc);
            },
            20 => {
                unreachable!();
            }
            21 | 22 | 23 | 24 | 25 | 26 | 27 => {
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

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
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

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<Self::Item>>, Vec<Vec<bool>>)>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> (*mut u8, *mut u8) {
        match dep_info.dep_type() {
            0 => {
                self.narrow(call_seq, input, true)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, input, dep_info)
            }
            2 => {
                let (data, cnt) = self.compute_inner(call_seq.tid, input);
                (to_ptr(data), to_ptr(cnt))
            }
            20 => {
                let mut num_real_elem = 0;
                let data_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
                for part in data_enc {
                    for sub_part in part {
                        let sub_part: Vec<((Option<K>, V), bool)> = ser_decrypt(&sub_part.clone());
                        num_real_elem += sub_part.len();
                    }
                }
                assert!(CNT_PER_PARTITION.lock().unwrap().insert((self.get_op_id(), call_seq.get_part_id()), num_real_elem).is_none());
                (0 as *mut u8, 0 as *mut u8)
            }
            21 | 22 | 23 => {
                let max_value = ((None, Default::default()), false);
                let cmp_f = |a: &((Option<K>, V), bool), b: &((Option<K>, V), bool)| {
                    if a.0.0.is_some() && b.0.0.is_some() {
                        (!a.1, &a.0.0).cmp(&(!b.1, &b.0.0))
                    } else {
                        a.0.0.cmp(&b.0.0).reverse()
                    }
                };
                let filter_f = |data: Vec<((Option<K>, V), bool)>| data.into_iter()
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
                if self.aggregator.is_default {
                    let mut sup_data: Vec<(Option<K>, u64)> = batch_decrypt(ser, true);
                    let mut agg_data: Vec<(Option<K>, u64)> = batch_decrypt(agg_data_ref, true);
                    assert!(sup_data.len() == 3 && agg_data.len() == 3);
                    if sup_data[1].0 == agg_data[0].0 {
                        agg_data[0].1 += sup_data[1].1;
                        if agg_data[0].0 == agg_data[1].0 {
                            agg_data[1] = agg_data[0].clone();
                        }
                        if agg_data[0].1 > agg_data[2].1 {
                            agg_data[2] = agg_data[0].clone();
                        }
                    }
                    if sup_data[2].1 > agg_data[2].1 {
                        agg_data[2] = sup_data[2].clone();
                    }
                    merge_enc(&mut res, &ser_encrypt(&agg_data));
                    let encrypted_cnt = batch_encrypt(&vec![agg_data[2].1], true);
                    (to_ptr(res), to_ptr(encrypted_cnt))
                } else {
                    let mut sup_data: Vec<((K, C), bool)> = batch_decrypt(ser, true);
                    assert_eq!(sup_data.len(), 1);
                    let mut last_kc = sup_data.remove(0);
                    let aggregator = self.aggregator.clone(); 

                    for sub_part_enc in agg_data_ref {
                        let mut sub_part: Vec<((K, C), bool)> = vec![last_kc];
                        sub_part.append(&mut ser_decrypt(&sub_part_enc.clone()));
                        for j in 1..sub_part.len() {
                            if sub_part[j].1 == false {
                                sub_part.swap(j-1, j);
                            } else if sub_part[j].0.0 == sub_part[j-1].0.0 {
                                assert!(sub_part[j-1].1);
                                sub_part[j].0.1 = (aggregator.merge_combiners)((std::mem::take(&mut sub_part[j-1].0.1), std::mem::take(&mut sub_part[j].0.1)));
                                sub_part[j-1].1 = false;
                            }
                        }
                        last_kc = sub_part.pop().unwrap();
                        if !sub_part.is_empty() {
                            let res_bl = ser_encrypt(&sub_part);
                            merge_enc(&mut res, &res_bl);
                        }
                    }
                    // the last one will be transmit to other servers
                    assert!(last_kc.1);
                    let sub_part = vec![last_kc];
                    let res_bl = ser_encrypt(&sub_part);
                    merge_enc(&mut res, &res_bl);
                    let encrypted_cnt = batch_encrypt(&vec![0u64], true);
                    (to_ptr(res), to_ptr(encrypted_cnt))
                }
            },
            29 => {
                //specially for group by 
                fn group_by<K, V, C>(data_enc: &[ItemE], mut sub_part_kc: Vec<(K, C)>, mut marks: Vec<bool>, res_data: &mut Vec<ItemE>, res_marks: &mut Vec<ItemE>, aggregator: &Arc<Aggregator<K, V, C>>, common_mark: bool) -> (Vec<(K, C)>, Vec<bool>) 
                where
                    K: Data + Eq + Hash + Ord,
                    V: Data, 
                    C: Data, 
                {
                    for sub_part in data_enc {
                        let sub_part: Vec<(K, V)> = ser_decrypt(&sub_part.clone());
                        for (j, group) in sub_part.group_by(|x, y| x.0 == y.0).enumerate() {
                            let k = group[0].0.clone(); //the type is consistent with aggregate
                            let mut iter = group.iter();
                            let init_v = iter.next().unwrap().1.clone();
                            let c = iter.fold((aggregator.create_combiner)(init_v), |acc, v| (aggregator.merge_value)((acc, v.1.clone())));
                            if j == 0 && !sub_part_kc.is_empty() {
                                let (lk, lc) = sub_part_kc.last_mut().unwrap();
                                if *lk == k {
                                    *lc = (aggregator.merge_combiners)((std::mem::take(lc), c));
                                } else {
                                    sub_part_kc.push((k, c));
                                    marks.push(common_mark);
                                }
                            } else {
                                sub_part_kc.push((k, c));
                                marks.push(common_mark);
                            }
                        };
                        let last_kc = sub_part_kc.pop().unwrap();
                        let last_m = marks.pop().unwrap();
                        if !sub_part_kc.is_empty() {
                            merge_enc(res_data, &ser_encrypt(&sub_part_kc));
                            merge_enc(res_marks, &ser_encrypt(&marks));
                        }
                        sub_part_kc = vec![last_kc];
                        marks = vec![last_m];
                    }
                    (sub_part_kc, marks)
                }

                assert!(self.aggregator.is_default);
                let sup_data_enc = call_seq.get_ser_captured_var().unwrap();
                let mut res_data = create_enc();
                let mut res_marks = create_enc();
                //all groups except the last one are marked invalid
                let (sub_part_kc, mut marks) = group_by(&sup_data_enc[..sup_data_enc.len()-2], Vec::new(), Vec::new(), &mut res_data, &mut res_marks, &self.aggregator, false);
                assert_eq!(sub_part_kc.len(), marks.len());
                if let Some(m) = marks.last_mut() {
                    *m = true;
                }
                
                let data_enc = input.get_enc_data::<Vec<ItemE>>();
                let (sub_part_kc, mut marks) = group_by(data_enc, sub_part_kc, marks, &mut res_data, &mut res_marks, &self.aggregator, true);
                //invalid the last group of local partition (except for the last partition)
                if call_seq.get_part_id() < self.number_of_splits() - 1 {
                    if let Some(m) = marks.last_mut() {
                        *m = false;
                    }
                }

                if !sub_part_kc.is_empty() {
                    merge_enc(&mut res_data, &ser_encrypt(&sub_part_kc));
                    merge_enc(&mut res_marks, &ser_encrypt(&marks));
                }
                (to_ptr(res_data), to_ptr(res_marks))
            }
            _ => panic!("Invalid is_shuffle"),
        }
    }
    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> ResIter<Self::Item> {
        let data_ptr = input.data;
        let marks_ptr = input.marks;

        let have_cache = call_seq.have_cache();
        let need_cache = call_seq.need_cache();
        let is_caching_final_rdd = call_seq.is_caching_final_rdd();
        call_seq.should_filter.0 = self.aggregator.is_default;

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
