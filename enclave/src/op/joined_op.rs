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
pub struct Joined<K, V, W> 
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
    pub(crate) cache_space: Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<(K, (V, W))>>, Vec<Vec<bool>>)>>>
}

impl<K, V, W> Joined<K, V, W> 
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
        Joined {
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
        println!("max_a = {:?}, max_b = {:?}", max_a_cnt.0, max_b_cnt.1);
        let mut cnts = batch_encrypt(&vec![max_a_cnt.0, max_b_cnt.1], true);
        assert_eq!(cnts.len(), 1);
        merge_enc(&mut cnts, &part_cnt.to_le_bytes().to_vec());
        (res, cnts)
    }
}

impl<K, V, W> OpBase for Joined<K, V, W> 
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
        if id == TypeId::of::<dyn Op<Item = (K, (V, W))>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (K, (V, W))>;
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

impl<K, V, W> Op for Joined<K, V, W>
where 
    K: Data + Eq + Hash + Ord,
    V: Data,
    W: Data,
{
    type Item = (K, (V, W));  
    
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
                (a.1, (&a.0.0, a.0.1.1.is_some())).cmp(&(b.1, (&b.0.0, b.0.1.1.is_some())))
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
                fn split_with_interval<T, F>(mut data: Vec<T>, cache_limit: usize, cmp_f: F) -> (Vec<Vec<T>>, usize) 
                where
                    T: Data,
                    F: FnMut(&T, &T) -> Ordering + Clone,
                {
                    let mut sub_parts = Vec::new();
                    let mut max_len = 0;
                    let mut sub_part = Vec::new();
                    while !data.is_empty() {
                        let b = data.len().saturating_sub(MAX_ENC_BL);
                        let mut block = data.split_off(b);
                        sub_part.append(&mut block);
                        if sub_part.deep_size_of() > cache_limit {
                            sub_part.sort_unstable_by(cmp_f.clone());
                            max_len = std::cmp::max(max_len, sub_part.len());
                            sub_parts.push(sub_part);
                            sub_part = Vec::new();
                        }
                    }
                    if !sub_part.is_empty() {
                        sub_part.sort_unstable_by(cmp_f.clone());
                        max_len = std::cmp::max(max_len, sub_part.len());
                        sub_parts.push(sub_part);
                    }
                    (sub_parts, max_len)
                }

                let sup_data_enc = call_seq.get_ser_captured_var().unwrap();
                let mut max_cnt_sum_buf = [0u8; 8];  
                max_cnt_sum_buf.copy_from_slice(&sup_data_enc[sup_data_enc.len()-2]);
                let max_cnt_sum = u64::from_le_bytes(max_cnt_sum_buf);
                let mut max_cnt_prod_buf = [0u8; 8];  
                max_cnt_prod_buf.copy_from_slice(&sup_data_enc[sup_data_enc.len()-1]);
                let max_cnt_prod = u64::from_le_bytes(max_cnt_prod_buf);
                //most of items in sup_data_enc may be marked invalid
                let mut sup_data = batch_decrypt::<(K, (Option<V>, Option<W>))>(&sup_data_enc[..sup_data_enc.len()-2], true)
                    .into_iter()
                    .map(|x| (x, (0u64, 0u64)))
                    .collect::<Vec<_>>();
                let mut cnt = (0, 0);
                if let Some(last_sup_group) = sup_data.group_by_mut(|x, y| x.0.0 == y.0.0).last() {
                    for ((_, (v, w)), _) in last_sup_group.iter_mut() {
                        cnt.0 += v.is_some() as u64;
                        cnt.1 += w.is_some() as u64;
                    }
                    if let Some(((k, _), last_cnt)) = last_sup_group.last_mut() {
                        *last_cnt = cnt;
                    }
                }
                let sup_data_len = sup_data.len();
                //count the items
                let data_enc = input.get_enc_data::<Vec<ItemE>>();
                let mut data = sup_data;
                data.append(&mut batch_decrypt(data_enc, true).into_iter()
                    .map(|x| (x, (0u64, 0u64)))
                    .collect::<Vec<_>>());
                for i in sup_data_len..data.len() {
                    if data.get(i-1).map_or(false, |d| d.0.0 == data[i].0.0) {
                        data[i].1.0 = data[i-1].1.0 + data[i].0.1.0.is_some() as u64;
                        data[i].1.1 = data[i-1].1.1 + data[i].0.1.1.is_some() as u64;
                    } else {
                        data[i].1.0 = data[i].0.1.0.is_some() as u64;
                        data[i].1.1 = data[i].0.1.1.is_some() as u64;
                    }
                }
                //fill the dimensions
                if let Some((_, cnt)) = data.last_mut() {
                    if call_seq.get_part_id() < self.number_of_splits() - 1 || cnt.0 == 0 || cnt.1 == 0 {
                        *cnt = (0, 0); //invalidate last one
                    }
                }

                for i in (0..data.len().saturating_sub(1)).rev() {
                    if data[i].0.0 == data[i+1].0.0 {
                        data[i].1 = data[i+1].1;
                    }
                    if data[i].1.0 == 0 || data[i].1.1 == 0 {
                        data[i].1 = (0, 0);
                    }
                }
                let data_len = data.len();
                //sort in order to split Ta and Tb
                let max_value = Default::default();
                //Tb < Ta < dummy
                let cmp_f = |a: &((K, (Option<V>, Option<W>)), (u64, u64)), b: &((K, (Option<V>, Option<W>)), (u64, u64))| {
                    ((3 - a.0.1.0.is_some() as u8 - 2 * a.0.1.1.is_some() as u8), &a.0.0).cmp(&((3 - b.0.1.0.is_some() as u8 - 2 * b.0.1.1.is_some() as u8), &b.0.0)) 
                };
                let (sub_parts, max_len) = split_with_interval(data, CACHE_LIMIT/input.get_parallel(), cmp_f.clone()); 
                let mut sort_helper = SortHelper::new(sub_parts, max_len, max_value, true, cmp_f);
                sort_helper.sort();
                let (ta, tb) = {
                    let mut sorted_data = sort_helper.take().0.into_iter().map(|((k, (v, w)), (a, b))| ((k, (v, w)), (a, b), 0u64)).collect::<Vec<_>>();
                    let cnt = sorted_data.group_by(|a, b| ((a.0.1.0.is_some(), a.0.1.1.is_some())) == ((b.0.1.0.is_some(), b.0.1.1.is_some()))).map(|x| x.len()).collect::<Vec<_>>();
                    assert_eq!(cnt.len(), 2);
                    let ta = sorted_data.split_off(cnt[0]);
                    (ta, sorted_data)
                };
                //compute the length needed to padding to
                let max_a = (((max_cnt_sum * max_cnt_sum) as f64/4.0 - max_cnt_prod as f64).sqrt() + max_cnt_sum as f64/2.0).round() as usize;
                let max_b = max_cnt_sum as usize - max_a; 
                let remaining_cnt = data_len % max_cnt_sum as usize;
                let remaining_cnt_prod = std::cmp::max(max_a * (remaining_cnt.saturating_sub(max_a)), max_b * (remaining_cnt.saturating_sub(max_b)));
                let len = data_len/max_cnt_sum as usize * max_cnt_prod as usize + remaining_cnt_prod;
                //oblivious expand
                let mut res = Vec::new();
                for (i, mut t) in vec![ta, tb].into_iter().enumerate() {
                    let mask = if i == 0 {
                        (0, 1)
                    } else {
                        (1, 0)
                    };
                    let mut s = 0;
                    for x in t.iter_mut() {
                        let g = x.1.0 * mask.0 + x.1.1 * mask.1;
                        if g != 0 {
                            x.2 = s;
                        } else {
                            x.2 = u64::MAX;
                        }
                        s += g;
                    }
                    println!("max_a = {:?}, max_b = {:?}, remaining_cnt = {:?}, remaining_cnt_prod = {:?}, len = {:?}, s = {:?}", max_a, max_b, remaining_cnt, remaining_cnt_prod, len, s);
                    assert!(s as usize <= len);

                    let max_value = (Default::default(), Default::default(), u64::MAX);
                    //real < dummy
                    let cmp_f = |a: &((K, (Option<V>, Option<W>)), (u64, u64), u64), b: &((K, (Option<V>, Option<W>)), (u64, u64), u64)| {
                        a.2.cmp(&b.2)
                    };
                    let (sub_parts, max_len) = split_with_interval(t, CACHE_LIMIT/input.get_parallel(), cmp_f.clone()); 
                    let mut sort_helper = SortHelper::new(sub_parts, max_len, max_value.clone(), true, cmp_f);
                    sort_helper.sort();
                    let mut sorted_data = sort_helper.take().0;
                    sorted_data.resize(len, max_value.clone());
                    //oblivious distribute
                    let mut j = 1usize << ((len as f64).log2().ceil() as u32 - 1);
                    while j >= 1 {
                        for i in (0..(len-j)).rev() {
                            if sorted_data[i].2 as usize >= i + j && sorted_data[i].2 != u64::MAX { //to be verified!
                                sorted_data.swap(i, i+j);
                            }
                        }
                        j = j >> 1;
                    }
                    //fill in missing entries
                    let mut alpha = 0;
                    for i in 0..sorted_data.len() {
                        if sorted_data[i].2 == u64::MAX && alpha > 0 {
                            sorted_data[i] = sorted_data[i-1].clone();
                            alpha -= 1;
                        } else {
                            alpha = sorted_data[i].1.0 * mask.0 + sorted_data[i].1.1 * mask.1 - 1;
                        }
                    }

                    //align tables
                    let t = if i == 1 {
                        let mut b = 0;
                        let mut q = 0;
                        for j in 1..sorted_data.len() {
                            if sorted_data[j].1.1 == 0 || sorted_data[j].1.0 == 0 {
                                for jj in j..sorted_data.len() {
                                    assert_eq!(sorted_data[jj].2, u64::MAX);
                                }
                                break;
                            }
                            if sorted_data[j].0.0 != sorted_data[j-1].0.0 {
                                q = 0;
                                b = sorted_data[j].2;
                            } else {
                                q += 1;
                                sorted_data[j].2 = b + q/sorted_data[j].1.0 + (q % sorted_data[j].1.0) * sorted_data[j].1.1; 
                            }
                        }
                        let (sub_parts, max_len) = split_with_interval(sorted_data, CACHE_LIMIT/input.get_parallel(), cmp_f.clone()); 
                        let mut sort_helper = SortHelper::new(sub_parts, max_len, max_value, true, cmp_f);
                        sort_helper.sort();
                        sort_helper.take().0.into_iter()
                            .map(|((k, (v, w)), _, pos)| ((k, (v, w)), pos != u64::MAX))
                            .collect::<Vec<_>>()
                    } else {
                        sorted_data.into_iter()
                            .map(|((k, (v, w)), _, pos)| ((k, (v, w)), pos != u64::MAX))
                            .collect::<Vec<_>>()
                    };
                    res.push(t);
                }
                let tb = res.pop().unwrap();
                let ta = res.pop().unwrap();
                let ((kp, (vp, _)), _) = ta[0].clone(); //in case the default value involved in invalid computation (e.g., /0).
                let ((_, (_, wp)), _) = tb[0].clone();
                let (table, marks): (Vec<_>, Vec<_>) =ta.into_iter().zip(tb.into_iter()).map(|(((ka, (va, wa)), ma), ((kb, (vb, wb)), mb))| {
                    assert_eq!(ma, mb);
                    if ma {
                        assert_eq!(ka, kb);
                        ((ka, (va.unwrap(), wb.unwrap())), ma)
                    } else {
                        ((kp.clone(), (vp.clone().unwrap(), wp.clone().unwrap())), false)
                    }
                }).unzip();

                let res_data = batch_encrypt(&table, true);
                let res_marks = batch_encrypt(&marks, true);

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