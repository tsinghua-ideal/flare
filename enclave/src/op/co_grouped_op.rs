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
    pub is_for_join: bool,
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    pub(crate) op0: Arc<dyn Op<Item = (K, V)>>,
    pub(crate) op1: Arc<dyn Op<Item = (K, W)>>,
    pub(crate) part: Box<dyn Partitioner>,
    pub(crate) cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<(K, (Vec<V>, Vec<W>))>>>>>
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
            cache_space: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn compute_inner(&self, tid: u64, input: Input) -> Vec<ItemE> {
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let mut agg: Vec<(Option<K>, (Vec<V>, Vec<W>))> = Vec::new();
        let mut res = create_enc();
        for sub_part in data_enc {
            let sub_part: Vec<(Option<K>, (Option<V>, Option<W>))> = ser_decrypt(&sub_part.clone());
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
                    }
                } else {
                    agg.push((k, (vs, ws)));
                }
            }
            let last = agg.pop().unwrap();
            if !agg.is_empty() {
                let res_bl = ser_encrypt(&agg);
                merge_enc(&mut res, &res_bl);
            }
            agg = vec![last];
        }
        // the last one will be transmit to other servers
        if !agg.is_empty() {
            let res_bl = ser_encrypt(&agg);
            merge_enc(&mut res, &res_bl);
        }
        res
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
            20 | 21 | 22 | 23 | 25 | 26 | 27 | 28 => {
                crate::ALLOCATOR.set_switch(true);
                let res = unsafe { Box::from_raw(res_ptr as *mut Vec<Vec<ItemE>>) };
                drop(res);
                crate::ALLOCATOR.set_switch(false);
            },
            24 => {
                crate::ALLOCATOR.set_switch(true);
                let res = unsafe { Box::from_raw(res_ptr as *mut Vec<ItemE>) };
                drop(res);
                crate::ALLOCATOR.set_switch(false);
            }
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
    
    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8{
        
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

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        match dep_info.dep_type() {
            0 => {       //narrow
                self.narrow(call_seq, input, true)
            },
            1 => {       //shuffle write
                self.shuffle(call_seq, input, dep_info)
            },
            2 => {       //shuffle read
                let res = self.compute_inner(call_seq.tid, input);
                to_ptr(res)
            },
            20 => {      //column sort, step 1 + step 2
                let data_enc_ref = input.get_enc_data::<(
                    Vec<ItemE>, 
                    Vec<Vec<ItemE>>, 
                    Vec<ItemE>, 
                    Vec<Vec<ItemE>>
                )>();
                // sub_parts[i][j] < sub_parts[i][j+1] in sub_parts[i]
                let mut data = Vec::new();
                let mut max_len = 0;

                // (Option<K>, V) -> (Option<K>, (Option<V>, Option<W>))
                for sub_part_enc in &data_enc_ref.0 {
                    //not sure about the type of the fed key, Option<K> or <K>
                    let mut sub_part: Vec<(K, V)> = ser_decrypt(&sub_part_enc.clone());
                    max_len = std::cmp::max(max_len, sub_part.len());
                    unimplemented!()
                }
                for part_enc in &data_enc_ref.1 {
                    let mut part = Vec::new();
                    let mut sub_part = Vec::new();
                    for block_enc in part_enc {
                        sub_part.append(&mut ser_decrypt::<Vec<(Option<K>, V)>>(&block_enc.clone()).into_iter()
                            .map(|(k, v)| (k, (Some(v), None::<W>)))
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
                for sub_part_enc in &data_enc_ref.2 {
                    let mut sub_part: Vec<(K, W)> = ser_decrypt(&sub_part_enc.clone());
                    max_len = std::cmp::max(max_len, sub_part.len());
                    unimplemented!();
                }

                for part_enc in &data_enc_ref.3 {
                    let mut part = Vec::new();
                    let mut sub_part = Vec::new();
                    for block_enc in part_enc {
                        sub_part.append(&mut ser_decrypt::<Vec<(Option<K>, W)>>(&block_enc.clone()).into_iter()
                            .map(|(k, w)| (k, (None::<V>, Some(w))))
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
                    res_enc_to_ptr(Vec::<Vec<ItemE>>::new())
                } else {
                    let mut sort_helper = SortHelper::<K, (Option<V>, Option<W>)>::new_with(data, max_len, true);
                    sort_helper.sort();
                    let (sorted_data, num_real_elem) = sort_helper.take();
                    assert!(CNT_PER_PARTITION.lock().unwrap().insert((op_id, call_seq.get_part_id()), num_real_elem).is_none());
                    let num_output_splits = self.number_of_splits();
                    let buckets_enc = column_sort_step_2::<(Option<K>, (Option<V>, Option<W>))>(call_seq.tid, sorted_data, max_len, num_output_splits);
                    to_ptr(buckets_enc)
                }
            },
            21 | 22 | 23 => {     //column sort, remaining steps
                combined_column_sort_step_4_6_8::<K, (Option<V>, Option<W>)>(call_seq.tid, input, dep_info, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits())
            },
            24 => { //aggregate again with the agg info from other servers
                let ser = call_seq.get_ser_captured_var().unwrap();
                let mut sup_data: Vec<(Option<K>, (Vec<V>, Vec<W>))> = batch_decrypt(ser, false);
                assert_eq!(sup_data.len(), 1);
                let mut sup_data = sup_data.remove(0);
                let agg_data_ref = input.get_enc_data::<Vec<ItemE>>();
                let mut res = create_enc();
                let mut agg_data: Vec<(Option<K>, (Vec<V>, Vec<W>))> = ser_decrypt(&agg_data_ref[0]);
                
                if !agg_data.is_empty() && agg_data[0].0 == sup_data.0 {
                    agg_data[0].1.0.append(&mut sup_data.1.0);
                    agg_data[0].1.1.append(&mut sup_data.1.1);
                } else {
                    agg_data.insert(0, sup_data);
                }

                let last = agg_data.pop().unwrap();
                if !agg_data.is_empty() {
                    let res_bl = ser_encrypt(&agg_data);
                    merge_enc(&mut res, &res_bl);
                }
                let res_bl = ser_encrypt(&vec![last]);
                merge_enc(&mut res, &res_bl);
                to_ptr(res)
            }
            25 => {
                combined_column_sort_step_2::<K, (Vec<V>, Vec<W>)>(call_seq.tid, input, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits())
            }
            26 | 27 | 28 => {
                combined_column_sort_step_4_6_8::<K, (Vec<V>, Vec<W>)>(call_seq.tid, input, dep_info, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits())
            }
            _ => panic!("Invalid is_shuffle")
        }
    }
}