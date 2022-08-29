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
        let data_enc = input.get_enc_data::<Vec<ItemE>>();
        let mut res = create_enc();
        let aggregator = self.aggregator.clone(); 
        if aggregator.is_default {
            //group by is not obliviously implemented
            let mut sub_part_kc: Vec<(Option<K>, C)> = Vec::new();
            for sub_part in data_enc {
                let sub_part: Vec<(Option<K>, V)> = ser_decrypt(&sub_part.clone());
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
                        }
                    } else {
                        sub_part_kc.push((k, c));
                    }
                };
                let last = sub_part_kc.pop().unwrap();
                if !sub_part_kc.is_empty() {
                    let res_bl = ser_encrypt(&sub_part_kc);
                    merge_enc(&mut res, &res_bl);
                }
                sub_part_kc = vec![last];
            }
            if !sub_part_kc.is_empty() {
                let res_bl = ser_encrypt(&sub_part_kc);
                merge_enc(&mut res, &res_bl);
            }
        } else {
            //assume others are min, max, avg, add, etc.
            //they can be implemented obliviously
            let mut last_k = None;
            let mut last_c = Default::default();
            let mut last_kc: Option<(Option<K>, C)> = None;
            for sub_part in data_enc {
                let sub_part: Vec<(Option<K>, V)> = ser_decrypt(&sub_part.clone());
                let mut sub_part_kc: Vec<(Option<K>, C)> = Vec::with_capacity(sub_part.len());
                if let Some(last_kc) = last_kc {
                    sub_part_kc.push(last_kc);
                }
                for (k, v) in sub_part {
                    if last_k == k {
                        last_c = (aggregator.merge_value)((last_c, v));
                        if let Some(last_kc) = sub_part_kc.last_mut() {
                            last_kc.0 = None;
                        }
                    } else {
                        last_k = k.clone();
                        last_c = (aggregator.create_combiner)(v);
                    }
                    sub_part_kc.push((k, last_c.clone()));
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
        }
        res
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
            20 => {
                unreachable!();
            }
            21 | 22 | 23 | 25 | 26 | 27 | 28 => {
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
            20 => {
                let mut num_real_elem = 0;
                let data_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
                for part in data_enc {
                    for sub_part in part {
                        let sub_part: Vec<(Option<K>, V)> = ser_decrypt(&sub_part.clone());
                        num_real_elem += sub_part.len();
                    }
                }
                assert!(CNT_PER_PARTITION.lock().unwrap().insert((self.get_op_id(), call_seq.get_part_id()), num_real_elem).is_none());
                0 as *mut u8
            }
            21 | 22 | 23 => {
                combined_column_sort_step_4_6_8::<K, V>(call_seq.tid, input, dep_info, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits())
            },
            24 => { //aggregate again with the agg info from other servers
                let ser = call_seq.get_ser_captured_var().unwrap();
                let mut sup_data: Vec<(Option<K>, C)> = batch_decrypt(ser, false);
                assert_eq!(sup_data.len(), 1);
                let mut last_kc = sup_data.remove(0);
                let agg_data_ref = input.get_enc_data::<Vec<ItemE>>();
                let mut res = create_enc();
                let aggregator = self.aggregator.clone(); 

                for sub_part_enc in agg_data_ref {
                    let mut sub_part: Vec<(Option<K>, C)> = vec![last_kc];
                    sub_part.append(&mut ser_decrypt(&sub_part_enc.clone()));
                    for j in 1..sub_part.len() {
                        if sub_part[j].0.is_none() {
                            sub_part.swap(j-1, j);
                        } else if sub_part[j].0 == sub_part[j-1].0 {
                            sub_part[j].1 = (aggregator.merge_combiners)((std::mem::take(&mut sub_part[j-1].1), std::mem::take(&mut sub_part[j].1)));
                            sub_part[j-1].0 = None;
                        }
                    }
                    last_kc = sub_part.pop().unwrap();
                    if !sub_part.is_empty() {
                        let res_bl = ser_encrypt(&sub_part);
                        merge_enc(&mut res, &res_bl);
                    }
                }
                // the last one will be transmit to other servers
                let sub_part = vec![last_kc];
                let res_bl = ser_encrypt(&sub_part);
                merge_enc(&mut res, &res_bl);
                
                to_ptr(res)
            }
            25 => {
                combined_column_sort_step_2::<K, C>(call_seq.tid, input, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits())
            }
            26 | 27 | 28 => {
                combined_column_sort_step_4_6_8::<K, C>(call_seq.tid, input, dep_info, self.get_op_id(), call_seq.get_part_id(), self.number_of_splits())
            }
            _ => panic!("Invalid is_shuffle"),
        }
    }
}
