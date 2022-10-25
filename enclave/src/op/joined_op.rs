use std::hash::{Hash, Hasher};

use crate::aggregator::Aggregator;
use crate::dependency::{
    NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::obliv_comp::{VALID_BIT, obliv_global_filter_stage3_kv, obliv_agg_stage1, obliv_agg_stage2, obliv_group_by_stage1, obliv_join::*};
use crate::op::*;
use crate::partitioner::hash;

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

    pub fn compute_inner(&self, tid: u64, input: Input, padding_len: usize) -> (Vec<ItemE>, Vec<ItemE>) {
        let buckets_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
        let outer_parallel = input.get_parallel();
        let (buckets, max_len) = read_sorted_bucket::<((K, (V, W, i64)), u64)>(buckets_enc, outer_parallel);
        let (table, marks) = obliv_join_stage6(buckets, max_len, padding_len, outer_parallel);
        
        let res_data = batch_encrypt(&table, true);
        let res_marks = batch_encrypt(&marks, true);

        (res_data, res_marks)
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
            0 | 2 | 21 | 24 | 25 | 12 | 14 => self.free_res_enc(data, marks, is_enc),
            1 | 11 => {
                assert_eq!(marks as usize, 0usize);
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(data, dep_info, is_enc);
            },
            20 => {
                //different from that in shuffled_op, because here are two tables, so an additional layer of vector
                crate::ALLOCATOR.set_switch(true);
                let data = unsafe { Box::from_raw(data as *mut Vec<Vec<ItemE>>) };
                drop(data);
                let cnt_buckets = unsafe { Box::from_raw(marks as *mut Vec<Vec<Vec<ItemE>>>) };
                drop(cnt_buckets);
                crate::ALLOCATOR.set_switch(false);
            },
            22 => {
                crate::ALLOCATOR.set_switch(true);
                let data = unsafe { Box::from_raw(data as *mut Vec<ItemE>) };
                drop(data);
                let marks = unsafe { Box::from_raw(marks as *mut ItemE) };
                drop(marks);
                crate::ALLOCATOR.set_switch(false);
            },
            23 | 26 | 27 | 13 => {
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
        match dep_info.dep_type() {
            0 => {       //narrow
                self.narrow(call_seq, input, true)
            },
            1 | 11 => {       //shuffle write
                self.shuffle(call_seq, input, dep_info)
            },
            2 => {       //shuffle read
                let padding_len: usize =
                    bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                let (data, marks) = self.compute_inner(call_seq.tid, input, padding_len);
                (to_ptr(data), to_ptr(marks))
            },
            20 => {
                let (_, buckets_enc_a, _, buckets_enc_b) = input.get_enc_data::<(
                    (Vec<ItemE>, Vec<ItemE>), 
                    Vec<Vec<ItemE>>, 
                    (Vec<ItemE>, Vec<ItemE>), 
                    Vec<Vec<ItemE>>
                )>();
                let outer_parallel = input.get_parallel();
                let part_id = call_seq.get_part_id();
                let seed = hash(&call_seq.get_cur_rdd_id());

                let mut part_enc = create_enc();
                let mut buckets_enc = create_enc();
                let (part_enc_a, buckets_enc_a) = filter_3_agg_1::<K, V>(buckets_enc_a, outer_parallel, part_id, &self.part, seed);
                let (part_enc_b, buckets_enc_b) = filter_3_agg_1::<K, W>(buckets_enc_b, outer_parallel, part_id, &self.part, seed);
                crate::ALLOCATOR.set_switch(true);
                part_enc.push(part_enc_a);
                part_enc.push(part_enc_b);
                buckets_enc.push(buckets_enc_a);
                buckets_enc.push(buckets_enc_b);
                crate::ALLOCATOR.set_switch(false);

                (to_ptr(part_enc), to_ptr(buckets_enc))
            },
            21 => {
                let buckets_enc_set = input.get_enc_data::<Vec<Vec<Vec<ItemE>>>>();
                assert_eq!(buckets_enc_set.len(), 2);
                let outer_parallel = input.get_parallel();

                //continue group count
                let create_combiner = Box::new(|v: i64| v);
                let merge_value = Box::new(move |(buf, v)| buf + v);
                let merge_combiners = Box::new(move |(b1, b2)| b1 + b2);
                let aggregator = Arc::new(Aggregator::new(
                    create_combiner,
                    merge_value,
                    merge_combiners,
                ));
            
                let mut info = Vec::new();
                let mut part_set = Vec::new(); //part A and part B
                for buckets_enc in buckets_enc_set {
                    let (parts, max_len) = read_sorted_bucket::<((K, i64), u64)>(buckets_enc, outer_parallel);
                    let part = obliv_agg_stage2(parts, max_len, false, false, &aggregator);
                    let (a, cnt) = obliv_group_by_stage1(&part);
                    part_set.push(part);
                    info.push((a, cnt));
                }

                let part_ptr = Box::into_raw(Box::new(part_set)) as *mut u8 as usize;
                let mut buf = create_enc();
                merge_enc(&mut buf, &ser_encrypt(&part_ptr));
                merge_enc(&mut buf, &ser_encrypt(&info));
                (to_ptr(buf), 0 as *mut u8)
            },
            22 => {
                let part_group: (usize, usize, usize) =
                    bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);
                let outer_parallel = input.get_parallel();
                let cache_limit = CACHE_LIMIT / MAX_THREAD / outer_parallel;

                let (part_ptr, stat) = input.get_enc_data::<(ItemE, Vec<ItemE>)>();
                let part_ptr: usize = ser_decrypt(&part_ptr.clone());
                let mut part_set = *unsafe {
                    Box::from_raw(part_ptr as *mut u8 as *mut Vec<Vec<((K, i64), u64)>>)
                };
                let stat = stat.iter().map(|x| ser_decrypt::<Vec<(usize, usize)>>(&x.clone())).collect::<Vec<_>>();
                let (alpha, beta): (Vec<usize>, Vec<usize>) = stat.into_iter()
                        .fold(vec![(0, 0), (0, 0)], |x, y| {
                            assert!(x.len() == 2 && y.len() == 2);
                            x.into_iter()
                                .zip(y.into_iter())
                                .map(|(xx, yy)| (std::cmp::max(xx.0, yy.0), xx.1 + yy.1))
                                .collect::<Vec<_>>()
                        })
                        .into_iter()
                        .unzip();
            
                let part_b = part_set.pop().unwrap();
                let part_a = part_set.pop().unwrap();
                let (last_bin_num, last_bin_size, n_out_prime, acc_prod, part) = obliv_join_stage1(
                    part_a,
                    part_b,
                    alpha[0],
                    alpha[1],
                    beta[0],
                    beta[1],
                    part_group.2,
                    cache_limit,
                );

                let part_ptr = Box::into_raw(Box::new(part)) as *mut u8 as usize;
                let mut buf = create_enc();
                merge_enc(&mut buf, &ser_encrypt(&part_ptr));
                merge_enc(&mut buf, &ser_encrypt(&(last_bin_num, last_bin_size, acc_prod)));
                let meta_data = bincode::serialize(&(alpha, beta, n_out_prime)).unwrap();
                crate::ALLOCATOR.set_switch(true);
                let meta_data_ptr = Box::into_raw(Box::new(meta_data.clone()));
                crate::ALLOCATOR.set_switch(false);
                (to_ptr(buf), meta_data_ptr as *mut u8)
            },
            23 => {
                let (alpha, n_out_prime): (Vec<usize>, usize) = bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();

                let part_id = call_seq.get_part_id();
                let cache_limit = CACHE_LIMIT/MAX_THREAD/input.get_parallel();

                let (part_ptr, last_bin_info) = input.get_enc_data::<(ItemE, Vec<ItemE>)>();
                let part_ptr: usize = ser_decrypt(&part_ptr.clone());
                let part = *unsafe {
                    Box::from_raw(part_ptr as *mut u8 as *mut Vec<((K, (i64, i64)), u64, u64)>)
                };
                let last_bin_info = last_bin_info.iter().map(|x| ser_decrypt::<(usize, usize, i64)>(&x.clone())).collect::<Vec<_>>();
                let (part, data, acc_col, idx_last, acc_col_rem) = obliv_join_stage2(
                    part,
                    cache_limit,
                    &alpha,
                    part_id,
                    last_bin_info,
                    n_out_prime,
                );
            
                let mut data_set_enc = create_enc();
                merge_enc(&mut data_set_enc, &batch_encrypt(&part, false));
                merge_enc(&mut data_set_enc, &batch_encrypt(&data, false));
                merge_enc(&mut data_set_enc, &vec![ser_encrypt(&acc_col_rem)]);
                merge_enc(
                    &mut data_set_enc,
                    &vec![ser_encrypt(&(acc_col, idx_last))],
                );
                (to_ptr(data_set_enc), 0 as *mut u8)
            },
            24 => {
                let n_out_prime: usize =
                    bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                let acc_col_rem = input.get_enc_data::<Vec<ItemE>>();
                let mut acc_col_rem = acc_col_rem
                    .iter()
                    .map(|e| ser_decrypt::<Vec<(usize, usize)>>(&e.clone()))
                    .enumerate()
                    .map(|(j, x)| x.into_iter().map(move |v| (v.0, v.1, j)))
                    .flatten()
                    .collect::<Vec<_>>();
                acc_col_rem.sort_by(|a, b| a.1.cmp(&b.1));
                let mut acc_col = (0..n_out_prime).map(|x| (x, 0)).collect::<Vec<_>>();
                for (i, d) in acc_col_rem.iter_mut().enumerate() {
                    if d.1 != usize::MAX {
                        let k = i % n_out_prime;
                        acc_col[k].1 += d.1;
                        d.1 = k;
                    }
                }
                acc_col_rem.sort_by(|a, b| (a.2, a.0).cmp(&(b.2, b.0)));
                let acc_col_rem = acc_col_rem.chunks(n_out_prime).collect::<Vec<_>>()[call_seq.get_part_id()]
                    .into_iter()
                    .map(|x| (x.0, x.1))
                    .collect::<Vec<_>>();
                let mut data_enc = create_enc();
                let acc_col_enc = ser_encrypt(&acc_col);
                let acc_col_rem_enc = ser_encrypt(&acc_col_rem);
                merge_enc(&mut data_enc, &acc_col_enc);
                merge_enc(&mut data_enc, &acc_col_rem_enc);

                (to_ptr(data_enc), 0 as *mut u8)
            }
            25 => {
                let part_group: (usize, usize, usize) =
                    bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);

                let acc_cols = input.get_enc_data::<Vec<ItemE>>();
                let (mut acc_col, mut last_bin_loc): (Vec<(usize, usize)>, i64) = ser_decrypt(&acc_cols[0].clone());   
                let acc_col_rem: Vec<(usize, usize)> = ser_decrypt(&acc_cols[1].clone());
                let mut acc_col_rec: Vec<(usize, usize)> = ser_decrypt(&acc_cols[2].clone());
                let last_bin_loc_rec: i64 = if acc_cols[3].is_empty() {
                    -1
                } else {
                    ser_decrypt(&acc_cols[3].clone())
                };

                obliv_join_stage3(&mut acc_col_rec, &mut acc_col, last_bin_loc_rec, &mut last_bin_loc, acc_col_rem);
                let mut data_enc = create_enc();
                let acc_col_rec_enc = ser_encrypt(&acc_col_rec);
                let acc_col_enc = ser_encrypt(&acc_col);
                let last_bin_loc_enc = ser_encrypt(&last_bin_loc);
                merge_enc(&mut data_enc, &acc_col_rec_enc);
                merge_enc(&mut data_enc, &acc_col_enc);
                merge_enc(&mut data_enc, &last_bin_loc_enc);

                if call_seq.get_part_id() == part_group.2 - 1 {
                    let join_cnt = acc_col_rec.into_iter().map(|x| x.1).sum::<usize>();
                    (to_ptr(data_enc), join_cnt as *mut u8)
                } else {
                    (to_ptr(data_enc), 0 as *mut u8)
                }
            },
            26 => {
                let part_group: (usize, usize, usize) =
                    bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);
                let cache_limit = CACHE_LIMIT / MAX_THREAD / input.get_parallel();

                let (part, data, acc_col, last_bin_loc, acc_col_rem) = input.get_enc_data::<(Vec<ItemE>, Vec<ItemE>, ItemE, ItemE, ItemE)>();
                let part: Vec<((K, (i64, i64)), u64, u64)> = batch_decrypt(part, true);
                let data: Vec<(usize, usize, usize)> = batch_decrypt(data, true);
                let acc_col: Vec<(usize, usize)> = ser_decrypt(&acc_col);
                let last_bin_loc: i64 = ser_decrypt(last_bin_loc);
                let acc_col_rem: Vec<(usize, usize)> = ser_decrypt(acc_col_rem);
                let buckets_enc = obliv_join_stage4(part, data, acc_col, last_bin_loc, acc_col_rem, part_group.2, cache_limit);
                
                (to_ptr(buckets_enc), 0 as *mut u8)
            },
            27 => {
                let (part_group, beta, n_out_prime): ((usize, usize, usize), Vec<usize>, usize) =
                    bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);
                let part_id = call_seq.get_part_id();
            
                let data_set_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
                let buckets_set_enc = input.get_enc_marks::<Vec<Vec<Vec<ItemE>>>>();
                assert!(data_set_enc.len() == 2 && buckets_set_enc.len() == 2);
            
                let buckets = obliv_join_stage5::<K, V, W>(data_set_enc, buckets_set_enc, beta, part_group.2, n_out_prime, input.get_parallel());
                let mut buckets_enc = create_enc();
                for bucket in buckets {
                    merge_enc(&mut buckets_enc, &batch_encrypt(&bucket, false));
                }

                (to_ptr(buckets_enc), 0 as *mut u8)
            },
            12 | 13 | 14 => {
                self.remove_dummy(call_seq, input, dep_info)
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