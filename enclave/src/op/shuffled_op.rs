use std::collections::BTreeMap;
use std::hash::Hash;

use crate::aggregator::Aggregator;
use crate::dependency::ShuffleDependency;
use crate::obliv_comp::{VALID_BIT, obliv_agg_stage2, obliv_group_by::*};
use crate::op::*;
use crate::partitioner::hash;
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
        let buckets_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
        let outer_parallel = input.get_parallel();
        let aggregator = self.aggregator.clone(); 
        
        if aggregator.is_default {
            let (buckets, max_len) = read_sorted_bucket::<((K, V), u64)>(buckets_enc, outer_parallel);
            let (data, marks) = obliv_group_by_stage5(buckets, max_len, &aggregator);
            let data_enc = batch_encrypt(&data, true);
            let marks_enc = batch_encrypt(&marks, true);
            (data_enc, marks_enc)
        } else {
            let (buckets, max_len) = read_sorted_bucket::<((K, C), u64)>(buckets_enc, outer_parallel);
            let part = obliv_agg_stage2(buckets, max_len, false, true, &aggregator);
            let part_enc = batch_encrypt(&part.into_iter().map(|(t, _)| t).collect::<Vec<_>>(), true);   
            (part_enc, Vec::new())
        }
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
            0 | 2 | 21 | 12 | 14 => self.free_res_enc(data, marks, is_enc),
            1 | 11 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(data, dep_info, is_enc);
            },
            20 => {
                crate::ALLOCATOR.set_switch(true);
                let data = unsafe { Box::from_raw(data as *mut Vec<ItemE>) };
                drop(data);
                let cnt_buckets = unsafe { Box::from_raw(marks as *mut Vec<Vec<ItemE>>) };
                drop(cnt_buckets);
                crate::ALLOCATOR.set_switch(false);
            },
            22 => {
                crate::ALLOCATOR.set_switch(true);
                let info = unsafe { Box::from_raw(data as *mut Vec<ItemE>) };
                drop(info);
                let meta_data = unsafe { Box::from_raw(marks as *mut ItemE) };
                drop(meta_data);
                crate::ALLOCATOR.set_switch(false);
            }
            23 | 24 | 13 => {
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
            1 | 11 => {      //Shuffle write
                self.shuffle(call_seq, input, dep_info)
            }
            2 => {
                let (data, marks) = self.compute_inner(call_seq.tid, input);
                (to_ptr(data), to_ptr(marks))
            }
            20 => {
                let buckets_enc = input.get_enc_data::<Vec<Vec<ItemE>>>();
                let outer_parallel = input.get_parallel();
                let seed = hash(&call_seq.get_cur_rdd_id());
                let (part_enc, buckets_enc) = filter_3_agg_1::<K, V>(buckets_enc, outer_parallel, call_seq.get_part_id(), &self.part, seed);
                (to_ptr(part_enc), to_ptr(buckets_enc))
            }
            21  => {
                let part_id = call_seq.get_part_id();
                let cache_limit = CACHE_LIMIT/MAX_THREAD/input.get_parallel();

                //continue group count
                let create_combiner = Box::new(|v: i64| v);
                let merge_value = Box::new(move |(buf, v)| { buf + v });
                let merge_combiners = Box::new(move |(b1, b2)| { b1 + b2 });
                let aggregator = Arc::new(Aggregator::new(create_combiner, merge_value, merge_combiners));

                let (parts, max_len) = read_sorted_bucket::<((K, i64), u64)>(input.get_enc_data::<Vec<Vec<ItemE>>>(), input.get_parallel());
                let part = obliv_agg_stage2(parts, max_len, false, false, &aggregator);
                let (a, cnt) = obliv_group_by_stage1(&part);

                let part_ptr = Box::into_raw(Box::new(part)) as *mut u8 as usize;
                let mut buf = create_enc();
                merge_enc(&mut buf, &ser_encrypt(&part_ptr));
                merge_enc(&mut buf, &ser_encrypt(&(a, cnt)));
                (to_ptr(buf), 0 as *mut u8)
            },
            22 => {
                let part_group: (usize, usize, usize) = bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);

                let (part_ptr, stat) = input.get_enc_data::<(ItemE, Vec<ItemE>)>();
                let part_ptr: usize = ser_decrypt(&part_ptr.clone());
                let part = *unsafe {
                    Box::from_raw(part_ptr as *mut u8 as *mut Vec<((K, i64), u64)>)
                };
                let stat = stat.iter().map(|x| ser_decrypt::<(usize, usize)>(&x.clone())).collect::<Vec<_>>();
                let (alpha, beta) = stat
                    .into_iter()
                    .fold((0, 0), |x, y| (std::cmp::max(x.0, y.0), x.1 + y.1));
                let mut part = part.into_iter().map(|((k, v), m)| ((k, v), m, 0)).collect::<Vec<_>>();    
                let (last_bin_num, last_bin_size, n_out_prime) = obliv_group_by_stage2(&mut part, alpha, beta, part_group.2);
                let part_ptr = Box::into_raw(Box::new(part)) as *mut u8 as usize;
                let mut buf = create_enc();
                merge_enc(&mut buf, &ser_encrypt(&part_ptr));
                merge_enc(&mut buf, &ser_encrypt(&(last_bin_num, last_bin_size)));
                let meta_data = bincode::serialize(&(alpha, beta, n_out_prime)).unwrap();
                crate::ALLOCATOR.set_switch(true);
                let meta_data_ptr = Box::into_raw(Box::new(meta_data.clone()));
                crate::ALLOCATOR.set_switch(false);
                (to_ptr(buf), meta_data_ptr as *mut u8)
            }, 
            23 => {
                let (part_group, alpha): ((usize, usize, usize), usize) = bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);

                let part_id = call_seq.get_part_id();
                let cache_limit = CACHE_LIMIT/MAX_THREAD/input.get_parallel();

                let (part_ptr, last_bin_info) = input.get_enc_data::<(ItemE, Vec<ItemE>)>();
                let part_ptr: usize = ser_decrypt(&part_ptr.clone());
                let part = *unsafe {
                    Box::from_raw(part_ptr as *mut u8 as *mut Vec<((K, i64), u64, u64)>)
                };
                let last_bin_info = last_bin_info.iter().map(|x| ser_decrypt::<(usize, usize)>(&x.clone())).collect::<Vec<_>>();
                let buckets = obliv_group_by_stage3(part, cache_limit, alpha, part_id, last_bin_info, part_group.2, part_group.2);
                let mut buckets_enc = create_enc();
                for bucket in buckets {
                    merge_enc(&mut buckets_enc, &batch_encrypt(&bucket, false));
                }
                (to_ptr(buckets_enc), 0 as *mut u8)
            },
            24 => {
                let (part_group, beta, n_out_prime): ((usize, usize, usize), usize, usize) = bincode::deserialize(&call_seq.get_ser_captured_var().unwrap()[0]).unwrap();
                assert_eq!(part_group.0, call_seq.stage_id);

                let data_enc = input.get_enc_data::<Vec<ItemE>>();
                let buckets_enc = input.get_enc_marks::<Vec<Vec<ItemE>>>();

                let data = data_enc.iter().map(|bl_enc| {
                    ser_decrypt::<Vec<(K, V)>>(&bl_enc.clone())
                        .into_iter()
                        .map(|t| (t, 1u64 << VALID_BIT))
                        .collect::<Vec<_>>()
                }).collect::<Vec<_>>();
                let (buckets, max_len_subpart) = read_sorted_bucket::<((K, i64), u64, u64)>(buckets_enc, input.get_parallel());
                let buckets = obliv_group_by_stage4(data, buckets, max_len_subpart, beta, part_group.2, n_out_prime, input.get_parallel());
                let mut buckets_enc = create_enc();
                for bucket in buckets {
                    merge_enc(&mut buckets_enc, &batch_encrypt(&bucket, false));
                }
                (to_ptr(buckets_enc), 0 as *mut u8)
            },
            12 | 13 | 14 => {
                self.remove_dummy(call_seq, input, dep_info)
            },
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
