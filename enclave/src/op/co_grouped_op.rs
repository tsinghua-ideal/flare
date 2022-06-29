use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

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
        fn decrypt_buckets<K: Ord + Data, T: Data>(data_enc: &Vec<Vec<ItemE>>) -> Vec<Vec<(K, T)>> {
            data_enc.iter().map(|bucket| batch_decrypt(bucket, true)).collect::<Vec<_>>()
        }
        fn merge_core<K: Ord + Data, V: Data, W: Data>(a1: Vec<Vec<(K, Vec<V>)>>, b1: Vec<Vec<(K, Vec<W>)>>) -> Vec<(K, (Vec<V>, Vec<W>))> {
            let mut agg: Vec<(K, (Vec<V>, Vec<W>))> = Vec::new();
            let mut iter_a = a1.into_iter().kmerge_by(|a, b| a.0 < b.0);
            let mut iter_b = b1.into_iter().kmerge_by(|a, b| a.0 < b.0);

            let mut cur_a = iter_a.next();
            let mut cur_b = iter_b.next();

            while cur_a.is_some() || cur_b.is_some() {
                let should_puta = cur_b.is_none()
                    || cur_a.is_some() 
                        &&  cur_a.as_ref().unwrap().0 < cur_b.as_ref().unwrap().0;
                if should_puta {
                    let (k, mut v) = cur_a.unwrap();
                    cur_a = iter_a.next();
                    if agg.last().map_or(false, |(last_k, _)| last_k == &k) {
                        let (_, (last_v, _)) = agg.last_mut().unwrap();
                        last_v.append(&mut v);
                    } else if agg.last().map_or(false, |(_, (last_v, last_w))| last_v.is_empty() || last_w.is_empty()) {
                        *agg.last_mut().unwrap() = (k, (v, Vec::new()));
                    } else {
                        agg.push((k, (v, Vec::new())));
                    }
                } else {
                    let (k, mut w) = cur_b.unwrap();
                    cur_b = iter_b.next();
                    if agg.last().map_or(false, |(last_k, _)| last_k == &k) {
                        let (_, (_, last_w)) = agg.last_mut().unwrap();
                        last_w.append(&mut w);
                    } else if agg.last().map_or(false, |(_, (last_v, last_w))| last_v.is_empty() || last_w.is_empty()) {
                        *agg.last_mut().unwrap() = (k, (Vec::new(), w));
                    } else {
                        agg.push((k, (Vec::new(), w)));
                    }
                }
            }
            agg
        }

        let data_enc = input.get_enc_data::<(
            Vec<Vec<ItemE>>,
            Vec<Vec<Vec<ItemE>>>, 
            Vec<Vec<ItemE>>, 
            Vec<Vec<Vec<ItemE>>>
        )>();

        let (mut a0, mut a1, mut b0, mut b1): (
            Vec<Vec<(K, V)>>,
            Vec<Vec<Vec<(K, Vec<V>)>>>,
            Vec<Vec<(K, W)>>,
            Vec<Vec<Vec<(K, Vec<W>)>>>,
        ) = (
            decrypt_buckets(&data_enc.0),
            data_enc.1.iter().map(|buckets| decrypt_buckets(buckets)).collect::<Vec<_>>(),
            decrypt_buckets(&data_enc.2),
            data_enc.3.iter().map(|buckets| decrypt_buckets(buckets)).collect::<Vec<_>>(),
        );
        assert_eq!(data_enc.1.len(), MAX_THREAD + 1);
        assert_eq!(data_enc.3.len(), MAX_THREAD + 1);

        //currently only support that both children are shuffle dep
    
        let (is_para_mer, agg) = {
            crate::ALLOCATOR.reset_alloc_cnt();
            let sample_a1 = a1.pop().unwrap();
            let sample_b1 = b1.pop().unwrap();
            let sample_len = sample_a1.iter().map(|v| v.len()).sum::<usize>() 
                + sample_b1.iter().map(|v| v.len()).sum::<usize>();
            let agg = merge_core(sample_a1, sample_b1);
            let alloc_cnt = crate::ALLOCATOR.get_alloc_cnt();
            let alloc_cnt_ratio = (alloc_cnt as f64)/(sample_len as f64);
            println!("for merge, alloc_cnt per len = {:?}", alloc_cnt_ratio);
            (alloc_cnt_ratio < PARA_THRESHOLD, agg)
        };

        let mut handlers = Vec::with_capacity(MAX_THREAD);
        if is_para_mer {
            for i in 0..MAX_THREAD {
                let a1 = a1.pop().unwrap();
                let b1 = b1.pop().unwrap();
                let builder = thread::Builder::new();
                let handler = builder
                    .spawn(move || {
                        merge_core(a1, b1)
                    }).unwrap();
                handlers.push(handler);
            }
        } 

        let mut acc = create_enc();
        for agg in vec![agg].into_iter()
            .chain(handlers.into_iter().map(|handler| handler.join().unwrap()))
            .chain(a1.into_iter().zip(b1.into_iter()).map(|(a1, b1)| merge_core(a1, b1))) 
        {
            //block reshape
            let res = if self.is_for_join {
                let mut len = 0;
                let agg = agg.split_inclusive(|(k, (v, w))| {
                        len += v.deep_size_of() * w.deep_size_of();
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
            combine_enc(&mut acc, res_enc);
        }
        acc
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
            }
            _ => panic!("Invalid is_shuffle")
        }
    }
}