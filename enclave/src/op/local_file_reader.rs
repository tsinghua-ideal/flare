use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use crate::op::*;
use serde_derive::{Deserialize, Serialize};

pub trait ReaderConfiguration<I: Data> {
    #[track_caller]
    fn make_reader<O, F, F0>(self, context: Arc<Context>, decoder: Option<F>, sec_decoder: Option<F0>) -> SerArc<dyn Op<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O,
        F0: SerFunc(I) -> Vec<ItemE>;
}

pub struct LocalFsReaderConfig {
    dir_path: PathBuf,   //placeholder
    executor_partitions: Option<u64>,
}

impl LocalFsReaderConfig {
    pub fn new<T: Into<PathBuf>>(path: T) -> LocalFsReaderConfig {
        LocalFsReaderConfig {
            dir_path: path.into(),
            executor_partitions: None,
        }
    }

    pub fn num_partitions_per_executor(mut self, num: u64) -> Self {
        self.executor_partitions = Some(num);
        self
    }
}

impl<I: Data> ReaderConfiguration<I> for LocalFsReaderConfig {
    fn make_reader<O, F, F0>(self, context: Arc<Context>, decoder: Option<F>, sec_decoder: Option<F0>) -> SerArc<dyn Op<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O,
        F0: SerFunc(I) -> Vec<ItemE>,
    {
        let reader = LocalFsReader::new(self, context, sec_decoder.clone());
        if !reader.get_context().get_is_tail_comp() {
            insert_opmap(reader.get_op_id(), reader.get_op_base());
        }
        let local_num_splits: usize = 1;
        let read_files = 
            Fn!(move |part: usize, readers: Box<dyn Iterator<Item = O>>| {
                let _part = part % local_num_splits;
                readers
            });
        reader.get_context().add_num(1);
        let files_per_executor = Arc::new(
            MapPartitions::new(Arc::new(reader) as Arc<dyn Op<Item = _>>, read_files),
        );
        insert_opmap(files_per_executor.get_op_id(), files_per_executor.get_op_base());
        files_per_executor.get_context().add_num(1);
        let decoder = Mapper::new(files_per_executor, Fn!(|v| v));
        let decoder = SerArc::new(decoder);
        insert_opmap(decoder.get_op_id(), decoder.get_op_base());
        decoder
    }
}

#[derive(Clone)]
pub struct LocalFsReader<I, U, F0> 
where
    I: Data,
    U: Data,
    F0: Func(I) -> Vec<ItemE> + Clone,
{
    vals: Arc<OpVals>,
    path: PathBuf,
    sec_decoder: Option<F0>,
    cache_space: Arc<Mutex<HashMap<(usize, usize), Vec<Vec<U>>>>>,
    _marker_text_data: PhantomData<I>,
}

impl<I, U, F0> LocalFsReader<I, U, F0> 
where
    I: Data,
    U: Data,
    F0: Func(I) -> Vec<ItemE> + Clone,
{
    #[track_caller]
    fn new(config: LocalFsReaderConfig, context: Arc<Context>, sec_decoder: Option<F0>) -> Self {
        let LocalFsReaderConfig {
            dir_path,
            executor_partitions,
        } = config;
        let vals = Arc::new(OpVals::new(context, 1)); // 1 is temporarily set
 
        LocalFsReader {
            vals,
            path: dir_path,
            sec_decoder,
            cache_space: Arc::new(Mutex::new(HashMap::new())),
            _marker_text_data: PhantomData,
        }
    }
}

impl<I, U, F0> OpBase for LocalFsReader<I, U, F0> 
where 
    I: Data,
    U: Data,
    F0: SerFunc(I) -> Vec<ItemE>,
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        } 
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, dep_info),
            _ => panic!("invalid is_shuffle"),
        } 
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_enc: bool, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr, is_enc),
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
        vec![]
    }

    fn get_next_deps(&self) -> Arc<RwLock<HashMap<(OpId, OpId), Dependency>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn is_in_loop(&self) -> bool {
        self.vals.in_loop
    }

    fn number_of_splits(&self) -> usize {
        self.vals.split_num.load(atomic::Ordering::SeqCst)
    }

    fn randomize_in_place(&self, input: *const u8, seed: Option<u64>, num: u64) -> *mut u8 {
        self.randomize_in_place_(input, seed, num)
    }

    fn etake(&self, input: *const u8, should_take: usize, have_take: &mut usize) -> *mut u8 {
        self.take_(input ,should_take, have_take)
    }

    fn iterator_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
            
        self.compute_start(call_seq, input, dep_info)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = U>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = U>;
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

impl<I, U, F0> Op for LocalFsReader<I, U, F0> 
where 
    I: Data,
    U: Data,
    F0: SerFunc(I) -> Vec<ItemE>,
{
    type Item = U;

    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>
    where
        Self: Sized,
    {
        Arc::new(self.clone()) as Arc<dyn Op<Item = Self::Item>>
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn get_cache_space(&self) -> Arc<Mutex<HashMap<(usize, usize), Vec<Vec<Self::Item>>>>> {
        self.cache_space.clone()
    }

    fn compute_start(&self, mut call_seq: NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        //suppose no shuffle will happen after this rdd
        self.narrow(call_seq, input, true)
    }

}


