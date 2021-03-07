use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use crate::op::*;
use serde_derive::{Deserialize, Serialize};

pub trait ReaderConfiguration<I: Data> {
    #[track_caller]
    fn make_reader<O, OE, F, F0, FE, FD>(self, context: Arc<Context>, decoder: Option<F>, sec_decoder: Option<F0>, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(I) -> O,
        F0: SerFunc(I) -> Vec<OE>,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>;
}

pub struct LocalFsReaderConfig {
    dir_path: PathBuf,   //placeholder
}

impl LocalFsReaderConfig {
    pub fn new<T: Into<PathBuf>>(path: T) -> LocalFsReaderConfig {
        LocalFsReaderConfig {
            dir_path: path.into(),
        }
    }
}

impl<I: Data> ReaderConfiguration<I> for LocalFsReaderConfig {
    fn make_reader<O, OE, F, F0, FE, FD>(self, context: Arc<Context>, decoder: Option<F>, sec_decoder: Option<F0>, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(I) -> O,
        F0: SerFunc(I) -> Vec<OE>,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>,
    {
        let reader = LocalFsReader::new(self, context, sec_decoder.clone(), fe.clone(), fd.clone());
        if !reader.get_context().get_is_tail_comp() {
            insert_opmap(reader.get_op_id(), reader.get_op_base());
        }
        let read_files = 
            |_part: usize, readers: Box<dyn Iterator<Item = O>>| {
                readers
            };
        reader.get_context().add_num(1);
        let files_per_executor = Arc::new(
            MapPartitions::new(Arc::new(reader) as Arc<dyn Op<Item = _>>, read_files, fe.clone(), fd.clone()),
        );
        insert_opmap(files_per_executor.get_op_id(), files_per_executor.get_op_base());
        files_per_executor.get_context().add_num(1);
        let decoder = Mapper::new(files_per_executor, Fn!(|v| v), fe, fd);
        let decoder = SerArc::new(decoder);
        insert_opmap(decoder.get_op_id(), decoder.get_op_base());
        decoder
    }
}

#[derive(Clone)]
pub struct LocalFsReader<I, U, UE, F0, FE, FD> 
where
    I: Data,
    U: Data,
    UE: Data,
    F0: Func(I) -> Vec<UE> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    vals: Arc<OpVals>,
    path: PathBuf,
    sec_decoder: Option<F0>,
    fe: FE,
    fd: FD,
    _marker_text_data: PhantomData<I>,
    _marker_data: PhantomData<U>,
    _marker_enc_data: PhantomData<UE>,
}

impl<I, U, UE, F0, FE, FD> LocalFsReader<I, U, UE, F0, FE, FD> 
where
    I: Data,
    U: Data,
    UE: Data,
    F0: Func(I) -> Vec<UE> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    #[track_caller]
    fn new(config: LocalFsReaderConfig, context: Arc<Context>, sec_decoder: Option<F0>, fe: FE, fd: FD) -> Self {
        let LocalFsReaderConfig {
            dir_path,
        } = config;

        let mut num: usize = 0;
        let sgx_status = unsafe { 
            ocall_get_addr_map_len(&mut num)
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] OCALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        let vals = Arc::new(OpVals::new(context, num));
 
        LocalFsReader {
            vals,
            path: dir_path,
            sec_decoder,
            fe,
            fd,
            _marker_text_data: PhantomData,
            _marker_data: PhantomData,
            _marker_enc_data: PhantomData,
        }
    }
}

impl<I, U, UE, F0, FE, FD> OpBase for LocalFsReader<I, U, UE, F0, FE, FD> 
where 
    I: Data,
    U: Data,
    UE: Data,
    F0: SerFunc(I) -> Vec<UE>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
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

    fn call_free_res_enc(&self, res_ptr: *mut u8, dep_info: &DepInfo) {
        match dep_info.dep_type() {
            0 => self.free_res_enc(res_ptr),
            1 => {
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(res_ptr);
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

    fn has_spec_oppty(&self) -> bool {
        true
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

    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
            
        self.compute_start(tid, call_seq, input, dep_info)
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

impl<I, U, UE, F0, FE, FD> Op for LocalFsReader<I, U, UE, F0, FE, FD> 
where 
    I: Data,
    U: Data,
    UE: Data,
    F0: SerFunc(I) -> Vec<UE>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
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

    fn compute(&self, call_seq: &mut NextOpId, input: Input) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        call_seq.fix_split_num();
        let data_enc  = input.get_enc_data::<Vec<UE>>();
        let lower = input.get_lower();
        let upper = input.get_upper();
        assert!(lower.len() == 1 && upper.len() == 1);
        let data = self.batch_decrypt(data_enc[lower[0]..upper[0]].to_vec());
        (Box::new(data.into_iter()), None)
    }

    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, input: Input, dep_info: &DepInfo) -> *mut u8 {
        //suppose no shuffle will happen after this rdd
        self.narrow(call_seq, input, dep_info)
    }

}

impl<I, U, UE, F0, FE, FD> OpE for LocalFsReader<I, U, UE, F0, FE, FD> 
where 
    I: Data,
    U: Data,
    UE: Data,
    F0: SerFunc(I) -> Vec<UE>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    type ItemE = UE;
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


