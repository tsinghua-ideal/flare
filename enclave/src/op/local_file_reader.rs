use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use crate::op::*;
use serde_derive::{Deserialize, Serialize};

pub trait ReaderConfiguration<I: Data> {
    #[track_caller]
    fn make_reader<O, OE, F, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(I) -> O,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>;
}

pub struct LocalFsReaderConfig {
    dir_path: PathBuf,   //placeholder
}

impl LocalFsReaderConfig {
    pub fn new<T: Into<PathBuf>>(path: T, _secure: bool) -> LocalFsReaderConfig {
        LocalFsReaderConfig {
            dir_path: path.into(),
        }
    }
}

impl ReaderConfiguration<Vec<u8>> for LocalFsReaderConfig {
    fn make_reader<O, OE, F, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(Vec<u8>) -> O,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>,
    {
        let reader = LocalFsReader::<Vec<u8>>::new(self, context);
        insert_opmap(reader.get_op_id(), reader.get_op_base());
        let read_files = 
            |_part: usize, readers: Box<dyn Iterator<Item = Vec<u8>>>| {
                readers
            };
        let fe_mpp = |v: Vec<Vec<u8>>| {
            let mut ct = Vec::with_capacity(v.len()); 
            for pt in v {
                ct.push(encrypt::<>(pt.as_ref()));
            }
            ct
        };
        let fd_mpp = |v: Vec<Vec<u8>>| {
            let mut pt = Vec::with_capacity(v.len());
            for ct in v {
                pt.push(decrypt::<>(ct.as_ref()));
            }
            pt
        };
        reader.get_context().add_num(1);
        let files_per_executor = Arc::new(
            MapPartitions::new(Arc::new(reader) as Arc<dyn Op<Item = _>>, read_files, fe_mpp, fd_mpp),
        );
        insert_opmap(files_per_executor.get_op_id(), files_per_executor.get_op_base());
        files_per_executor.get_context().add_num(1);
        let decoder = Mapper::new(files_per_executor, decoder, fe, fd);
        let decoder = SerArc::new(decoder);
        insert_opmap(decoder.get_op_id(), decoder.get_op_base());
        decoder
    }
}

#[derive(Clone)]
pub struct LocalFsReader<T> {
    id: OpId,
    path: PathBuf,
    context: Arc<Context>,
    num_splits: Arc<AtomicUsize>,
    _marker_reader_data: PhantomData<T>,
}

impl<T: Data> LocalFsReader<T> {
    #[track_caller]
    fn new(config: LocalFsReaderConfig, context: Arc<Context>) -> Self {
        let LocalFsReaderConfig {
            dir_path,
        } = config;
        let loc = Location::caller(); 
        LocalFsReader {
            id: context.new_op_id(loc),
            path: dir_path,
            context,
            num_splits: Arc::new(AtomicUsize::new(0)),
            _marker_reader_data: PhantomData,
        }
    }
}

macro_rules! impl_common_lfs_opb_funcs {
    () => {
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

        fn get_op_id(&self) -> OpId {
            self.id
        }

        fn get_context(&self) -> Arc<Context> {
            self.context.clone()
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
            let num = self.num_splits.load(atomic::Ordering::SeqCst);

            if num != 0 {
                return num;
            } else {
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
                self.num_splits.store(num, atomic::Ordering::SeqCst);
                assert!(num != 0);
                return num;
            } 

        }

        fn randomize_in_place(&self, input: *mut u8, seed: Option<u64>, num: u64) -> *mut u8 {
            self.randomize_in_place_(input, seed, num)
        }

        fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
                
		    self.compute_start(tid, call_seq, data_ptr, dep_info)
        }

        fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
            if id == TypeId::of::<dyn Op<Item = Vec<u8>>>() {
                let x = std::ptr::null::<Self>() as *const dyn Op<Item = Vec<u8>>;
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

    };
}

impl<T: Data> OpBase for LocalFsReader<T> {
    impl_common_lfs_opb_funcs!();
}

macro_rules! impl_common_lfs_op_funcs {
    () => {
        fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>>
        where
            Self: Sized,
        {
            Arc::new(self.clone()) as Arc<dyn Op<Item = Self::Item>>
        }

        fn get_op_base(&self) -> Arc<dyn OpBase> {
            Arc::new(self.clone()) as Arc<dyn OpBase>
        }
    };
}

impl<T: Data> Op for LocalFsReader<T> {
    type Item = Vec<u8>;

    impl_common_lfs_op_funcs!();

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        //TODO decrypt
        let data_enc  = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<u8>>) };
        let data = self.get_fd()(*(data_enc.clone()));
        forget(data_enc);
        (Box::new(data.into_iter()), None)
    }

    fn compute_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, dep_info: &DepInfo) -> *mut u8 {
        //suppose no shuffle will happen after this rdd
        self.narrow(call_seq, data_ptr, dep_info)
    }

}

impl<T: Data> OpE for LocalFsReader<T> {
    type ItemE = Vec<Vec<u8>>;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        let fe = |v: Vec<Self::Item>| {
            let mut ct = Vec::with_capacity(v.len()); 
            for pt in v {
                ct.push(encrypt::<>(pt.as_ref()));
            }
            ct
        }; 
        Box::new(fe) as Box<dyn Func(Vec<Self::Item>)->Self::ItemE>
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        let fd = |v: Self::ItemE| {
            let mut pt = Vec::with_capacity(v.len());
            for ct in v {
                pt.push(decrypt::<>(ct.as_ref()));
            }
            pt
        };
        Box::new(fd) as Box<dyn Func(Self::ItemE)->Vec<Self::Item>>
    }

}


