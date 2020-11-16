use std::boxed::Box;
use std::marker::PhantomData;
use std::mem::forget;
use std::path::{Path, PathBuf};
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::basic::{Data, Arc as SerArc, Func, SerFunc};
use crate::dependency::Dependency;
use crate::op::{Context, Op, OpE, OpBase, MapPartitions, Mapper, encrypt, decrypt};
use serde_derive::{Deserialize, Serialize};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<O, OE, F, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(I) -> O,
        FE: SerFunc(Vec<O>) -> Vec<OE>,
        FD: SerFunc(Vec<OE>) -> Vec<O>;
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

impl ReaderConfiguration<Vec<u8>> for LocalFsReaderConfig {
    fn make_reader<O, OE, F, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn OpE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(Vec<u8>) -> O,
        FE: SerFunc(Vec<O>) -> Vec<OE>,
        FD: SerFunc(Vec<OE>) -> Vec<O>,
    {
        let reader = LocalFsReader::<Vec<u8>>::new(self, context);
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
        let files_per_executor = Arc::new(
            MapPartitions::new(Arc::new(reader) as Arc<dyn Op<Item = _>>, read_files, fe_mpp, fd_mpp),
        );
        let decoder = Mapper::new(files_per_executor, decoder, fe, fd);
        SerArc::new(decoder)
    }
}

#[derive(Clone)]
pub struct LocalFsReader<T> {
    id: usize,
    path: PathBuf,
    context: Arc<Context>,
    _marker_reader_data: PhantomData<T>,
}

impl<T: Data> LocalFsReader<T> {
    fn new(config: LocalFsReaderConfig, context: Arc<Context>) -> Self {
        let LocalFsReaderConfig {
            dir_path,
        } = config;

        LocalFsReader {
            id: context.new_op_id(),
            path: dir_path,
            context,
            _marker_reader_data: PhantomData,
        }
    }
}

macro_rules! impl_common_lfs_opb_funcs {
    () => {
        fn get_id(&self) -> usize {
            self.id
        }

        fn get_context(&self) -> Arc<Context> {
            self.context.clone()
        }

        fn get_deps(&self) -> Vec<Dependency> {
            vec![]
        }

        fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
            Arc::new(SgxMutex::new(Vec::new()))
        }

        fn iterator(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
            self.compute_start(data_ptr, is_shuffle)
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

    fn compute(&self, data_ptr: *mut u8) -> Box<dyn Iterator<Item = Self::Item>> {
        //TODO decrypt
        let data_enc  = unsafe{ Box::from_raw(data_ptr as *mut Vec<Vec<u8>>) };
        let data = self.get_fd()(*(data_enc.clone()));
        forget(data_enc);
        Box::new(data.into_iter())
    }

    fn compute_start(&self, data_ptr: *mut u8, is_shuffle: u8) -> *mut u8 {
        //suppose no shuffle will happen after this rdd
        self.narrow(data_ptr)
    }

}

impl<T: Data> OpE for LocalFsReader<T> {
    type ItemE = Vec<u8>;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>> {
        let fe = |v: Vec<Self::Item>| {
            let mut ct = Vec::with_capacity(v.len()); 
            for pt in v {
                ct.push(encrypt::<>(pt.as_ref()));
            }
            ct
        }; 
        Box::new(fe) as Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>>
    }

    fn get_fd(&self) -> Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>> {
        let fd = |v: Vec<Self::ItemE>| {
            let mut pt = Vec::with_capacity(v.len());
            for ct in v {
                pt.push(decrypt::<>(ct.as_ref()));
            }
            pt
        };
        Box::new(fd) as Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>>
    }

}

