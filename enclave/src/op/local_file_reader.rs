use std::boxed::Box;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::basic::{Data, Arc as SerArc};
use crate::dependency::Dependency;
use crate::op::{Context, Op, OpBase, MapPartitions, Mapper};
use serde_derive::{Deserialize, Serialize};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Op<Item = O>>
    where
        O: Data,
        F: Fn(I) -> O + Clone + Send + Sync + 'static;
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
    fn make_reader<F, U>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Op<Item = U>>
    where
        F: Fn(Vec<u8>) -> U + Clone + Send + Sync + 'static,
        U: Data,
    {
        let reader = LocalFsReader::<BytesReader>::new(self, context);
        let read_files = 
            |_part: usize, readers: Box<dyn Iterator<Item = BytesReader>>| {
                Box::new(readers.into_iter().map(|file| file.into_iter()).flatten())
                    as Box<dyn Iterator<Item = _>>
            };
        let files_per_executor = Arc::new(
            MapPartitions::new(Arc::new(reader) as Arc<dyn Op<Item = _>>, read_files),
        );
        let decoder = Mapper::new(files_per_executor, decoder);
        SerArc::new(decoder)
    }
}

impl ReaderConfiguration<PathBuf> for LocalFsReaderConfig {
    fn make_reader<F, U>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Op<Item = U>>
    where
        F: Fn(PathBuf) -> U + Clone + Send + Sync + 'static,
        U: Data,
    {
        let reader = LocalFsReader::<FileReader>::new(self, context);
        let read_files = 
            |_part: usize, readers: Box<dyn Iterator<Item = FileReader>>| {
                Box::new(readers.map(|reader| reader.into_iter()).flatten())
                    as Box<dyn Iterator<Item = _>>
            };
        let files_per_executor = Arc::new(
            MapPartitions::new(Arc::new(reader) as Arc<dyn Op<Item = _>>, read_files),
        );
        let decoder = Mapper::new(files_per_executor, decoder);
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

        fn iterator(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
            self.compute_start(ser_data, ser_data_idx, is_shuffle)
        }
    };
}

impl OpBase for LocalFsReader<BytesReader> {
    impl_common_lfs_opb_funcs!();
}

impl OpBase for LocalFsReader<FileReader> {
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

impl Op for LocalFsReader<BytesReader> {
    type Item = BytesReader;

    impl_common_lfs_op_funcs!();

    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        //TODO
        Box::new(Vec::new().into_iter())
    }

    fn compute_start(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        //TODO
        (Vec::new(),Vec::new())
    }

}

impl Op for LocalFsReader<FileReader> {
    type Item = FileReader;

    impl_common_lfs_op_funcs!();

    fn compute(&self, ser_data: &[u8], ser_data_idx: &[usize]) -> Box<dyn Iterator<Item = Self::Item>> {
        //TODO
        Box::new(Vec::new().into_iter())
    }

    fn compute_start(&self, ser_data: &[u8], ser_data_idx: &[usize], is_shuffle: u8) -> (Vec<u8>, Vec<usize>) {
        //TODO
        (Vec::new(),Vec::new())
    }

}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BytesReader {
    files: Vec<PathBuf>,
}

impl Iterator for BytesReader {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        //TODO
        None
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileReader {
    files: Vec<PathBuf>,
}

impl Iterator for FileReader {
    type Item = PathBuf;
    fn next(&mut self) -> Option<Self::Item> {
        self.files.pop()
    }
}
