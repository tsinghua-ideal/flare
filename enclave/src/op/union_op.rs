use std::any::TypeId;
use std::boxed::Box;
use std::mem::forget;
use std::raw::TraitObject;
use std::sync::{Arc, SgxMutex};
use std::vec::Vec;
use crate::basic::{Data, Func, SerFunc};
use crate::dependency::{Dependency, OneToOneDependency};
use crate::op::{CacheMeta, Context, NextOpId, Op, OpE, OpBase, OpVals};
use crate::partitioner::Partitioner;
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::custom_thread::PThread;

pub struct Union<T: Data, TE: Data>
{
    ops: Vec<Arc<dyn OpE<Item = T, ItemE = TE>>>,
    vals: Arc<OpVals>,
    next_deps: Arc<SgxMutex<Vec<Dependency>>>,
    part: Option<Box<dyn Partitioner>>,
}

impl<T: Data, TE: Data> Clone for Union<T, TE>
{
    fn clone(&self) -> Self {
        Union {
            ops: self.ops.clone(),
            vals: self.vals.clone(),
            next_deps: self.next_deps.clone(),
            part: self.part.clone(),
        }
    }
}

impl<T: Data, TE: Data> Union<T, TE>
{
    pub(crate) fn new(ops: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> Self {
        let mut vals = OpVals::new(ops[0].get_context());

        for prev in ops {
            let mut prev_ids = prev.get_prev_ids();
            prev_ids.insert(prev.get_id()); 
            vals.deps
                .push(Dependency::NarrowDependency(Arc::new(
                    OneToOneDependency::new(prev_ids.clone())
                )));
            prev.get_next_deps().lock().unwrap().push(
                Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(prev_ids))
                )
            );
        } 

        let vals = Arc::new(vals);
        let part = match Union::has_unique_partitioner(ops) {
            true => ops[0].partitioner(),
            false => None,
        };
        let ops: Vec<_> = ops.iter().map(|op| op.clone().into()).collect();

        Union {
            ops,
            vals,
            next_deps: Arc::new(SgxMutex::new(Vec::<Dependency>::new())),
            part,
        }
    }

    fn has_unique_partitioner(ops: &[Arc<dyn OpE<Item = T, ItemE = TE>>]) -> bool {
        ops.iter()
            .map(|p| p.partitioner())
            .try_fold(None, |prev: Option<Box<dyn Partitioner>>, p| {
                if let Some(partitioner) = p {
                    if let Some(prev_partitioner) = prev {
                        if prev_partitioner.equals((&*partitioner).as_any()) {
                            // only continue in case both partitioners are the same
                            Ok(Some(partitioner))
                        } else {
                            Err(())
                        }
                    } else {
                        // first element
                        Ok(Some(partitioner))
                    }
                } else {
                    Err(())
                }
            })
            .is_ok()
    }

}

impl<T: Data, TE: Data> OpBase for Union<T, TE>
{
    fn build_enc_data_sketch(&self, p_buf: *mut u8, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step0_of_clone(p_buf, p_data_enc, is_shuffle),
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn clone_enc_data_out(&self, p_out: usize, p_data_enc: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 | 1 => self.step1_of_clone(p_out, p_data_enc, is_shuffle), 
            _ => panic!("invalid is_shuffle"),
        }   
    }

    fn call_free_res_enc(&self, res_ptr: *mut u8, is_shuffle: u8) {
        match is_shuffle {
            0 => self.free_res_enc(res_ptr),
            1 => {
                let next_deps = self.get_next_deps().lock().unwrap().clone();
                let shuf_dep = match &next_deps[0] {  //TODO maybe not zero
                    Dependency::ShuffleDependency(shuf_dep) => shuf_dep,
                    Dependency::NarrowDependency(nar_dep) => panic!("dep not match"),
                };
                shuf_dep.free_res_enc(res_ptr);
            },
            _ => panic!("invalid is_shuffle"),
        }
    }

    fn get_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    
    fn get_deps(&self) -> Vec<Dependency> {
        self.vals.deps.clone()
    }
    
    fn get_next_deps(&self) -> Arc<SgxMutex<Vec<Dependency>>> {
        self.next_deps.clone()
    }
    
    fn iterator_start(&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        
		self.compute_start(tid, call_seq, data_ptr, is_shuffle, cache_meta)
    }

    fn __to_arc_op(self: Arc<Self>, id: TypeId) -> Option<TraitObject> {
        if id == TypeId::of::<dyn Op<Item = T>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = T>;
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

impl<T: Data, TE: Data> Op for Union<T, TE>
{
    type Item = T;
        
    fn get_op(&self) -> Arc<dyn Op<Item = Self::Item>> { 
        Arc::new(self.clone())
    }

    fn get_op_base(&self) -> Arc<dyn OpBase> {
        Arc::new(self.clone()) as Arc<dyn OpBase>
    }

    fn compute_start (&self, tid: u64, call_seq: &mut NextOpId, data_ptr: *mut u8, is_shuffle: u8, cache_meta: &mut CacheMeta) -> *mut u8 {
        match is_shuffle {
            0 => {       //No shuffle later
                self.narrow(call_seq, data_ptr, cache_meta)
            },
            1 => {      //Shuffle write
                self.shuffle(call_seq, data_ptr, cache_meta)
            },
            _ => panic!("Invalid is_shuffle"),
        }
    }

    fn compute(&self, call_seq: &mut NextOpId, data_ptr: *mut u8, cache_meta: &mut CacheMeta) -> (Box<dyn Iterator<Item = Self::Item>>, Option<PThread>) {
        let data = unsafe{ Box::from_raw(data_ptr as *mut Vec<Self::Item>) };
        (Box::new(data.into_iter()), None)
    }

}

impl<T: Data, TE: Data> OpE for Union<T, TE>
{
    type ItemE = TE;
    fn get_ope(&self) -> Arc<dyn OpE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        self.ops[0].get_fe()
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        self.ops[0].get_fd()
    }
}


