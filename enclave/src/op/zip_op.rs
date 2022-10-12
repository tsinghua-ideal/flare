use std::hash::{Hash, Hasher};

use crate::dependency::{
    NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::op::*;

#[derive(Clone)]
pub struct Zipped<T, U> 
where
    T: Data, 
    U: Data, 
{
    pub(crate) vals: Arc<OpVals>,
    pub(crate) next_deps: Arc<RwLock<HashMap<(OpId, OpId), Dependency>>>,
    pub(crate) first: Arc<dyn Op<Item = T>>,
    pub(crate) second: Arc<dyn Op<Item = U>>,
    pub(crate) cache_space: Arc<Mutex<HashMap<(usize, usize), (Vec<Vec<(T, U)>>, Vec<Vec<bool>>)>>>,
}

impl<T, U> Zipped<T, U> 
where
    T: Data, 
    U: Data, 
{
    #[track_caller]
    pub fn new(first: Arc<dyn Op<Item = T>>, 
        second: Arc<dyn Op<Item = U>>) -> Self 
    {
        let mut vals = OpVals::new(first.get_context(), std::cmp::min(first.number_of_splits(), second.number_of_splits()));
        let cur_id = vals.id;
        let first_id = first.get_op_id();
        let second_id = second.get_op_id();
             
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(first_id, cur_id)
            )));
        vals.deps
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(second_id, cur_id)
            )));
        first.get_next_deps().write().unwrap().insert(
            (first_id, cur_id),
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(first_id, cur_id))
            )
        );
        second.get_next_deps().write().unwrap().insert(
            (second_id, cur_id),
            Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(second_id, cur_id))
            )
        );
        
        let vals = Arc::new(vals);
        Zipped {
            vals,
            next_deps: Arc::new(RwLock::new(HashMap::new())),
            first,
            second,
            cache_space: Arc::new(Mutex::new(HashMap::new()))
        }
    }
}

impl<T, U> OpBase for Zipped<T, U> 
where 
    T: Data, 
    U: Data, 
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
            0 | 2 | 12 | 14 => self.free_res_enc(data, marks, is_enc),
            1 | 11 => {
                assert_eq!(marks as usize, 0usize);
                let shuf_dep = self.get_next_shuf_dep(dep_info).unwrap();
                shuf_dep.free_res_enc(data, dep_info, is_enc);
            },
            13 => {
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
        if id == TypeId::of::<dyn Op<Item = (T, U)>>() {
            let x = std::ptr::null::<Self>() as *const dyn Op<Item = (T, U)>;
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

impl<T, U> Op for Zipped<T, U> 
where 
    T: Data, 
    U: Data, 
{
    type Item = (T, U);  
    
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
            2 => {       //zip
                println!("secure_zip ");
                let (first, second) = input.get_enc_data::<(Vec<ItemE>, Vec<ItemE>)>();
                let (first_marks, second_marks) = input.get_enc_marks::<(Vec<ItemE>, Vec<ItemE>)>();
                assert!(first_marks.len() == first.len() && second_marks.len() == second.len());
                assert!(batch_decrypt::<bool>(first_marks, true).is_empty() && batch_decrypt::<bool>(second_marks, true).is_empty());
                let mut cur_f = 0; 
                let mut cur_s = 0;

                let mut acc = create_enc();
                let mut r_f = Vec::new();
                let mut r_s = Vec::new();
                while (cur_f < first.len() || !r_f.is_empty()) && (cur_s < second.len() || !r_s.is_empty()) {
                    let mut f = Vec::new();
                    let mut s = Vec::new();
                    std::mem::swap(&mut f, &mut r_f);
                    std::mem::swap(&mut s, &mut r_s);
                    f.append(&mut ser_decrypt::<Vec<T>>(&first[cur_f].clone()));
                    s.append(&mut ser_decrypt::<Vec<U>>(&second[cur_s].clone()));
                    if f.len() > s.len() {
                        r_f = f.split_off(s.len());
                    } else {
                        r_s = s.split_off(f.len());
                    }
                    let block = f.into_iter().zip(s.into_iter()).collect::<Vec<_>>();
                    merge_enc(&mut acc, &ser_encrypt(&block));
                    cur_f += 1;
                    cur_s += 1;
                }
                (to_ptr(acc), res_enc_to_ptr(Vec::<ItemE>::new()))
            },
            12 | 13 | 14 => {
                self.remove_dummy(call_seq, input, dep_info)
            },
            _ => panic!("Invalid is_shuffle")
        }
    }
}