use std::any::TypeId;
use std::boxed::Box;
use std::mem;
use std::string::String;
use std::vec::Vec;

// function clone_in_place is unnecessary in untrusted part
#[derive(Debug)]
pub struct SizeBuf {
    vec_buf: Vec<usize>,  //indicate the length of vector
    op_buf: Vec<bool>,   //indicate whether Some() or not
}

impl SizeBuf {
    pub fn new(len: usize) -> Self {
        SizeBuf {
            vec_buf: vec![0; len],
            op_buf: vec![false; len],
        }
    }
}

#[derive(Debug)]
pub struct Idx {
    vec_idx: usize,
    op_idx: usize,
}

impl Idx {
    pub fn new() -> Self {
        Idx { 
            vec_idx: 0, 
            op_idx: 0,
        }
    }
}

pub trait Construct {
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx);
    
    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx);
    //This function should be called at the upper layer
    fn need_recursive(&self) -> bool;

    fn clone_in_place(&mut self, other: &Self);

    fn get_size(&self) -> usize;

    fn get_aprox_size(&self) -> usize;

}

impl<T> Construct for T 
where
    T: Default + Clone + 'static,
{
    default fn send(&self, _buf: &mut SizeBuf, _idx: &mut Idx) {
        ();
    }
    
    default fn recv(&mut self, _buf: &SizeBuf, _idx: &mut Idx) {
        ();
    }

    default fn need_recursive(&self) -> bool {
        false
    }

    default fn clone_in_place(&mut self, other: &Self) {
        self.clone_from(other);
    }

    default fn get_size(&self) -> usize {
        mem::size_of::<T>()
    }

    default fn get_aprox_size(&self) -> usize {
        mem::size_of::<T>()
    }

}

impl<T> Construct for Option<T>
where
    T: Clone + Construct + Default + 'static,
{
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        let probe: T = Default::default();
        if !probe.need_recursive() {
            return;
        }
        idx.op_idx += 1;
        match &self {
            Some(value) => {
                buf.op_buf[idx.op_idx] = true;
                value.send(buf, idx);
            },
            None => {
                buf.op_buf[idx.op_idx] = false;
            },
        };
    }
    
    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        if !self.need_recursive() {
            return;
        }
        idx.op_idx += 1;
        let is_some = buf.op_buf[idx.op_idx];
        if is_some {
            let mut value: T = Default::default();
            value.recv(buf, idx);
            *self = Some(value);
        }   
    }

    fn need_recursive(&self) -> bool {
        let probe: T = Default::default();
        return probe.need_recursive() 
    }

    fn clone_in_place(&mut self, other: &Self) {
        let probe: T = Default::default();
        if probe.need_recursive() {
            match self {
                Some(value) => value.clone_in_place(other.as_ref().unwrap()),
                None => (),
            }
        } else {
            self.clone_from(&other);
        }
    }

    fn get_size(&self) -> usize {
        let size_option = mem::size_of::<Option<T>>();
        let probe: T = Default::default();
        if probe.need_recursive() {
            match &self {
                Some(value) => size_option + value.get_size(),
                None => size_option,
            }
        } else {
            size_option
        }
    }

    fn get_aprox_size(&self) -> usize {
        let size_option = mem::size_of::<Option<T>>();
        let probe: T = Default::default();
        if probe.need_recursive() {
            match &self {
                Some(value) => size_option + value.get_aprox_size(),
                None => size_option,
            }
        } else {
            size_option
        }
    }
}

impl<T> Construct for Box<T>
where 
    T: Clone + Construct + Default + 'static,
{

    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        (&**self).send(buf, idx);
    }


    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        (&mut **self).recv(buf, idx);
    }

    fn need_recursive(&self) -> bool {
        let probe: T = Default::default();
        return probe.need_recursive() 
    }

    fn clone_in_place(&mut self, other: &Self) {
        let probe: T = Default::default();
        if probe.need_recursive() {
            (**self).clone_in_place(other);
        } else {
            self.clone_from(&other);
        }
    }

    fn get_size(&self) -> usize {
        let size_box = mem::size_of::<Box<T>>();
        let probe: T = Default::default();
        if probe.need_recursive() {
            size_box + (**self).get_size()
        } else {
            size_box + mem::size_of::<T>()
        }
    }

    fn get_aprox_size(&self) -> usize {
        let size_box = mem::size_of::<Box<T>>();
        let probe: T = Default::default();
        if probe.need_recursive() {
            size_box + (**self).get_aprox_size()
        } else {
            size_box + mem::size_of::<T>()
        }
    }

}

impl<K, V> Construct for (K, V)
where 
    K: Clone + Construct + Default + 'static,
    V: Clone + Construct + Default + 'static,
{
    
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        self.0.send(buf, idx);
        self.1.send(buf, idx);
    }
    
    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        self.0.recv(buf, idx); 
        self.1.recv(buf, idx);
    }

    fn need_recursive(&self) -> bool {
        self.0.need_recursive() || self.1.need_recursive() 
    }

    fn clone_in_place(&mut self, other: &Self) {
        self.0.clone_in_place(&other.0);
        self.1.clone_in_place(&other.1);
    }

    fn get_size(&self) -> usize {
        self.0.get_size() + self.1.get_size()
    }

    fn get_aprox_size(&self) -> usize {
        self.0.get_aprox_size() + self.1.get_aprox_size()
    }

}

impl<K, V, W> Construct for (K, V, W)
where 
    K: Clone + Construct + Default + 'static,
    V: Clone + Construct + Default + 'static,
    W: Clone + Construct + Default + 'static,
{
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        self.0.send(buf, idx);
        self.1.send(buf, idx);
        self.2.send(buf, idx);
    }

    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        self.0.recv(buf, idx);
        self.1.recv(buf, idx);
        self.2.recv(buf, idx);
    }

    fn need_recursive(&self) -> bool {
        self.0.need_recursive() || 
        self.1.need_recursive() ||
        self.2.need_recursive()
    }

    fn clone_in_place(&mut self, other: &Self) {
        self.0.clone_in_place(&other.0);
        self.1.clone_in_place(&other.1);
        self.2.clone_in_place(&other.2);
    }

    fn get_size(&self) -> usize {
        self.0.get_size() + 
        self.1.get_size() + 
        self.2.get_size()
    }

    fn get_aprox_size(&self) -> usize {
        self.0.get_aprox_size() + 
        self.1.get_aprox_size() + 
        self.2.get_aprox_size()
    }
}

impl<A, B, C, D> Construct for (A, B, C, D)
where 
    A: Clone + Construct + Default + 'static,
    B: Clone + Construct + Default + 'static,
    C: Clone + Construct + Default + 'static,
    D: Clone + Construct + Default + 'static,
{
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        self.0.send(buf, idx);
        self.1.send(buf, idx);
        self.2.send(buf, idx);
        self.3.send(buf, idx);
    }

    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        self.0.recv(buf, idx);
        self.1.recv(buf, idx);
        self.2.recv(buf, idx);
        self.3.recv(buf, idx);
    }

    fn need_recursive(&self) -> bool {
        self.0.need_recursive() || 
        self.1.need_recursive() ||
        self.2.need_recursive() ||
        self.3.need_recursive()
    }

    fn clone_in_place(&mut self, other: &Self) {
        self.0.clone_in_place(&other.0);
        self.1.clone_in_place(&other.1);
        self.2.clone_in_place(&other.2);
        self.3.clone_in_place(&other.3);
    }

    fn get_size(&self) -> usize {
        self.0.get_size() + 
        self.1.get_size() + 
        self.2.get_size() + 
        self.3.get_size()
    }

    fn get_aprox_size(&self) -> usize {
        self.0.get_aprox_size() + 
        self.1.get_aprox_size() + 
        self.2.get_aprox_size() +
        self.3.get_aprox_size()
    }
}

impl<T> Construct for Vec<T> 
where T: Clone + Construct + Default + 'static
{
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        idx.vec_idx += 1;
        buf.vec_buf[idx.vec_idx] = self.len();
        let probe: T = Default::default();
        if probe.need_recursive() {
            for i in self.iter() {
                i.send(buf, idx);
            }
        }
    }

    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        idx.vec_idx += 1;
        let len = buf.vec_buf[idx.vec_idx];
        self.resize_with(len, Default::default);
        let probe: T = Default::default();
        if probe.need_recursive() {
            for i in self.iter_mut() {
                i.recv(buf, idx);
            }
        }
    }

    fn need_recursive(&self) -> bool {
        true
    }

    fn clone_in_place(&mut self, other: &Self) {
        let probe: T = Default::default();
        if probe.need_recursive() && TypeId::of::<String>() != TypeId::of::<T>() {
            for (idx, i) in self.iter_mut().enumerate() {
                i.clone_in_place(&other[idx]);
            }
        } else {
            self.clone_from(&other);
        }
    }

    fn get_size(&self) -> usize {
        let size_vec = mem::size_of::<Vec<T>>();
        let probe: T = Default::default();
        if probe.need_recursive() {
            let mut acc = 0; 
            for i in self.iter() {
                acc += i.get_size();
            }
            size_vec + acc
        } else {
            size_vec + mem::size_of::<T>() * self.len()
        }
    }

    fn get_aprox_size(&self) -> usize {
        let size_vec = mem::size_of::<Vec<T>>();
        let len = self.len();
        if len == 0 {
            return size_vec
        }
        let probe: T = Default::default();
        if probe.need_recursive() {
            size_vec + len * (self[0].get_aprox_size() + self[len-1].get_aprox_size() + self[(len-1)/2].get_aprox_size()) / 3
        } else {
            size_vec + len * mem::size_of::<T>()
        }
    }
}

impl Construct for String
{
    fn send(&self, buf: &mut SizeBuf, idx: &mut Idx) {
        idx.vec_idx += 1;
        buf.vec_buf[idx.vec_idx] = self.len();
    }

    fn recv(&mut self, buf: &SizeBuf, idx: &mut Idx) {
        idx.vec_idx += 1;
        let len = buf.vec_buf[idx.vec_idx];
        *self = String::with_capacity(len);
    }

    fn need_recursive(&self) -> bool {
        true
    }

    fn clone_in_place(&mut self, other: &Self) {
        self.clone_from(&other);
    }

    fn get_size(&self) -> usize {
        let size_string = mem::size_of::<String>();
        size_string + self.capacity()
    }

    fn get_aprox_size(&self) -> usize {
        let size_string = mem::size_of::<String>();
        size_string + self.capacity()
    }

}