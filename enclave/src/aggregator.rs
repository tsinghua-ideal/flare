use std::boxed::Box;
use std::vec::Vec;
use std::marker::PhantomData;
use crate::basic::Data;

// Aggregator for shuffle tasks.
pub struct Aggregator<K: Data, V: Data, C: Data> {
    pub create_combiner: Box<dyn Fn(V) -> C + Send + Sync>,
    pub merge_value: Box<dyn Fn((C, V)) -> C + Send + Sync>,
    pub merge_combiners: Box<dyn Fn((C, C)) -> C + Send + Sync>,
    pub is_default: bool,
    _marker: PhantomData<K>,
}

impl<K: Data, V: Data, C: Data> Aggregator<K, V, C> {
    pub fn new(
        create_combiner: Box<dyn Fn(V) -> C + Send + Sync>,
        merge_value: Box<dyn Fn((C, V)) -> C + Send + Sync>,
        merge_combiners: Box<dyn Fn((C, C)) -> C + Send + Sync>,
    ) -> Self {
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            is_default: false,
            _marker: PhantomData,
        }
    }
}

impl<K: Data, V: Data> Default for Aggregator<K, V, Vec<V>> {
    fn default() -> Self {
        let merge_value = Box::new(|mv: (Vec<V>, V)| {
            let (mut buf, v) = mv;
            buf.push(v);
            buf
        });
        let create_combiner = Box::new(|v: V| vec![v]);
        let merge_combiners = Box::new(|mc: (Vec<V>, Vec<V>)| {
            let (mut b1, mut b2) = mc;
            b1.append(&mut b2);
            b1
        });
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            is_default: true,
            _marker: PhantomData,
        }
    }
}
