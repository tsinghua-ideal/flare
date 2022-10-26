use std::boxed::Box;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::vec::Vec;

use deepsize::DeepSizeOf;
use sgx_trts::trts::rsgx_read_rand;
use sgx_types::*;
use rand::Rng;

use crate::aggregator::Aggregator;
use crate::basic::{AnyData, Arc as SerArc, Data, Func, SerFunc};
use crate::partitioner::{hash, Partitioner};
use crate::op::{ser_encrypt, ser_decrypt, ItemE, SortHelper, CACHE_LIMIT, MAX_ENC_BL, MAX_THREAD, compose_subpart, split_with_interval, batch_encrypt, batch_decrypt, create_enc, merge_enc};
use crate::utils::random::get_default_rng_from_seed;

pub mod obliv_filter;
pub mod obliv_aggregate;
pub mod obliv_group_by;
pub mod obliv_join;

pub use obliv_filter::*;
pub use obliv_aggregate::*;
pub use obliv_group_by::*;
pub use obliv_join::*;

pub const PAD_FACTOR: f64 = 20.0;
pub static VALID_BIT: u64 = 63;
static TID_BIT: u64 = 62;
static DUMMY_BIT: u64 = 61; //used for padding during sort
static CNT_BITS: u64 = 46;
static PARTID_BITS: u64 = 61;
static MASK_BIN: u64 = (1 << CNT_BITS) - 1;
static MASK_C: u64 = (1 << CNT_BITS) - 1;
static MASK_LOC: u64 = (1 << CNT_BITS) - 1;
static MASK_PARTID: u64 = (1 << PARTID_BITS) - (1 << CNT_BITS);
static MASK_BUCKET: u64 = (1 << CNT_BITS) - 1;

trait TupleU64<T> {
    fn get_first_u64(&self) -> u64;
    fn get_first_u64_mut(&mut self) -> &mut u64;
}

impl<T> TupleU64<T> for (T, u64) {
    #[inline(always)]
    fn get_first_u64(&self) -> u64 {
        self.1
    }
    #[inline(always)]
    fn get_first_u64_mut(&mut self) -> &mut u64 {
        &mut self.1
    }
}

impl<T> TupleU64<T> for (T, u64, u64) {
    #[inline(always)]
    fn get_first_u64(&self) -> u64 {
        self.1
    }
    #[inline(always)]
    fn get_first_u64_mut(&mut self) -> &mut u64 {
        &mut self.1
    }
}

#[inline(always)]
fn is_valid<U, T>(a: &U) -> bool
where
    U: TupleU64<T>,
{
    (a.get_first_u64() & 1 << VALID_BIT) > 0
}

#[inline(always)]
fn set_valid<U, T>(a: &mut U, b: bool)
where
    U: TupleU64<T>,
{
    *(a.get_first_u64_mut()) =
        (a.get_first_u64() & (1 << VALID_BIT) - 1) | ((b as u64) << VALID_BIT);
}

#[inline(always)]
fn get_field_tid<U, T>(a: &U) -> u64
where
    U: TupleU64<T>,
{
    (a.get_first_u64() >> TID_BIT) & 1
}

#[inline(always)]
fn set_field_tid<U, T>(a: &mut U, b: u64)
where
    U: TupleU64<T>,
{
    *(a.get_first_u64_mut()) =
        (a.get_first_u64() & !(1 << TID_BIT)) | ((b & 1) << TID_BIT);
}

#[inline(always)]
fn is_dummy<U, T>(a: &U) -> bool
where
    U: TupleU64<T>,
{
    (a.get_first_u64() & 1 << DUMMY_BIT) > 0
}

#[inline(always)]
fn set_dummy<U, T>(a: &mut U, b: bool)
where
    U: TupleU64<T>,
{
    *(a.get_first_u64_mut()) =
        (a.get_first_u64() & !(1 << DUMMY_BIT)) | ((b as u64) << DUMMY_BIT);
}

#[inline(always)]
fn get_field_partid<U, T>(a: &U) -> usize
where
    U: TupleU64<T>,
{
    ((a.get_first_u64() & MASK_PARTID) >> CNT_BITS) as usize
}

#[inline(always)]
fn set_field_partid<U, T>(a: &mut U, b: usize)
where
    U: TupleU64<T>,
{
    *(a.get_first_u64_mut()) = (a.get_first_u64() & !MASK_PARTID) | (((b as u64) << CNT_BITS) & MASK_PARTID);
}

#[inline(always)]
fn get_field_loc<U, T>(a: &U) -> usize
where
    U: TupleU64<T>,
{
    (a.get_first_u64() & MASK_LOC) as usize
}

#[inline(always)]
fn set_field_loc<U, T>(a: &mut U, b: usize)
where
    U: TupleU64<T>,
{
    *(a.get_first_u64_mut()) = (a.get_first_u64() & !MASK_LOC) | ((b as u64) & MASK_LOC);
}

#[inline(always)]
fn set_field_binid<T>(a: &mut (T, u64, u64), b: usize) {
    (*a).2 = (a.2 & !MASK_BIN) | (b as u64 & MASK_BIN);
}

#[inline(always)]
fn get_field_binid<T>(a: &(T, u64, u64)) -> usize {
    (a.2 & MASK_BIN) as usize
}

#[inline(always)]
fn set_field_c<T>(a: &mut (T, u64), b: u64) {
    (*a).1 = (a.1 & !MASK_C) | (b & MASK_C);
}

#[inline(always)]
fn get_field_c<T>(a: &(T, u64)) -> u64 {
    a.1 & MASK_C
}

#[inline(always)]
fn set_field_bktid<T>(a: &mut (T, u64), b: usize) {
    (*a).1 = (a.1 & !MASK_BUCKET) | (b as u64 & MASK_BUCKET);
}

#[inline(always)]
fn get_field_bktid<T>(a: &(T, u64)) -> usize {
    (a.1 & MASK_BUCKET) as usize
}

fn obliv_place<T, F>(data: &mut Vec<T>, total_len: usize, f: F, padding_value: T)
where
    T: Data,
    F: Fn(&T) -> usize,
{
    let mut len = 1usize << ((total_len as f64).log2().ceil() as u32 - 1);
    let pv = f(&padding_value);
    data.resize(total_len, padding_value);
    while len >= 1 {
        for i in (0..total_len - len).rev() {
            let p = f(&data[i]);
            if p >= i + len && p != pv {
                data.swap(i, i + len);
            }
        }
        len >>= 1;
    }
}

fn obliv_place_rev<T, F>(data: &mut Vec<T>, total_len: usize, f: F, padding_value: T)
where
    T: Data,
    F: Fn(&T) -> usize,
{
    let mut len = 1;
    data.resize(total_len, padding_value);

    while len < total_len {
        for i in len..total_len {
            let p = f(&data[i]);
            if p < i && ((i - p) & len) > 0 {
                data.swap(i, i - len);
            }
        }
        len <<= 1;
    }
}

fn calc(k: usize, mut n: usize) -> usize {
    if k == 1 {
        return n;
    }
    n = std::cmp::max(k, n);
    let n: f64 = n as f64;
    let k: f64 = k as f64;
    let tmp = (k.ln() - 0.5 * k.ln().ln()) * k / n;
    let mut x: f64 = tmp;
    let mut d: f64 = tmp;
    for i in 0..10 {
        if (1. + x - d) * (1. + x - d).ln() - x + d > tmp {
            x -= d;
        }
        d /= 2.;
    }
    return ((1. + x) * n / k + (0.5 * n / (k * k.ln())).sqrt() * 20.).ceil() as usize;
}

fn build_buckets<T, F>(
    data: Vec<Vec<(T, u64)>>,
    cmp_f: F,
    max_value: (T, u64),
    outer_parallel: usize,
    total_len: usize,
    n: usize,
) -> Vec<Vec<(T, u64)>>
where
    T: Data,
    F: FnMut(&(T, u64), &(T, u64)) -> Ordering + Clone,
{
    let (sub_parts, max_len) = compose_subpart(Box::new(data.into_iter()), outer_parallel, true, cmp_f.clone());
    let mut sort_helper = SortHelper::new(sub_parts, max_len, max_value.clone(), true, cmp_f.clone());
    sort_helper.sort();
    let (mut data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    let sep = total_len / n;
    assert_ne!(sep, 0);
    assert_eq!(total_len % n, 0);
    if data.len() > 0 {
        let mut b_last = usize::MAX;
        let mut loc = usize::MAX;
        for d in data.iter_mut() {
            let b_cur = get_field_bktid(d);
            if b_cur != MASK_BUCKET as usize {
                let c = b_cur != b_last;
                b_last = get_field_bktid(d);
                if c {
                    loc = sep * b_last;
                }
                set_field_bktid(d, loc);
                loc += 1;
            }
        }
    }
    obliv_place(&mut data, total_len, get_field_bktid, max_value);
    let mut buckets = vec![Vec::new(); n];
    let mut i = n - 1;
    while data.len() > 0 {
        buckets[i] = data.split_off(data.len() - sep)
            .into_iter()
            .map(|mut x| {
                set_field_bktid(&mut x, 0);
                set_dummy(&mut x, false);
                x
            }).collect::<Vec<_>>();      
        i -= 1;
    }
    return buckets;
}

fn next_fit(
    cap: usize,
    c: bool,
    w: usize,
    mut bin_num: usize,
    mut bin_size: usize,
    n: Option<usize>,
) -> (usize, usize) {
    let n = n.unwrap_or(bin_num + 1);
    let tmp = bin_size + w;
    if c && tmp <= cap {
        bin_size = tmp;
    }
    if c && tmp > cap {
        bin_num = n;
        bin_size = w;
    }
    return (bin_num, bin_size);
}

fn coordinate_bin_num<T>(
    data: &mut Vec<(T, u64, u64)>,
    cap: usize,
    id: usize,
    last_bin_info: &mut Vec<(usize, usize)>,
) where
    T: Data,
{
    let mut bin_num = last_bin_info[0].0;
    let mut bin_size = last_bin_info[0].1;
    let mut n = bin_num + 1;
    let mut n_last = 0;
    for i in 1..last_bin_info.len() {
        let cur_bin_num = last_bin_info[i].0;
        let (a, b) = next_fit(cap, true, last_bin_info[i].1, bin_num, bin_size, Some(n + cur_bin_num));
        if i == id {
            n_last = n;
        }
        if bin_size + last_bin_info[i].1 > cap {
            n += 1;
        }
        n += cur_bin_num;
        last_bin_info[i].0 = a;
        bin_num = a;
        bin_size = b;
    }
    if data.len() > 0 {
        let tmp = get_field_binid(&data[0]);
        for d in data.iter_mut() {
            if get_field_binid(d) != tmp {
                set_field_binid(d, n_last + get_field_binid(d));
            } else {
                set_field_binid(d, last_bin_info[id].0);
            }
        }
    }
}

fn patch_part_num<K, V, T, F1, F2>(
    part: &mut Vec<Vec<((K, V), u64)>>,
    buckets: Vec<Vec<Vec<(T, u64, u64)>>>,
    max_len_subpart: usize,
    mut f_from_bkt: F1,
    mut f_from_part: F2,
) where
    K: Data + Ord + Hash,
    V: Data,
    T: Data,
    F1: FnMut(&mut ((K, V), u64), &(T, u64, u64)) + Clone,
    F2: FnMut(&mut ((K, V), u64), &((K, V), u64)) + Clone,
{

    // prepare to patch new partition number back to original items
    let cmp_f = |x: &(T, u64, u64), y: &(T, u64, u64)| {
        (is_dummy(x), get_field_partid(x), get_field_loc(x)).cmp(&(is_dummy(y), get_field_partid(y), get_field_loc(y)))
    };
    let max_value = (Default::default(), 1 << DUMMY_BIT, 0);
    let mut sort_helper = SortHelper::new_with(buckets, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);
    
    for d in data.into_iter() {
        if get_field_loc(&d) != MASK_LOC as usize {
            let loc = get_field_loc(&d);
            let part_mut = &mut part[loc/MAX_ENC_BL][loc%MAX_ENC_BL];
            f_from_bkt(part_mut, &d);
        }
    }

    let last_item = part.last().map(|x| x.last()).flatten();
    if last_item.is_some() {
        let mut last_d = last_item.unwrap().clone();
        for bl in part.iter_mut().rev() {
            for d in bl.iter_mut().rev() {
                if d.0.0 == last_d.0.0 {
                    f_from_part(d, &last_d);
                }
                last_d = d.clone();
            }
        }
    }
}

pub fn zip_data_marks<K, V>(data: Vec<(Vec<(K, V)>, Vec<bool>)>) -> Box<dyn Iterator<Item = Vec<((K, V), u64)>>>
where
    K: Data + Ord,
    V: Data,
{
    Box::new(data.into_iter().map(|(bl, mut blmarks)| {
        if blmarks.is_empty() {
            blmarks.resize(bl.len(), true);
        }
        assert_eq!(bl.len(), blmarks.len());
        bl.into_iter().zip(blmarks.into_iter().map(|m| (m as u64) << VALID_BIT)).collect::<Vec<_>>()
    }))
}