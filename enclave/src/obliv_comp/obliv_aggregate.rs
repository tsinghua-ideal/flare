use std::thread::{self, ThreadId, SgxThread};

use crate::obliv_comp::*;
use crate::obliv_comp::obliv_filter::obliv_local_filter;

pub fn obliv_agg_presort<K, V>(
    data: Vec<(Vec<(K, V)>, Vec<bool>)>,
    outer_parallel: usize,
) -> Vec<((K, V), u64)> 
where
    K: Data + Eq + Hash + Ord,
    V: Data,
{
    let cmp_f = |a: &((K, V), u64), b: &((K, V), u64)| {
        (is_dummy(a), &a.0.0, is_valid(a)).cmp(&(is_dummy(b), &b.0.0, is_valid(b)))
    };

    let (mut sub_parts, max_len) = compose_subpart(zip_data_marks(data), outer_parallel, false, cmp_f.clone());

    let mut handlers = Vec::with_capacity(MAX_THREAD);
    let r = sub_parts.len().saturating_sub(1) / MAX_THREAD + 1;
    for _ in 0..MAX_THREAD {
        let mut sub_parts = sub_parts.split_off(sub_parts.len().saturating_sub(r));
        let handler = thread::Builder::new()
            .spawn(move || {
                for sub_part in sub_parts.iter_mut() {
                    sub_part.sort_unstable_by(cmp_f);
                }
                sub_parts
            }).unwrap();
        handlers.push(handler);
    }
    assert!(sub_parts.is_empty());

    let sub_parts = handlers.into_iter().map(|handler| {
        let res = handler.join().unwrap();
        res.into_iter()
    }).flatten().collect::<Vec<_>>();

    //partition sort (local sort), and pad so that each sub partition should have the same number of (K, V)
    let (sorted_data, num_real_elem) = if max_len == 0 {
        (Vec::new(), 0)
    } else {
        let max_value = (Default::default(), 1 << DUMMY_BIT);
        let mut sort_helper = SortHelper::new(sub_parts, max_len, max_value, true, cmp_f);
        sort_helper.sort();
        sort_helper.take()
    };
    assert_eq!(sorted_data.len(), num_real_elem);
    sorted_data
}

pub fn obliv_agg_stage1<K, V, C>(
    mut part: Vec<((K, V), u64)>,
    id: usize,
    should_assign_loc: bool,
    aggregator: &Arc<Aggregator<K, V, C>>,
    partitioner: &Box<dyn Partitioner>,
    seed_seed: u64,
    outer_parallel: usize,
) -> Vec<Vec<ItemE>>
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    C: Data,
{
    let n_out = partitioner.get_num_of_partitions();
    if part.len() == 0 {
        return Vec::new();
    }
    let seed = {
        let mut rng = get_default_rng_from_seed(seed_seed);
        (rng.gen::<u64>(), rng.gen::<u64>(), rng.gen::<u64>(), rng.gen::<u64>())
    };
    let padding_len = calc(n_out, part.len());

    if should_assign_loc {
        for i in 0..part.len() {
            set_field_loc(&mut part[i], i);
        }
    }

    let mut data = Vec::with_capacity(part.len());
    if !part.is_empty() {
        let mut iter = part.into_iter();
        let ((k, v), m) = iter.next().unwrap();
        let mut acc = ((aggregator.create_combiner)(v), m);
        data.push(((k, acc.0.clone()), m));
        for (i, d) in iter.enumerate() {
            //now i+1 is the index of current item
            let is_d_valid = is_valid(&d);
            let ((k, v), m) = d;
            if !is_valid(&acc) || data[i].0 .0 != k {
                acc = ((aggregator.create_combiner)(v), m);
            } else {
                if is_d_valid {
                    let (mut acc_v, acc_m) = acc;
                    acc_v = (aggregator.merge_value)((acc_v, v));
                    acc = (acc_v, acc_m)
                }
                set_valid(&mut data[i], false);
            }
            data.push(((k, acc.0.clone()), m));
            set_valid(data.last_mut().unwrap(), is_valid(&acc));
        }
    }

    //assume the existence of OM
    let mut cnts = vec![0; n_out];
    let mut rng = {
        let mut seed_buf = [0u8; 8];
        rsgx_read_rand(&mut seed_buf).unwrap();
        get_default_rng_from_seed(u64::from_le_bytes(seed_buf))
    };
    for d in data.iter_mut() {
        let b = if is_valid(d) {  //bucket id
            partitioner.get_partition_with_seed(&d.0.0, seed)
        } else {
            hash(&rng.gen::<u64>()) as usize % n_out
        };
        cnts[b] += 1; //access to OM
        set_field_partid(d, b); //reuse this field
    }
    for cnt in cnts.iter_mut() {
        assert!(padding_len >= *cnt);
        *cnt = padding_len - *cnt;
    }

    //padding with max key
    let last_one = data.pop().unwrap_or(Default::default());
    let mut tmp = ((last_one.0 .0 .clone(), C::default()), 0);
    if should_assign_loc {
        set_field_loc(&mut tmp, MASK_LOC as usize);
    }
    data.resize(padding_len * n_out - 1, tmp.clone());
    data.push(last_one);

    //adjust the part_id for padding one
    let mut cur_bucket = n_out - 1;
    for d in data.iter_mut().rev() {
        if d.1 == tmp.1 {
            if cnts[cur_bucket] == 0 && cur_bucket == 0 {
                break;  //use cmov to make the loop body oblivious
            } else if cnts[cur_bucket] == 0 { 
                cur_bucket -= 1;
            }
            set_field_partid(d, cur_bucket);
            cnts[cur_bucket] -= 1;
        }
    }

    let cmp_f = |x: &((K, C), u64), y: &((K, C), u64)| {
        (is_dummy(x), get_field_partid(x), &x.0 .0, is_valid(x)).cmp(&(
            is_dummy(y),
            get_field_partid(y),
            &y.0 .0,
            is_valid(y),
        ))
    };
    let max_value = (Default::default(), 1 << DUMMY_BIT);
    let (sub_parts, max_len) = split_with_interval(data, CACHE_LIMIT/outer_parallel, cmp_f); 
    let mut sort_helper = SortHelper::new(sub_parts, max_len, max_value, true, cmp_f);
    sort_helper.sort();
    let (mut data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    let mut buckets_enc = create_enc();
    for bucket in data.chunks_mut(padding_len) {
        assert_eq!(bucket.len(), padding_len);
        for d in bucket.iter_mut() {
            set_field_partid(d, id);
        }
        let bucket_enc = batch_encrypt(&bucket, true);
        crate::ALLOCATOR.set_switch(true);
        buckets_enc.push(bucket_enc);
        crate::ALLOCATOR.set_switch(false);
    }

    assert_eq!(buckets_enc.len(), n_out);
    return buckets_enc;
}

pub fn obliv_agg_stage2<K, V, C>(
    data: Vec<Vec<Vec<((K, C), u64)>>>,
    max_len_subpart: usize,
    should_back_up: bool,
    should_remove_dummy: bool,
    aggregator: &Arc<Aggregator<K, V, C>>,
) -> Vec<((K, C), u64)>
where
    K: Data + Eq + Hash + Ord,
    V: Data,
    C: Data,
{
    let cmp_f = |a: &((K, C), u64), b: &((K, C), u64)| {
        (is_dummy(a), &a.0 .0, is_valid(a)).cmp(&(is_dummy(b), &b.0 .0, is_valid(b)))
    };

    let max_value = (Default::default(), 1 << DUMMY_BIT);
    let mut sort_helper = SortHelper::new_with(data, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    if should_back_up {
        //reserve for join split & replication
    }

    let mut part = Vec::with_capacity(data.len());
    if !data.is_empty() {
        let mut iter = data.into_iter();
        let ((k, c), m) = iter.next().unwrap();
        let mut acc = (c.clone(), m);
        part.push(((k, c), m));

        for (i, d) in iter.enumerate() {
            //now i+1 is the index of current item
            let is_d_valid = is_valid(&d);
            let ((k, c), m) = d;
            if !is_valid(&acc) || part[i].0 .0 != k {
                acc = (c, m);
            } else {
                if is_d_valid {
                    let (mut acc_c, acc_m) = acc;
                    acc_c = (aggregator.merge_combiners)((acc_c, c));
                    acc = (acc_c, acc_m)
                }
                set_valid(&mut part[i], false);
            }
            part.push(((k, acc.0.clone()), m));
            set_valid(part.last_mut().unwrap(), is_valid(&acc));
        }
    }

    if should_remove_dummy {
        return obliv_local_filter(part);
    } else {
        return part;
    }
}