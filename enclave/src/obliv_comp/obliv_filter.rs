use crate::obliv_comp::*;

pub fn obliv_local_filter<K, V>(mut data: Vec<((K, V), u64)>) -> Vec<((K, V), u64)>
where
    K: Data + Ord,
    V: Data,
{
    let mut len: usize = 0;
    for d in data.iter_mut() {
        d.1 |= MASK_C;
        if is_valid(d) {
            set_field_c(d, len as u64);
            len = len + 1;
        }
    }
    let mut start = 0;
    if len > 0 && len <= data.len() / 2 {
        data.resize(
            (data.len() + len - 1) / len * len,
            ((K::default(), V::default()), MASK_C),
        );
        for i in 0..data.len() / len {
            let (front, end) = data.split_at_mut(i * len);
            let tmp = &mut end[..len];
            let mut cur = 0;
            let mut c_last = start - 1;
            for d in tmp.iter_mut() {
                if get_field_c(d) != MASK_C {
                    c_last = d.1 & MASK_C;
                } else if cur < start {
                    set_field_c(d, cur as u64);
                    cur = cur + 1;
                }
            }
            start = c_last + 1;
            //TODO: use SortHelper
            tmp.sort_by(|a, b| get_field_c(a).cmp(&get_field_c(b)));
            if i > 0 {
                for j in 0..len {
                    if !is_valid(&front[j]) && get_field_c(&tmp[j]) != MASK_C {
                        front[j] = std::mem::take(&mut tmp[j]);
                    }
                }
            }
        }
    } else if len > data.len() / 2 {
        //directly sort data and remove invalid items
        //TODO: use SortHelper
        data.sort_by(|a, b| is_valid(a).cmp(&is_valid(b)).reverse());
    }

    data.truncate(len);
    return data;
}

pub fn obliv_global_filter_stage1<T>(data: Box<dyn Iterator<Item = Vec<(T, u64)>>>) -> (Vec<Vec<(T, u64)>>, usize)
where
    T: Data
{
    let mut invalid_cnt = 0;
    let data = data.into_iter().map(|d| {
        invalid_cnt += d.iter().filter(|&x| !is_valid(x)).count();
        d
    }).collect::<Vec<_>>();
    (data, invalid_cnt)
    //Without OM
    // let mut len = 0;
    // for d in data.iter() {
    //     if !is_valid(d) {
    //         len += 1;
    //     }
    // }
    // return len;
}

fn obliv_global_filter_stage2<T, F>(
    mut data: Vec<Vec<(T, u64)>>,
    cmp_f: F,
    max_value: (T, u64),
    part_id: usize,
    n_out: usize,
    num_invalids: Vec<usize>,
    outer_parallel: usize,
) -> Vec<Vec<(T, u64)>>
where
    T: Data,
    F: FnMut(&(T, u64), &(T, u64)) -> Ordering + Clone,
{
    let data_len = data.iter().map(|x| x.len()).sum::<usize>();
    if data_len == 0 {
        return Vec::new();
    }
    let mut padding_len = calc(n_out, data_len);
    let mut b: usize = (&num_invalids[0..part_id]).iter().sum();
    let mut b_count = vec![0; n_out];
    let mut rng = {
        let mut seed_buf = [0u8; 8];
        rsgx_read_rand(&mut seed_buf).unwrap();
        get_default_rng_from_seed(u64::from_le_bytes(seed_buf))
    };
    for block in data.iter_mut() {
        for d in block.iter_mut() {
            if is_valid(d) {
                set_field_bktid(d, rng.gen::<usize>() % n_out);
            } else {
                set_field_bktid(d, b % n_out);
                b += 1;
            }
            b_count[get_field_bktid(d)] += 1;
        }
    }
    let max_len = b_count.iter().max().unwrap();
    println!(
        "padding len {} compared with max len {}",
        padding_len, max_len
    );
    if *max_len > padding_len {
        println!("padding len {} < max len {}", padding_len, max_len);
        padding_len = *max_len;
    }
    return build_buckets(data, cmp_f, max_value, outer_parallel, padding_len * n_out, n_out);
}

fn obliv_global_filter_stage3<T, F>(data: Vec<Vec<Vec<(T, u64)>>>, cmp_f: F, max_value: (T, u64), max_len_subpart: usize) -> Vec<T>
where
    T: Data,
    F: FnMut(&(T, u64), &(T, u64)) -> Ordering + Clone,
{
    let mut sort_helper = SortHelper::new_with(data, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    return data
        .into_iter()
        .filter(|x| is_valid(x) && !is_dummy(x))
        .map(|x| x.0)
        .collect();
}

pub fn obliv_global_filter_stage2_t<T>(
    data: Vec<Vec<(T, u64)>>,
    part_id: usize,
    n_out: usize,
    num_invalids: Vec<usize>,
    outer_parallel: usize,
) -> Vec<Vec<(T, u64)>>
where
    T: Data,
{
    let cmp_f = |a: &(T, u64), b: &(T, u64)| {
        (is_dummy(a), get_field_bktid(a), !is_valid(a)).cmp(&(is_dummy(b), get_field_bktid(b), !is_valid(b)))
    };
    obliv_global_filter_stage2(data, cmp_f, (Default::default(), 1u64 << DUMMY_BIT | MASK_BUCKET), part_id, n_out, num_invalids, outer_parallel)
}

pub fn obliv_global_filter_stage2_kv<K, V>(
    data: Vec<Vec<((K, V), u64)>>,
    part_id: usize,
    n_out: usize,
    num_invalids: Vec<usize>,
    outer_parallel: usize,
) -> Vec<Vec<((K, V), u64)>>
where
    K: Data + Ord,
    V: Data,
{
    let cmp_f = |a: &((K, V), u64), b: &((K, V), u64)| {
        (is_dummy(a), get_field_bktid(a), !is_valid(a), &a.0.0).cmp(&(is_dummy(b), get_field_bktid(b), !is_valid(b), &b.0.0))
    };
    obliv_global_filter_stage2(data, cmp_f, (Default::default(), 1u64 << DUMMY_BIT | MASK_BUCKET), part_id, n_out, num_invalids, outer_parallel)
}

pub fn obliv_global_filter_stage3_t<T>(data: Vec<Vec<Vec<(T, u64)>>>, max_len_subpart: usize) -> Vec<T>
where
    T: Data,
{
    let cmp_f = |a: &(T, u64), b: &(T, u64)| {
        (is_dummy(a), !is_valid(a)).cmp(&(is_dummy(b), !is_valid(b)))
    };
    obliv_global_filter_stage3(data, cmp_f, (Default::default(), 1u64 << DUMMY_BIT), max_len_subpart)
}

pub fn obliv_global_filter_stage3_kv<K, V>(data: Vec<Vec<Vec<((K, V), u64)>>>, max_len_subpart: usize) -> Vec<(K, V)>
where
    K: Data + Ord,
    V: Data,
{
    let cmp_f = |a: &((K, V), u64), b: &((K, V), u64)| {
        (is_dummy(a), !is_valid(a), &a.0 .0).cmp(&(is_dummy(b), !is_valid(b), &b.0 .0))
    };
    obliv_global_filter_stage3(data, cmp_f, (Default::default(), 1u64 << DUMMY_BIT), max_len_subpart)
}
