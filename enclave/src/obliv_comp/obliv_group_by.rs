use crate::obliv_comp::*;

pub fn obliv_group_by_stage1<K>(data: &Vec<((K, i64), u64)>) -> (usize, usize)
where
    K: Data,
{
    let mut a = 0;
    let mut cnt = 0;
    for i in 0..data.len() {
        if is_valid(&data[i]) {
            a = std::cmp::max(a, data[i].0 .1);
            cnt += data[i].0 .1;
        }
    }
    return (a as usize, cnt as usize);
}

pub fn obliv_group_by_stage2<K>(
    data: &mut Vec<((K, i64), u64, u64)>,
    alpha: usize,
    beta: usize,
    n_out: usize,
) -> (usize, usize, usize)
where
    K: Data + Ord + Hash,
{
    let n_out_prime = if alpha as f32 / beta as f32 > 1.0 / n_out as f32 {
        (beta + alpha - 1) / alpha
    } else {
        n_out
    };
    let mut bin_num = 0;
    let mut bin_size = 0;
    for d in data.iter_mut().rev() {
        let (new_bin_num, new_bin_size) =
            next_fit(alpha, is_valid(d), d.0 .1 as usize, bin_num, bin_size, None);
        bin_size = new_bin_size;
        bin_num = new_bin_num;
        set_field_binid(d, bin_num);
    }
    return (bin_num, bin_size, n_out_prime);
}

pub fn obliv_group_by_stage3<K>(
    mut data: Vec<((K, i64), u64, u64)>,
    cache_limit: usize,
    alpha: usize,
    id: usize,
    mut last_bin_info: Vec<(usize, usize)>,
    n_in: usize,
    n_out: usize,
) -> Vec<Vec<((K, i64), u64, u64)>>
where
    K: Data + Ord + Hash,
{
    coordinate_bin_num(&mut data, alpha, id, &mut last_bin_info);
    let cmp_f = |a: &((K, i64), u64, u64), b: &((K, i64), u64, u64)| {
        (is_dummy(a), get_field_partid(a), get_field_loc(a)).cmp(&(is_dummy(b), get_field_partid(b), get_field_loc(b)))
    };
    let (sub_parts, max_len_subpart) = split_with_interval(data, cache_limit, cmp_f);
    let max_value = (Default::default(), 1 << DUMMY_BIT, 0);
    let mut sort_helper = SortHelper::new(sub_parts, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    let mut data_out = vec![Vec::new(); n_in];
    for mut d in data.into_iter() {
        let tmp = get_field_binid(&d) % n_out;
        set_field_binid(&mut d, tmp);
        data_out[get_field_partid(&d)].push(d);
    }
    return data_out;
}

pub fn obliv_group_by_stage4<K, V>(
    mut part: Vec<Vec<((K, V), u64)>>,
    buckets: Vec<Vec<Vec<((K, i64), u64, u64)>>>,
    max_len_subpart: usize,
    beta: usize,
    n_in: usize,
    n_out: usize,
    outer_parallel: usize,
) -> Vec<Vec<((K, V), u64)>>
where
    K: Data + Ord + Hash,
    V: Data,
{
    let f_from_bkt = |a: &mut ((K, V), u64), b: &((K, i64), u64, u64)| {
        set_field_bktid(a, get_field_binid(b));
    };
    let f_from_part = |a: &mut ((K, V), u64), b: &((K, V), u64)| {
        set_field_bktid(a, get_field_bktid(b));
    };
    patch_part_num(&mut part, buckets, max_len_subpart, f_from_bkt, f_from_part);

    let mut lim = (2 * beta + n_in * n_out - 1) / n_in / n_out;
    lim += ((lim as f64).sqrt() * PAD_FACTOR) as usize;

    let cmp_f = |a: &((K, V), u64), b: &((K, V), u64)| {
        (is_dummy(a), get_field_bktid(a), !is_valid(a), &a.0.0).cmp(&(is_dummy(b), get_field_bktid(b), !is_valid(b), &b.0.0))
    };

    let max_value = (Default::default(), 1 << DUMMY_BIT | MASK_BUCKET);
    return build_buckets(
        part,
        cmp_f,
        max_value,
        outer_parallel,
        lim * n_out,
        n_out,
    );
}

pub fn obliv_group_by_stage5<K, V, C>(
    mut buckets: Vec<Vec<Vec<((K, V), u64)>>>,
    max_len_subpart: usize,
    aggregator: &Arc<Aggregator<K, V, C>>,
) -> (Vec<(K, C)>, Vec<bool>)
where
    K: Data + Ord + Hash,
    V: Data,
    C: Data,
{
    let cmp_f = |a: &((K, V), u64), b: &((K, V), u64)| {
        (!is_valid(a), &a.0.0).cmp(&(!is_valid(b), &b.0.0))
    };
    //since the invalid ones will be filtered out, and they are sorted to the end
    //we do no not need to use DUMMY_BIT, directly use the VALID_BIT=0
    let max_value = (Default::default(), 0);
    let mut sort_helper = SortHelper::new_with(buckets, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    let mut iter = data.into_iter();
    let mut cur_item = iter.next();
    let mut groups: Vec<(K, C)> = Vec::new();
    let mut marks = Vec::new();
    while cur_item.is_some() {
        let ((k, v), m) = cur_item.unwrap();
        let m = (m >> VALID_BIT) == 1;
        if let Some(last_group) = groups.last_mut() {
            if last_group.0 == k && marks.last().unwrap() == &m {
                let c = std::mem::take(&mut last_group.1);
                last_group.1 = (aggregator.merge_value)((c, v));
            } else {
                groups.push((k, (aggregator.create_combiner)(v)));
                marks.push(m);
            }
        } else {
            groups.push((k, (aggregator.create_combiner)(v)));
            marks.push(m);
        }
        cur_item = iter.next();
    } 

    return (groups, marks);
}