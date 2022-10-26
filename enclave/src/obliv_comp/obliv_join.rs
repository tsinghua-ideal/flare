use crate::op::read_sorted_bucket;
use crate::obliv_comp::*;

pub fn obliv_join_stage1<K>(
    part_a: Vec<((K, i64), u64)>,
    part_b: Vec<((K, i64), u64)>,
    alpha_a: usize,
    alpha_b: usize,
    beta_a: usize,
    beta_b: usize,
    n_out: usize,
    cache_limit: usize,
) -> (usize, usize, usize, i64, Vec<((K, (i64, i64)), u64, u64)>)
where
    K: Data + Eq + Ord + Hash,
{
    let part = part_a.into_iter().map(|mut d| {
        set_field_tid(&mut d, 0);
        let ((k, v), m) = d;
        ((k, (v, 0)), m)
    }).chain(part_b.into_iter().map(|mut d| {
        set_field_tid(&mut d, 1);
        let ((k, v), m) = d;
        ((k, (0, v)), m)
    })).collect::<Vec<_>>();

    let cmp_f = |a: &((K, (i64, i64)), u64), b: &((K, (i64, i64)), u64)| {
        (is_dummy(a), &a.0.0, get_field_tid(a), is_valid(a)).cmp(&(is_dummy(b), &b.0.0, get_field_tid(b), is_valid(b)))
    };
    let (sub_parts, max_len_subpart) = split_with_interval(part, cache_limit, cmp_f);
    let max_value = (Default::default(), 1 << DUMMY_BIT);
    let mut sort_helper = SortHelper::new(sub_parts, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (mut part, num_elems) = sort_helper.take();
    assert_eq!(part.len(), num_elems);

    let n_out_prime = std::cmp::min(
        (beta_a + beta_b + alpha_a + alpha_b - 1) / (alpha_a + alpha_b),
        n_out,
    );

    //compute sum per group and invalidate group only has A items or B items
    let mut acc = (0, 0);
    for i in 0..part.len() {
        let c = i == 0 || part[i - 1].0 .0 != part[i].0 .0;
        if c && acc.0 * acc.1 > 0 {
            set_valid(&mut part[i - 1], true);
            part[i - 1].0 .1 = acc;
        }
        if c {
            acc = (0, 0);
        }
        if is_valid(&part[i]) {
            acc.0 += part[i].0 .1 .0;
            acc.1 += part[i].0 .1 .1;
        }
        part[i].0 .1 = (0, 0);
        set_valid(&mut part[i], false);
    }
    if !part.is_empty() && acc.0 * acc.1 > 0 {
        set_valid(part.last_mut().unwrap(), true);
        part.last_mut().unwrap().0 .1 = acc;
    }

    let mut data = part.into_iter().map(|((k, v), m)| ((k, v), m, 0)).collect::<Vec<_>>(); 
    let cap = alpha_a + alpha_b;
    let mut bin_num = 0;
    let mut bin_size = 0;
    let mut acc = 0;
    let mut v_a = 0;
    let mut v_b = 0;
    for i in (0..data.len()).rev() {
        //fill the vacant
        let c = i + 1 == data.len() || data[i + 1].0 .0 != data[i].0 .0;
        if c {
            v_a = data[i].0 .1 .0;
            v_b = data[i].0 .1 .1;
        } else {
            data[i].0 .1 = (v_a, v_b);
        }
        //avoid repeated count
        let c0 = i + 1 == data.len() || (data[i + 1].0 .0 != data[i].0 .0 && is_valid(&data[i]));
        let (a, b) = next_fit(
            cap,
            c0,
            (data[i].0 .1 .0 + data[i].0 .1 .1) as usize,
            bin_num,
            bin_size,
            None,
        );
        let is_new_bin = a > bin_num;
        bin_num = a;
        bin_size = b;
        set_field_binid(&mut data[i], bin_num);
        //compute accumulated product for normal items
        let c1 = is_new_bin && i + 1 < data.len();
        let cur_tid = get_field_tid(&data[i]) as i64;
        data[i].0 .1 .0 = 0;  //re-interpret as acc_prod 
        data[i].0 .1 .1 = cur_tid * v_a + (1 - cur_tid) * v_b;   //re-interpret as group cnt in the other table      
        if c0 {
            if c1 {
                data[i + 1].0 .1 .0 = acc;
                acc = v_a * v_b;
            } else {
                acc += v_a * v_b;
            }
        }
    }

    return (bin_num, bin_size, n_out_prime, acc, data);
}

pub fn obliv_join_stage2<K>(
    mut data: Vec<((K, (i64, i64)), u64, u64)>,
    cache_limit: usize,
    alpha: &Vec<usize>,
    id: usize,
    last_bin_info: Vec<(usize, usize, i64)>,
    n_out: usize,
) -> (
    Vec<((K, (i64, i64)), u64, u64)>,
    Vec<(usize, usize, usize)>,
    Vec<(usize, usize)>,
    i64,
    Vec<(usize, usize)>,
)
where
    K: Data + Eq + Ord + Hash,
{
    let cap = alpha[0] + alpha[1];
    let mut last_bin_info_b = last_bin_info.iter().map(|x| (x.0, x.1)).collect::<Vec<_>>();
    coordinate_bin_num(&mut data, cap, id, &mut last_bin_info_b);
    //compute accumulated product for last bins
    let mut j_s = 0;
    let mut j_sl = 0;
    let mut acc = 0;
    for i in 0..last_bin_info.len() {
        if i == 0 || last_bin_info_b[i].0 != last_bin_info_b[i - 1].0 {
            j_s = i;
        }
        if j_s == i && j_s <= id {
            acc = last_bin_info[i].2;
        }
        if j_s != i && j_s <= id {
            acc += last_bin_info[i].2;
        }
        if j_s <= id {
            j_sl = j_s;
        }
    }
    if id != j_sl {
        acc = -acc;
    }

    if !data.is_empty() {
        data[0].0 .1 .0 = acc; //items with non-positive number does not involve 2nd assignment
        //items with the same b share the same a
        for i in 1..data.len() {
            if get_field_binid(&data[i]) == get_field_binid(&data[i - 1]) {
                data[i].0 .1 .0  = data[i - 1].0 .1 .0 ;
            }
        }
    }

    //begin second-level assignment to balance the results after join
    let part = data;
    let mut idx = 0;
    let mut idx_last = -1;
    let num_chunks = part.len()/n_out + (part.len() % n_out > 0) as usize;
    let mut data = (0..n_out * num_chunks).map(|x| (x, usize::MAX, 0)).collect::<Vec<_>>();
    for i in 0..part.len() {
        let c = (i == 0 || get_field_binid(&part[i]) != get_field_binid(&part[i - 1])) && part[i].0 .1 .0  > 0;
        if c {
            data[i] = (i, idx, part[i].0 .1 .0 as usize);
            idx += 1;
        }
    }
    let mut acc_col = (0..n_out).map(|x| (x, 0)).collect::<Vec<_>>();
    let mut cur = 0;
    let mut tmp = (0..n_out).map(|x| (x, usize::MAX, usize::MAX)).collect::<Vec<_>>();
    for chunk in data.chunks_mut(n_out) {
        for d in chunk.iter_mut() {
            if d.1 != usize::MAX {
                d.1 = cur % n_out;
                cur += 1;
            }
        }

        chunk.sort_by(|a, b| a.1.cmp(&b.1));   //use OM to simplify 
        for src in (0..n_out).rev() {
            let dst = chunk[src].1;
            if dst != usize::MAX { 
                chunk.swap(src, dst);         //use OM to simplify
            }
        }
        for j in 0..n_out {
            if chunk[j].1 != usize::MAX && tmp[j].1 == usize::MAX {
                tmp[j].1 = chunk[j].2;  //copy acc
            }
        }
        acc_col.sort_by(|a, b| a.1.cmp(&b.1).reverse()); //descending
        tmp.sort_by(|a, b| a.1.cmp(&b.1)); //ascending
        let c = cur >= n_out;
        if c {
            for j in 0..n_out {
                acc_col[j].1 += tmp[j].1;
                tmp[j].2 = acc_col[j].0; //assign init partition number
            }
        }
        tmp.sort_by(|a, b| a.0.cmp(&b.0));
        cur = cur % n_out;
        for j in 0..n_out {
            if c {
                if j < cur {
                    tmp[j].1 = chunk[j].2;
                } else {
                    tmp[j].1 = usize::MAX;
                }
                chunk[j].2 = tmp[j].2; //patch init partition number
                if part[0].0 .1 .0 > 0 && idx_last == -1 {
                    idx_last = chunk[0].2 as i64; //record original partition number for the last bin
                }
            } else {
                chunk[j].2 = usize::MAX;
            }
        }
    }
    if part[0].0 .1 .0 > 0 && idx_last == -1 {
        idx_last = -2;  //the last bin is in the remainder bins
    }
    let acc_col_rem = tmp.into_iter().map(|d| (d.0, d.1)).collect::<Vec<_>>();

    return (part, data, acc_col, idx_last, acc_col_rem);
}

pub fn obliv_join_stage3(
    acc_col_rec: &mut Vec<(usize, usize)>,
    acc_col_own: &mut Vec<(usize, usize)>,
    last_bin_loc_rec: i64,
    last_bin_loc_own: &mut i64,
    acc_col_rem: Vec<(usize, usize)>,
) {
    let n_out = acc_col_own.len();
    acc_col_rec.sort_by(|a, b| b.1.cmp(&a.1));
    acc_col_own.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(acc_col_rec.len(), acc_col_own.len());
    for i in 0..acc_col_rec.len() {
        acc_col_rec[i].1 += acc_col_own[i].1;
        acc_col_own[i].1 = acc_col_rec[i].0;
    }
    acc_col_own.sort_by_key(|x| x.0);

    let last_bin_loc = *last_bin_loc_own; 
    for s in acc_col_own.iter() {
        let c = last_bin_loc >= 0 && last_bin_loc == s.0 as i64;
        if c {
            *last_bin_loc_own = s.1 as i64; //if last bin is in regular bins
        }
    }
    if last_bin_loc == -2 {
        *last_bin_loc_own = acc_col_rem[0].1 as i64; //if last bin is in remainder bins
    } 
    if last_bin_loc == -1 {
        *last_bin_loc_own = last_bin_loc_rec;
    }
}

pub fn obliv_join_stage4<K>(
    mut part: Vec<((K, (i64, i64)), u64, u64)>,
    mut data: Vec<(usize, usize, usize)>,
    acc_col: Vec<(usize, usize)>,
    last_bin_loc: i64,
    acc_col_rem: Vec<(usize, usize)>,
    n_in: usize,
    cache_limit: usize,
) -> Vec<Vec<ItemE>>
where
    K: Data + Eq + Ord + Hash,
{
    let n_out = acc_col.len();
    // prepare the patch of new partition numbers
    let mut cur = 0;
    for i in 0..data.len() {
        if data[i].1 != usize::MAX {
            cur = (cur + 1) % n_out;
        }
    }
    let mut tmp = acc_col_rem;
    for chunk in data.chunks_mut(n_out).rev() {
        let mut buf = chunk.to_vec();
        let is_full_chunk = buf.iter().filter(|x| x.2 == usize::MAX).count() == 0;
        let last_cur = cur;
        for j in (0..n_out).rev() {
            if chunk[j].1 != usize::MAX && chunk[j].1 < last_cur {
                chunk[j].2 = tmp[j].1; //assign partition number to current chunk
                cur -= 1;
            }
            buf[j].1 = j;
        }
        buf.sort_by(|a, b| a.2.cmp(&b.2));
        for (b, a) in buf.iter_mut().zip(acc_col.iter()) {
            b.2 = a.1;
        }
        buf.sort_by(|a, b| a.1.cmp(&b.1));
        if cur == 0 && is_full_chunk {
            cur = n_out;
        }
        for j in (0..n_out).rev() {
            if is_full_chunk {
                tmp[j].1 = buf[j].2;  //update tmp for patching next chunk
            }
            if chunk[j].1 != usize::MAX && chunk[j].1 >= last_cur {
                chunk[j].2 = tmp[j].1; //assign partition number to next chunk
                cur -= 1;
            }
        }
        chunk.sort_by(|a, b| a.0.cmp(&b.0));
    }
    // assign new partition number
    set_field_binid(&mut part[0], last_bin_loc as usize);
    for i in 1..part.len() {
        if data[i].1 != usize::MAX {
            assert!(data[i].2 < n_out);
            set_field_binid(&mut part[i], data[i].2); //data[i].2 is definitely smaller than n_out
        } else {
            let tmp = get_field_binid(&part[i - 1]);
            set_field_binid(&mut part[i], tmp);
        }
    }
    // prepare to patch new partition number back to original items
    let cmp_f = |x: &((K, (i64, i64)), u64, u64), y: &((K, (i64, i64)), u64, u64)| {
        (is_dummy(x), get_field_tid(x), get_field_partid(x), get_field_loc(x)).cmp(&(is_dummy(y), get_field_tid(y), get_field_partid(y), get_field_loc(y)))
    };
    let (sub_parts, max_len_subpart) = split_with_interval(part, cache_limit, cmp_f);
    let max_value = (Default::default(), 1 << DUMMY_BIT, 0);
    let mut sort_helper = SortHelper::new(sub_parts, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (part, num_elems) = sort_helper.take();
    assert_eq!(part.len(), num_elems);
    crate::ALLOCATOR.set_switch(true);
    let mut buckets_enc =  vec![Vec::new(); n_in * 2];
    crate::ALLOCATOR.set_switch(false);
    for bucket in part.group_by(|x, y| (get_field_tid(x), get_field_partid(x)) == (get_field_tid(y), get_field_partid(y))) {
        let tid = get_field_tid(&bucket[0]) as usize;
        let part_id = get_field_partid(&bucket[0]);
        let bucket_enc = batch_encrypt(bucket, true);
        crate::ALLOCATOR.set_switch(true);
        buckets_enc[tid * n_in + part_id] = bucket_enc;
        crate::ALLOCATOR.set_switch(false);
    }
    return buckets_enc;
}

pub fn obliv_join_stage5<K, V, W>(
    data_set_enc: &Vec<Vec<ItemE>>,
    buckets_set_enc: &Vec<Vec<Vec<ItemE>>>,
    beta: Vec<usize>,
    n_in: usize,
    n_out: usize,
    outer_parallel: usize,
) -> Vec<Vec<((K, (V, W, i64)), u64)>>
where
    K: Data + Eq + Ord + Hash,
    V: Data,
    W: Data,
{
    let mut part_a = data_set_enc[0]
        .iter()
        .map(|block_enc| {
            ser_decrypt::<Vec<(K, V)>>(&block_enc.clone())
                .into_iter()
                .map(|(k, v)| ((k, (v, W::default(), 0)), 0))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut part_b = data_set_enc[1]
        .iter()
        .map(|block_enc| {
            ser_decrypt::<Vec<(K, W)>>(&block_enc.clone())
                .into_iter()
                .map(|(k, w)| ((k, (V::default(), w, 0)), 1 << TID_BIT))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let (buckets_a, max_len_subpart_a) = read_sorted_bucket::<((K, (i64, i64)), u64, u64)>(&buckets_set_enc[0], outer_parallel);
    let (buckets_b, max_len_subpart_b) = read_sorted_bucket::<((K, (i64, i64)), u64, u64)>(&buckets_set_enc[1], outer_parallel);
    let f_from_bkt = |a: &mut ((K, (V, W, i64)), u64), b: &((K, (i64, i64)), u64, u64)| {
        a.0 .1 .2 = b.0 .1 .1;
        set_field_bktid(a, get_field_binid(b));
    };
    let f_from_part = |a: &mut ((K, (V, W, i64)), u64), b: &((K, (V, W, i64)), u64)| {
        a.0 .1 .2 = b.0 .1 .2;
        set_field_bktid(a, get_field_bktid(b));
    };
    patch_part_num(&mut part_a, buckets_a, max_len_subpart_a, f_from_bkt.clone(), f_from_part.clone());
    patch_part_num(&mut part_b, buckets_b, max_len_subpart_b, f_from_bkt, f_from_part);
    for block in part_a.iter_mut().chain(part_b.iter_mut()) {
        for d in block.iter_mut() {
            if d.0 .1 .2 != 0 {
                assert!(d.0 .1 .2 > 0);
                set_valid(d, true);
            } else {
                set_field_bktid(d, MASK_BUCKET as usize);
            }
        }
    }

    let mut lim = (2 * (beta[0] + beta[1]) + n_in * n_out - 1) / n_in / n_out;
    lim += ((lim as f64).sqrt() * PAD_FACTOR) as usize;

    let mut part = part_a;
    part.append(&mut part_b);
    let cmp_f = |x: &((K, (V, W, i64)), u64), y: &((K, (V, W, i64)), u64)| {
        (is_dummy(x), get_field_bktid(x), !is_valid(x), get_field_tid(x), &x.0 .0).cmp(&(
            is_dummy(y),
            get_field_bktid(y),
            !is_valid(y),
            get_field_tid(y),
            &y.0 .0,
        ))
    };
    let max_value = (Default::default(), 1 << DUMMY_BIT | MASK_BUCKET);
    let buckets_new = build_buckets(part, cmp_f, max_value, outer_parallel, lim * n_out, n_out);

    return buckets_new;
}

pub fn obliv_join_stage6<K, V, W>(buckets: Vec<Vec<Vec<((K, (V, W, i64)), u64)>>>, max_len_subpart: usize, padding_len: usize, outer_parallel: usize) -> (Vec<(K, (V, W))>, Vec<bool>)
where
    K: Data + Eq + Ord + Hash,
    V: Data,
    W: Data,
{
    let cmp_f = |x: &((K, (V, W, i64)), u64), y: &((K, (V, W, i64)), u64)| {
        (is_dummy(x), !is_valid(x), get_field_tid(x), &x.0 .0).cmp(&(
            is_dummy(y),
            !is_valid(y),
            get_field_tid(y),
            &y.0 .0,
        ))
    };

    let max_value = (Default::default(), 1 << DUMMY_BIT);
    let mut sort_helper = SortHelper::new_with(buckets, max_len_subpart, max_value, true, cmp_f);
    sort_helper.sort();
    let (data, num_elems) = sort_helper.take();
    assert_eq!(data.len(), num_elems);

    if data.is_empty() {
        return (Vec::new(), Vec::new());
    }

    //compute cnt
    let mut last_k = data[0].0 .0.clone(); 
    let mut last_tid = 0;
    let mut cnt = 0;
    let mut data = data.into_iter().map(|d| {
        let is_valid = is_valid(&d);
        let tid = get_field_tid(&d);
        let ((k, (v, w, other)), m) = d;
        if tid == last_tid && k == last_k {
            cnt += 1;
        } else {
            cnt = 1;
        }
        last_k = k.clone();
        last_tid = tid;
        let res = if is_valid && tid == 0 {
            ((k, (v, w, cnt, other)), m)
        } else if is_valid {
            ((k, (v, w, other, cnt)), m)
        } else {
            assert_eq!(other, 0);
            ((k, (v, w, 0, 0)), m)
        };
        res
    }).collect::<Vec<_>>();
    //fill the cnt
    for i in (0..data.len().saturating_sub(1)).rev() {
        if is_valid(&data[i+1]) && get_field_tid(&data[i]) == get_field_tid(&data[i+1]) && data[i].0 .0 == data[i+1].0 .0 {
            assert!(data[i].0 .1 .2 == data[i+1].0 .1 .2 || data[i].0 .1 .3 == data[i+1].0 .1 .3);
            data[i].0 .1 .2 = data[i+1].0 .1 .2;
            data[i].0 .1 .3 = data[i+1].0 .1 .3;
        }
    }

    //oblivious expand, during which reuse the LOC field
    let mut s = 0;
    for i in 0..data.len() {
        let tid = get_field_tid(&data[i]);
        if i != 0 && tid != get_field_tid(&data[i-1]) {
            s = 0;
        }
        let g = data[i].0 .1 .2 as u64 * tid + data[i].0 .1 .3 as u64 * (1 - tid);
        if g != 0 {
            set_field_loc(&mut data[i], s as usize + tid as usize * padding_len);
        } else {
            set_field_loc(&mut data[i], MASK_LOC as usize);
        }
        s += g;
    }

    //oblivious distribute/place
    obliv_place(&mut data, padding_len << 1, get_field_loc, (Default::default(), MASK_LOC));

    //split into table A and table B
    let mut data_b = data.split_off(padding_len); //note that loc in data_b is still added by padding_len
    let mut data_a = data;

    //fill in missing entries
    for (tid, table) in [&mut data_a, &mut data_b].iter_mut().enumerate() {
        let mut alpha = 0;
        for i in 0..table.len() {
            let mut cur_loc = get_field_loc(&table[i]);
            if cur_loc == MASK_LOC as usize && alpha > 0 {
                table[i] = table[i-1].clone();
                alpha -= 1;
            } else {
                if cur_loc != MASK_LOC as usize {
                    cur_loc -= tid * padding_len;
                    set_field_loc(&mut table[i], cur_loc);
                }
                alpha = table[i].0 .1 .2 as usize * tid + table[i].0 .1 .3 as usize * (1 - tid) - 1;
            }
        }
    }
    //align table B with table A
    {
        let cmp_f = |a: &((K, (V, W, i64, i64)), u64), b: &((K, (V, W, i64, i64)), u64)| {
            get_field_loc(a).cmp(&get_field_loc(b))
        };
        let mut b = 0;
        let mut q = 0;
        for j in 1..data_b.len() {
            if data_b[j].0 .1 .2 == 0 || data_b[j].0 .1 .3 == 0 {
                break;
            }
            if data_b[j].0 .0 != data_b[j-1].0 .0 {
                q = 0;
                b = get_field_loc(&data_b[j]) as i64;
            } else {
                q += 1;
                let alpha_a = data_b[j].0 .1 .2;
                let alpha_b = data_b[j].0 .1 .3;
                set_field_loc(&mut data_b[j], (b + q/alpha_a + (q % alpha_a) * alpha_b) as usize);
            }
        }
        let (sub_parts, max_len) = split_with_interval(data_b, CACHE_LIMIT/outer_parallel, cmp_f); 
        let mut sort_helper = SortHelper::new(sub_parts, max_len, (Default::default(), MASK_LOC), true, cmp_f);
        sort_helper.sort();
        let (data, num_elems) = sort_helper.take();
        assert_eq!(data.len(), num_elems);
        data_b = data;
    }

    //join
    let (table, marks): (Vec<_>, Vec<_>) = {
        let ((kp, (vp, _, _, _)), _) = data_a[0].clone(); //in case the default value involved in invalid computation (e.g., /0).
        let ((_, (_, wp, _, _)), _) = data_b[0].clone();
        data_a.into_iter().map(|d| {
            let new_m = is_valid(&d);
            let ((k, (v, _, _, _)), _) = d;
            ((k, v), new_m)
        }).zip(data_b.into_iter().map(|d| {
            let new_m = is_valid(&d);
            let ((k, (_, w, _, _)), _) = d;
            ((k, w), new_m)
        })).map(|(((ka, va), ma), ((kb, wb), mb))| {
            assert_eq!(ma, mb);
            if ma {
                assert_eq!(ka, kb);
                ((ka, (va, wb)), ma)
            } else {
                ((kp.clone(), (vp.clone(), wp.clone())), false)
            }
        }).unzip()
    };

    return (table, marks);
}