use rand::Rng;
use std::vec::Vec;

pub(crate) mod bounded_priority_queue;
pub(crate) mod random;

/// Shuffle the elements of a vec into a random order in place, modifying it.
pub(crate) fn randomize_in_place<T, R>(iter: &mut Vec<T>, rand: &mut R)
where
    R: Rng,
{
    for i in (1..(iter.len() - 1)).rev() {
        let idx = rand.gen_range(0, i + 1);
        iter.swap(idx, i);
    }
}
