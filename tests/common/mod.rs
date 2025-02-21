#![allow(dead_code)]
pub mod postgres;
pub mod row;
pub mod sqlite;

use rand::distr::{slice::Choose, Distribution};

fn gen_database_name() -> String {
    let chars = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let chars_dist = Choose::new(&chars).unwrap();
    return chars_dist.sample_iter(&mut rand::rng()).take(10).collect();
}
