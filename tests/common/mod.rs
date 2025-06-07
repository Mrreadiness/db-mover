#![allow(dead_code)]
pub mod mysql;
pub mod postgres;
pub mod row;
pub mod sqlite;
pub mod testable_database;

use rand::distr::{Distribution, slice::Choose};

fn gen_database_name() -> String {
    let chars = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let chars_dist = Choose::new(&chars).unwrap();
    return format!(
        "test_{}",
        chars_dist
            .sample_iter(&mut rand::rng())
            .take(10)
            .collect::<String>()
    );
}
