mod common;

use common::postgres::TestPostresDatabase;
use common::testcases;

#[test]
fn empty() {
    testcases::empty(TestPostresDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn one_table() {
    testcases::one_table(TestPostresDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn multiple_tables() {
    testcases::multiple_tables(TestPostresDatabase::new(), TestPostresDatabase::new());
}
