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

#[test]
fn in_table_not_found() {
    testcases::in_table_not_found(TestPostresDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn out_table_not_found() {
    testcases::out_table_not_found(TestPostresDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn out_table_is_not_empty() {
    testcases::out_table_is_not_empty(TestPostresDatabase::new(), TestPostresDatabase::new());
}
