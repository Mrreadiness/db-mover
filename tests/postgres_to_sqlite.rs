mod common;

use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testcases;

#[test]
fn empty() {
    testcases::empty(TestPostresDatabase::new(), TestSqliteDatabase::new());
}

#[test]
fn one_table() {
    testcases::one_table(TestPostresDatabase::new(), TestSqliteDatabase::new());
}

#[test]
fn multiple_tables() {
    testcases::multiple_tables(TestPostresDatabase::new(), TestSqliteDatabase::new());
}
