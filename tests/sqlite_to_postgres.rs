mod common;

use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testcases;

#[test]
fn empty() {
    testcases::empty(TestSqliteDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn one_table() {
    testcases::one_table(TestSqliteDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn multiple_tables() {
    testcases::multiple_tables(TestSqliteDatabase::new(), TestPostresDatabase::new());
}
