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

#[test]
fn in_table_not_found() {
    testcases::in_table_not_found(TestSqliteDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn out_table_not_found() {
    testcases::out_table_not_found(TestSqliteDatabase::new(), TestPostresDatabase::new());
}

#[test]
fn out_table_is_not_empty() {
    testcases::out_table_is_not_empty(TestSqliteDatabase::new(), TestPostresDatabase::new());
}
