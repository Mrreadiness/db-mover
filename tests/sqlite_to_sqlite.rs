mod common;

use common::sqlite::TestSqliteDatabase;
use common::testcases;

#[test]
fn empty() {
    testcases::empty(TestSqliteDatabase::new(), TestSqliteDatabase::new());
}

#[test]
fn one_table() {
    testcases::one_table(TestSqliteDatabase::new(), TestSqliteDatabase::new());
}

#[test]
fn multiple_tables() {
    testcases::multiple_tables(TestSqliteDatabase::new(), TestSqliteDatabase::new());
}
