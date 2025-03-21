mod common;

use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testable_database::TestableDatabase;

use rstest::rstest;

fn create_test_tables(in_db: &mut impl TestableDatabase, out_db: &mut impl TestableDatabase) {
    in_db.create_test_table("test");
    in_db.create_test_table("test1");
    out_db.create_test_table("test");
    out_db.create_test_table("test1");
}

#[rstest]
#[case(TestSqliteDatabase::new(), TestSqliteDatabase::new())]
#[case(TestPostresDatabase::new(), TestPostresDatabase::new())]
#[case(TestPostresDatabase::new(), TestSqliteDatabase::new())]
#[case(TestSqliteDatabase::new(), TestPostresDatabase::new())]
fn empty(#[case] mut in_db: impl TestableDatabase, #[case] mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[rstest]
#[case(TestSqliteDatabase::new(), TestSqliteDatabase::new())]
#[case(TestPostresDatabase::new(), TestPostresDatabase::new())]
#[case(TestPostresDatabase::new(), TestSqliteDatabase::new())]
#[case(TestSqliteDatabase::new(), TestPostresDatabase::new())]
fn one_table(#[case] mut in_db: impl TestableDatabase, #[case] mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 1000);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[rstest]
#[case(TestSqliteDatabase::new(), TestSqliteDatabase::new())]
#[case(TestPostresDatabase::new(), TestPostresDatabase::new())]
#[case(TestPostresDatabase::new(), TestSqliteDatabase::new())]
#[case(TestSqliteDatabase::new(), TestPostresDatabase::new())]
fn multiple_tables(
    #[case] mut in_db: impl TestableDatabase,
    #[case] mut out_db: impl TestableDatabase,
) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 1000);
    in_db.fill_test_table("test1", 100);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_ne!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    args.table.push("test1".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[rstest]
#[case(TestSqliteDatabase::new(), TestSqliteDatabase::new())]
#[case(TestPostresDatabase::new(), TestPostresDatabase::new())]
#[case(TestPostresDatabase::new(), TestSqliteDatabase::new())]
#[case(TestSqliteDatabase::new(), TestPostresDatabase::new())]
fn in_table_not_found(
    #[case] in_db: impl TestableDatabase,
    #[case] mut out_db: impl TestableDatabase,
) {
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    out_db.create_test_table("test");

    assert!(db_mover::run(args.clone()).is_err());
}

#[rstest]
#[case(TestSqliteDatabase::new(), TestSqliteDatabase::new())]
#[case(TestPostresDatabase::new(), TestPostresDatabase::new())]
#[case(TestPostresDatabase::new(), TestSqliteDatabase::new())]
#[case(TestSqliteDatabase::new(), TestPostresDatabase::new())]
fn out_table_not_found(
    #[case] mut in_db: impl TestableDatabase,
    #[case] out_db: impl TestableDatabase,
) {
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    in_db.create_test_table("test");

    assert!(db_mover::run(args.clone()).is_err());
}

#[rstest]
#[case(TestSqliteDatabase::new(), TestSqliteDatabase::new())]
#[case(TestPostresDatabase::new(), TestPostresDatabase::new())]
#[case(TestPostresDatabase::new(), TestSqliteDatabase::new())]
#[case(TestSqliteDatabase::new(), TestPostresDatabase::new())]
fn out_table_is_not_empty(
    #[case] mut in_db: impl TestableDatabase,
    #[case] mut out_db: impl TestableDatabase,
) {
    create_test_tables(&mut in_db, &mut out_db);
    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
    in_db.fill_test_table("test", 10);
    out_db.fill_test_table("test", 10);

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    assert!(db_mover::run(args.clone()).is_err());
}
