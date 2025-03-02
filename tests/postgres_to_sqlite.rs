use db_mover;

mod common;

use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;

fn create_postgres_db() -> TestPostresDatabase {
    let mut db = TestPostresDatabase::new();
    db.create_test_table("test");
    db.create_test_table("test1");
    return db;
}

fn create_sqlite_db() -> TestSqliteDatabase {
    let db = TestSqliteDatabase::new();
    db.create_test_sqlite("test");
    db.create_test_sqlite("test1");
    return db;
}

#[test]
fn empty() {
    let mut in_db = create_postgres_db();
    let out_db = create_sqlite_db();
    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!("sqlite://{}", out_db.path.to_str().unwrap())),
        table: vec![],
        queue_size: Some(100_000),
    };
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!("sqlite://{}", out_db.path.to_str().unwrap())),
        table: vec!["test".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[test]
fn one_table() {
    let mut in_db = create_postgres_db();
    in_db.fill_test_table("test", 1000);
    let out_db = create_sqlite_db();
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!("sqlite://{}", out_db.path.to_str().unwrap())),
        table: vec!["test".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[test]
fn multiple_tables() {
    let mut in_db = create_postgres_db();
    in_db.fill_test_table("test", 1000);
    in_db.fill_test_table("test1", 100);
    let out_db = create_sqlite_db();
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_ne!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!("sqlite://{}", out_db.path.to_str().unwrap())),
        table: vec!["test".to_owned(), "test1".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}
