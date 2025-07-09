mod common;

use std::{thread::sleep, time::Duration};

use common::mysql::TestMysqlDatabase;
use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testable_database::TestableDatabase;
use pretty_assertions::{assert_eq, assert_ne};

use rstest::rstest;
use rstest_reuse::{self, *};

fn create_test_tables(in_db: &mut impl TestableDatabase, out_db: &mut impl TestableDatabase) {
    in_db.create_test_table("test");
    in_db.create_test_table("test1");
    out_db.create_test_table("test");
    out_db.create_test_table("test1");
}

#[template]
#[rstest]
fn all_databases_combinations(
    #[values(
        TestSqliteDatabase::new(),
        TestPostresDatabase::new(),
        TestMysqlDatabase::new_mysql(),
        TestMysqlDatabase::new_mariadb()
    )]
    in_db: impl TestableDatabase,
    #[values(
        TestSqliteDatabase::new(),
        TestPostresDatabase::new(),
        TestMysqlDatabase::new_mysql(),
        TestMysqlDatabase::new_mariadb()
    )]
    out_db: impl TestableDatabase,
) {
}

#[apply(all_databases_combinations)]
fn empty(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
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

#[apply(all_databases_combinations)]
fn one_table(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[apply(all_databases_combinations)]
fn multiple_tables(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 10);
    in_db.fill_test_table("test1", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_ne!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    args.table.push("test1".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[apply(all_databases_combinations)]
fn auto_detect_tables(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 10);
    in_db.fill_test_table("test1", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_ne!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

#[apply(all_databases_combinations)]
fn in_table_not_found(in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    out_db.create_test_table("test");

    assert!(db_mover::run(args.clone()).is_err());
}

#[apply(all_databases_combinations)]
fn out_table_not_found(mut in_db: impl TestableDatabase, out_db: impl TestableDatabase) {
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    in_db.create_test_table("test");

    assert!(db_mover::run(args.clone()).is_err());
}

#[apply(all_databases_combinations)]
fn out_table_is_not_empty(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
    in_db.fill_test_table("test", 10);
    out_db.fill_test_table("test", 10);

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    assert!(db_mover::run(args.clone()).is_err());
}

#[apply(all_databases_combinations)]
fn different_set_of_columnds(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    out_db.execute("ALTER TABLE test ADD COLUMN test_different_columns INTEGER");
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    let result = db_mover::run(args.clone());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("Incompatable set of columns for table \"test\""),
        "{:?}",
        err,
    );
}

#[apply(all_databases_combinations)]
fn nullable_to_non_nullable(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    in_db.execute("CREATE TABLE test (test_different_columns INTEGER)");
    out_db.execute("CREATE TABLE test (test_different_columns INTEGER NOT NULL)");
    in_db.execute("INSERT INTO test VALUES (1)");
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    let result = db_mover::run(args.clone());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("Incompatable set of columns for table \"test\""),
        "{:?}",
        err,
    );
}

#[apply(all_databases_combinations)]
fn non_nullable_to_nullable(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    in_db.execute("CREATE TABLE test (test_different_columns INTEGER NOT NULL)");
    out_db.execute("CREATE TABLE test (test_different_columns INTEGER)");
    in_db.execute("INSERT INTO test VALUES (1)");
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    let result = db_mover::run(args.clone());
    assert!(result.is_ok(), "{:?}", result.unwrap_err());
}

#[apply(all_databases_combinations)]
#[case("SMALLINT", "INTEGER", "1")]
#[case("SMALLINT", "BIGINT", "1")]
#[case("INTEGER", "BIGINT", "1")]
fn safe_type_conversion(
    mut in_db: impl TestableDatabase,
    mut out_db: impl TestableDatabase,
    #[case] in_type: &str,
    #[case] out_type: &str,
    #[case] value: &str,
) {
    in_db.execute(format!("CREATE TABLE test (value {in_type})"));
    out_db.execute(format!("CREATE TABLE test (value {out_type})"));
    in_db.execute(format!("INSERT INTO test VALUES ({value})"));
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    let result = db_mover::run(args.clone());
    assert!(result.is_ok(), "{:?}", result.unwrap_err());

    assert_eq!(
        out_db.query_count(format!("SELECT count(1) FROM test WHERE value = {value}")),
        1
    );
}

#[apply(all_databases_combinations)]
fn safe_type_conversion_float(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    in_db.execute("CREATE TABLE test (value REAL)");
    out_db.execute("CREATE TABLE test (value DOUBLE PRECISION)");
    in_db.execute("INSERT INTO test VALUES (1.1)");
    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    let result = db_mover::run(args.clone());
    assert!(result.is_ok(), "{:?}", result.unwrap_err());

    assert_eq!(
        out_db.query_count("SELECT count(1) FROM test WHERE ABS(value - 1.1) < 0.0001"),
        1
    );
}

#[rstest]
#[case("public", "test_schema")]
#[case("test_schema", "public")]
#[case("test_schema", "test_schema")]
#[case("test_schema", "other_test_schema")]
fn postgres_different_schemas(#[case] in_schema: &str, #[case] out_schema: &str) {
    let mut in_db = TestPostresDatabase::new();
    let mut out_db = TestPostresDatabase::new();
    let in_table = format!("{in_schema}.test");
    let out_table = format!("{out_schema}.test");
    let query = format!("CREATE SCHEMA IF NOT EXISTS {in_schema}");
    in_db.client.execute(&query, &[]).unwrap();

    let query = format!("CREATE SCHEMA IF NOT EXISTS {out_schema}");
    out_db.client.execute(&query, &[]).unwrap();
    in_db.create_test_table(&in_table);
    out_db.create_test_table(&out_table);
    assert_eq!(
        in_db.get_all_rows(&in_table),
        out_db.get_all_rows(&out_table)
    );
    in_db.fill_test_table(&in_table, 10);

    let in_uri = db_mover::uri::URI::Postgres(format!(
        "{}?options=-c%20search_path={in_schema}",
        in_db.uri
    ));
    let out_uri = db_mover::uri::URI::Postgres(format!(
        "{}?options=-c%20search_path={out_schema}",
        out_db.uri
    ));

    let mut args = db_mover::args::Args::new(in_uri, out_uri);
    args.table.push("test".to_string());

    db_mover::run(args).unwrap();

    assert_eq!(
        in_db.get_all_rows(&in_table),
        out_db.get_all_rows(&out_table)
    );
}

#[rstest]
fn postgres_retry_reconnect() {
    let mut in_db = TestPostresDatabase::new();
    let mut out_db = TestPostresDatabase::new();

    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 10);

    let mut trx_client = out_db.new_client();
    let mut trx = trx_client.transaction().unwrap();
    trx.execute("LOCK TABLE test IN EXCLUSIVE MODE", &[])
        .unwrap();

    std::thread::scope(|s| {
        let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
        args.table.push("test".to_string());
        args.batch_write_retries = 6;
        s.spawn(move || {
            db_mover::run(args).unwrap();
        });
        s.spawn(|| {
            // Wait until db_mover blocked by lock
            let mut num_queries_blocked_by_lock = 0_i64;
            while num_queries_blocked_by_lock == 0 {
                num_queries_blocked_by_lock = out_db
                    .client
                    .query_one(
                        "SELECT count(1) FROM pg_stat_activity WHERE wait_event_type = 'Lock'",
                        &[],
                    )
                    .unwrap()
                    .get(0);
                sleep(Duration::from_millis(500));
            }
            // Force disconnect all clients
            trx.execute(
                "SELECT pg_terminate_backend(pid)
                 FROM pg_stat_activity
                 WHERE pid <> pg_backend_pid()
                   AND datname = current_database();",
                &[],
            )
            .unwrap();
            drop(trx) // Release the lock
        });
    });

    out_db.reconect();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
}

#[apply(all_databases_combinations)]
fn dry_run(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 10);

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    args.dry_run = true;
    db_mover::run(args).unwrap();

    assert_eq!(out_db.get_all_rows("test").len(), 0);
}

#[apply(all_databases_combinations)]
fn dry_run_err(mut in_db: impl TestableDatabase, out_db: impl TestableDatabase) {
    in_db.create_test_table("test");
    in_db.fill_test_table("test", 10);

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    args.dry_run = true;
    assert!(db_mover::run(args.clone()).is_err());
}
