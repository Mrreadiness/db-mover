mod common;

use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testable_database::TestableDatabase;
use pretty_assertions::{assert_eq, assert_ne};

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
    in_db.fill_test_table("test", 10);
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

#[rstest]
#[case("bigint", "9223372036854775800", "9223372036854775800")]
#[case("integer", "2147483647", "2147483647")]
#[case("smallint", "32767", "32767")]
// Expected side effect of f32 to f64 conversion
// while writing to the sqlite
#[case("real", "123.12345", "123.12345123291")]
#[case("double precision", "123.12345678", "123.12345678")]
#[case("varchar(10)", "'test'", "test")]
#[case("char(10)", "'test'", "test")]
#[case("bpchar", "'test'", "test")]
#[case("text", "'test'", "test")]
#[case("bytea", "cast('test' as BLOB)", "test")]
#[case("timestamp", "'2004-10-19 10:23:54'", "2004-10-19 10:23:54")]
fn sqlite_types_compatability(
    #[case] type_name: &str,
    #[case] value: &str,
    #[case] expected: &str,
) {
    let in_db = TestSqliteDatabase::new();
    let out_db = TestSqliteDatabase::new();
    let create_table_query = format!("CREATE TABLE test (field {type_name})");
    in_db.conn.execute(&create_table_query, []).unwrap();
    out_db.conn.execute(&create_table_query, []).unwrap();

    in_db
        .conn
        .execute("INSERT INTO test VALUES (NULL)", [])
        .unwrap();
    let insert_query = format!("INSERT INTO test VALUES ({value})");
    in_db.conn.execute(&insert_query, []).unwrap();

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    db_mover::run(args.clone()).unwrap();

    let mut expected = vec![None, Some(expected.to_string())];
    expected.sort();

    let mut stmt = out_db
        .conn
        .prepare("SELECT CAST(field as TEXT) FROM test")
        .unwrap();
    let mut result: Vec<Option<String>> = stmt
        .query_map([], |row| row.get::<_, Option<String>>(0))
        .unwrap()
        .map(|res| res.unwrap())
        .collect();
    result.sort();
    assert_eq!(expected, result);
}

#[rstest]
#[case("bigint", "9223372036854775800", "9223372036854775800")]
#[case("integer", "2147483647", "2147483647")]
#[case("smallint", "32767", "32767")]
#[case("real", "123.12345", "123.12345")]
#[case("double precision", "123.12345678", "123.12345678")]
#[case("varchar(10)", "'test'", "test")]
#[case("char(10)", "'test'", "test")]
#[case("bpchar", "'test'", "test")]
#[case("text", "'test'", "test")]
#[case("bytea", "'test'", "\\x74657374")] // Hex output
#[case("timestamp", "'2004-10-19 10:23:54'", "2004-10-19 10:23:54")]
fn postgres_types_compatability(
    #[case] type_name: &str,
    #[case] value: &str,
    #[case] expected: &str,
) {
    let mut in_db = TestPostresDatabase::new();
    let mut out_db = TestPostresDatabase::new();
    let create_table_query = format!("CREATE TABLE test (field {type_name})");
    in_db.client.execute(&create_table_query, &[]).unwrap();
    out_db.client.execute(&create_table_query, &[]).unwrap();

    in_db
        .client
        .execute("INSERT INTO test VALUES (NULL)", &[])
        .unwrap();
    let insert_query = format!("INSERT INTO test VALUES ({value})");
    in_db.client.execute(&insert_query, &[]).unwrap();

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    db_mover::run(args.clone()).unwrap();

    let mut expected = vec![None, Some(expected.to_string())];
    expected.sort();
    let mut result: Vec<Option<String>> = out_db
        .client
        .query("SELECT CAST(field as TEXT) FROM test", &[])
        .unwrap()
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect();
    result.sort();
    assert_eq!(expected, result);
}
