mod common;

use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testable_database::TestableDatabase;
use pretty_assertions::assert_eq;

use rstest::rstest;

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
