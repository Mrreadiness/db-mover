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
#[case("bigserial", "9223372036854775800", "9223372036854775800")]
#[case("serial", "2147483647", "2147483647")]
#[case("smallserial", "32767", "32767")]
// Expected side effect of f32 to f64 conversion
// while writing to the sqlite
#[case("real", "123.12345", "123.12345123291")]
#[case("double precision", "123.12345678", "123.12345678")]
#[case("bool", "true", "1")]
#[case("varchar(10)", "'test'", "test")]
#[case("char(10)", "'test'", "test")]
#[case("bpchar", "'test'", "test")]
#[case("text", "'test'", "test")]
#[case("bytea", "cast('test' as BLOB)", "test")]
#[case(
    "timestamptz",
    "'2004-10-19 10:23:54+00:00'",
    "2004-10-19 10:23:54+00:00"
)]
#[case("timestamp", "'2004-10-19 10:23:54'", "2004-10-19 10:23:54")]
#[case("date", "'2004-10-19'", "2004-10-19")]
#[case("time", "'10:23:54'", "10:23:54")]
#[case("json", r#"'{"test":1}'"#, r#"{"test":1}"#)]
#[case("json", r#"'[{"test":1},{"test":2}]'"#, r#"[{"test":1},{"test":2}]"#)]
#[case("jsonb", r#"'{"test":1}'"#, r#"{"test":1}"#)]
#[case("jsonb", r#"'[{"test":1},{"test":2}]'"#, r#"[{"test":1},{"test":2}]"#)]
#[case(
    "uuid",
    "X'67e5504410b1426f9247bb680e5fe0c8'",
    "67e55044-10b1-426f-9247-bb680e5fe0c8"
)]
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
    let mut result: Vec<Option<String>>;
    if type_name == "uuid" {
        let mut stmt = out_db
            .conn
            .prepare("SELECT CAST(field as BLOB) FROM test")
            .unwrap();
        result = stmt
            .query_map([], |row| row.get::<_, Option<uuid::Uuid>>(0))
            .unwrap()
            .map(|res| res.unwrap().map(|val| val.to_string()))
            .collect();
    } else {
        let mut stmt = out_db
            .conn
            .prepare("SELECT CAST(field as TEXT) FROM test")
            .unwrap();
        result = stmt
            .query_map([], |row| row.get::<_, Option<String>>(0))
            .unwrap()
            .map(|res| res.unwrap())
            .collect();
    }
    result.sort();
    assert_eq!(expected, result);
}

const POSTGRES_NULL_DISABLED_TYPES: [&'static str; 3] = ["smallserial", "serial", "bigserial"];

#[rstest]
#[case("bigint", "9223372036854775800", "9223372036854775800")]
#[case("integer", "2147483647", "2147483647")]
#[case("smallint", "32767", "32767")]
#[case("smallserial", "32767", "32767")]
#[case("serial", "2147483647", "2147483647")]
#[case("bigserial", "9223372036854775807", "9223372036854775807")]
#[case("real", "123.12345", "123.12345")]
#[case("double precision", "123.12345678", "123.12345678")]
#[case("bool", "true", "true")]
#[case("varchar(10)", "'test'", "test")]
#[case("char(10)", "'test'", "test")]
#[case("bpchar", "'test'", "test")]
#[case("text", "'test'", "test")]
#[case("bytea", "'test'", "\\x74657374")] // Hex output
#[case("timestamptz", "'2004-10-19 10:23:54+00'", "2004-10-19 10:23:54+00")]
#[case("timestamp", "'2004-10-19 10:23:54'", "2004-10-19 10:23:54")]
#[case("date", "'2004-10-19'", "2004-10-19")]
#[case("time", "'10:23:54'", "10:23:54")]
#[case("json", r#"'{"test":1}'"#, r#"{"test":1}"#)]
#[case("json", r#"'[{"test":1},{"test":2}]'"#, r#"[{"test":1},{"test":2}]"#)]
#[case("jsonb", r#"'{"test":1}'"#, r#"{"test": 1}"#)]
#[case(
    "jsonb",
    r#"'[{"test":1},{"test":2}]'"#,
    r#"[{"test": 1}, {"test": 2}]"#
)]
#[case(
    "uuid",
    "'67e55044-10b1-426f-9247-bb680e5fe0c8'",
    "67e55044-10b1-426f-9247-bb680e5fe0c8"
)]
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

    let mut expected = vec![Some(expected.to_string())];
    let insert_query = format!("INSERT INTO test VALUES ({value})");
    in_db.client.execute(&insert_query, &[]).unwrap();

    if !POSTGRES_NULL_DISABLED_TYPES.contains(&type_name) {
        in_db
            .client
            .execute("INSERT INTO test VALUES (NULL)", &[])
            .unwrap();
        expected.push(None);
    }

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    db_mover::run(args.clone()).unwrap();

    let mut result: Vec<Option<String>> = out_db
        .client
        .query("SELECT CAST(field as TEXT) FROM test", &[])
        .unwrap()
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect();
    expected.sort();
    result.sort();
    assert_eq!(expected, result);
}
