mod common;

use common::mysql::TestMysqlDatabase;
use common::testable_database::TestableDatabase;
use db_mover::databases::{
    mysql::{MysqlDB, MysqlTypeOptions},
    traits::DBInfoProvider,
};
use mysql::prelude::Queryable;
use pretty_assertions::assert_eq;

use rstest::rstest;

use crate::common::postgres::TestPostresDatabase;

const MYSQL_BYTES_IN: &'static str = "CAST('Hello World' AS BINARY)";
const MYSQL_BYTES_EXPECTED: &'static str = "Hello World";

#[rstest]
#[case("bigint", "9223372036854775800", "9223372036854775800")]
#[case("integer", "2147483647", "2147483647")]
#[case("int", "2147483647", "2147483647")]
#[case("smallint", "32767", "32767")]
#[case("float", "123.123", "123.123")]
#[case("real", "123.12345", "123.12345")]
#[case("double precision", "123.12345678", "123.12345678")]
#[case("double", "123.12345678", "123.12345678")]
#[case("numeric(12, 8)", "123.12345678", "123.12345678")]
#[case("decimal(12, 8)", "123.12345678", "123.12345678")]
#[case("bool", "true", "1")]
#[case("boolean", "true", "1")]
#[case("tinyint(1)", "true", "1")]
#[case("varchar(10)", "'test'", "test")]
#[case("char(10)", "'test'", "test")]
#[case("tinytext", "'test'", "test")]
#[case("text", "'test'", "test")]
#[case("mediumtext", "'test'", "test")]
#[case("longtext", "'test'", "test")]
#[case("timestamp", "'2004-10-19 10:23:54'", "2004-10-19 10:23:54")]
#[case("datetime", "'2004-10-19 10:23:54'", "2004-10-19 10:23:54")]
#[case("datetime", "'1001-10-19 10:23:54'", "1001-10-19 10:23:54")]
#[case("date", "'2004-10-19'", "2004-10-19")]
#[case("time", "'10:23:54'", "10:23:54")]
#[case("json", r#"'{"test":1}'"#, r#"{"test":1}"#)]
#[case("json", r#"'[{"test":1},{"test":2}]'"#, r#"[{"test":1},{"test":2}]"#)]
#[case("binary(11)", MYSQL_BYTES_IN, MYSQL_BYTES_EXPECTED)]
#[case("varbinary(11)", MYSQL_BYTES_IN, MYSQL_BYTES_EXPECTED)]
#[case("tinyblob", MYSQL_BYTES_IN, MYSQL_BYTES_EXPECTED)]
#[case("blob", MYSQL_BYTES_IN, MYSQL_BYTES_EXPECTED)]
#[case("mediumblob", MYSQL_BYTES_IN, MYSQL_BYTES_EXPECTED)]
#[case("longblob", MYSQL_BYTES_IN, MYSQL_BYTES_EXPECTED)]
fn mysql_types_compatability(
    #[values(TestMysqlDatabase::new_mysql(), TestMysqlDatabase::new_mariadb())]
    mut in_db: TestMysqlDatabase,
    #[values(TestMysqlDatabase::new_mysql(), TestMysqlDatabase::new_mariadb())]
    mut out_db: TestMysqlDatabase,
    #[case] type_name: &str,
    #[case] value: &str,
    #[case] expected: &str,
) {
    let create_table_query = format!("CREATE TABLE test (field {type_name})");
    in_db.execute(&create_table_query);
    out_db.execute(&create_table_query);

    let mut expected = vec![Some(expected.to_string())];
    in_db.execute(format!("INSERT INTO test VALUES ({value})"));

    in_db
        .connection
        .query_drop("INSERT INTO test VALUES (NULL)")
        .unwrap();
    expected.push(None);

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());

    db_mover::run(args.clone()).unwrap();

    let mut result = out_db
        .connection
        .query_map("SELECT CAST(field AS CHAR) FROM test", |row: mysql::Row| {
            row.get::<Option<String>, _>(0).unwrap().map(|val| {
                // Remove formatting difference
                if type_name == "json" {
                    return val.replace(" ", "");
                };
                return val;
            })
        })
        .unwrap();
    expected.sort();
    result.sort();
    assert_eq!(expected, result);
}

#[rstest]
fn mysql_binary_16_uuid(
    #[values(TestMysqlDatabase::new_mysql(), TestMysqlDatabase::new_mariadb())]
    mut in_db: TestMysqlDatabase,
) {
    let mut out_db = TestPostresDatabase::new();
    in_db.execute("CREATE TABLE test (field binary(16))");
    out_db.execute("CREATE TABLE test (field UUID)");

    in_db.execute(format!(
        "INSERT INTO test VALUES (X'67e5504410b1426f9247bb680e5fe0c8')"
    ));
    in_db.execute("INSERT INTO test VALUES (NULL)");
    let mut expected = vec![Some("67e55044-10b1-426f-9247-bb680e5fe0c8".to_string())];
    expected.push(None);

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

#[rstest]
fn mysql_binary_16_no_uuid(
    #[values(TestMysqlDatabase::new_mysql(), TestMysqlDatabase::new_mariadb())]
    mut in_db: TestMysqlDatabase,
) {
    let mut out_db = TestPostresDatabase::new();
    in_db.execute("CREATE TABLE test (field binary(16))");
    out_db.execute("CREATE TABLE test (field bytea)");

    in_db.execute(format!("INSERT INTO test VALUES (X'9fad5e9eefdfb449')"));
    in_db.execute("INSERT INTO test VALUES (NULL)");
    let mut expected = vec![Some("\\x9fad5e9eefdfb4490000000000000000".to_string())];
    expected.push(None);

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    args.no_mysql_binary_16_as_uuid = true;

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

#[rstest]
fn mysql_table_list(
    #[values(TestMysqlDatabase::new_mysql(), TestMysqlDatabase::new_mariadb())]
    mut test_db: TestMysqlDatabase,
) {
    let mut db = MysqlDB::new(&test_db.uri, MysqlTypeOptions::default()).unwrap();

    let tables = db.get_tables().unwrap();
    assert_eq!(tables.len(), 0);

    test_db.create_test_table("test");
    let tables = db.get_tables().unwrap();
    assert_eq!(tables, vec![String::from("test")]);

    test_db.create_test_table("test2");
    let mut tables = db.get_tables().unwrap();
    tables.sort();
    assert_eq!(tables, vec![String::from("test"), String::from("test2")]);
}
