mod common;
use anyhow::Context;
use db_mover::databases::{
    mysql::value::MysqlTypeConvertor,
    postgres::value::PostgresTypeConvertor,
    sqlite::value::SqliteTypeConvertor,
    table::{ColumnType, Value},
    type_convertor::{DefaultTypeConvertor, TypeConvetor},
};
use std::io::Write;

use common::all_databases_combinations;
use common::mysql::TestMysqlDatabase;
use common::postgres::TestPostresDatabase;
use common::sqlite::TestSqliteDatabase;
use common::testable_database::TestableDatabase;
use pretty_assertions::{assert_eq, assert_ne};
use rstest::rstest;
use rstest_reuse::{self, *};
use rusqlite::ToSql;

struct CustomTypeConvertor;

static CUSTOM_STRING_OVERRIDE: &str = "CustomTypeConvertor override";
static CUSTOM_F32_OVERRIDE: f32 = -1.1;

impl SqliteTypeConvertor for CustomTypeConvertor {
    fn sqlite_from(
        data: db_mover::databases::sqlite::value::SqliteFromData,
    ) -> anyhow::Result<db_mover::databases::table::Value> {
        if data.column.column_type == ColumnType::String {
            return Ok(Value::String(CUSTOM_STRING_OVERRIDE.to_string()));
        }
        return DefaultTypeConvertor::sqlite_from(data);
    }

    fn sqlite_to(
        data: db_mover::databases::sqlite::value::SqliteToData<'_>,
    ) -> anyhow::Result<rusqlite::types::ToSqlOutput<'_>> {
        if data.column.column_type == ColumnType::F32 {
            return CUSTOM_F32_OVERRIDE
                .to_sql()
                .context("Failed to convert F32 for CustomTypeConvertor");
        }
        return DefaultTypeConvertor::sqlite_to(data);
    }
}
impl PostgresTypeConvertor for CustomTypeConvertor {
    fn postgres_from(
        data: db_mover::databases::postgres::value::PostgresFromData,
    ) -> anyhow::Result<Value> {
        if data.column.column_type == ColumnType::String {
            return Ok(Value::String(CUSTOM_STRING_OVERRIDE.to_string()));
        }
        return DefaultTypeConvertor::postgres_from(data);
    }

    fn postgres_to(
        writer: &mut postgres::CopyInWriter<'_>,
        data: db_mover::databases::postgres::value::PostgresToData,
    ) -> Result<(), db_mover::databases::traits::WriterError> {
        if ColumnType::try_from(data.column.column_type.clone()).unwrap() == ColumnType::F32 {
            writer.write_all(&(size_of_val(&CUSTOM_F32_OVERRIDE) as i32).to_be_bytes())?;
            writer.write_all(&CUSTOM_F32_OVERRIDE.to_be_bytes())?;
            return Ok(());
        }
        return DefaultTypeConvertor::postgres_to(writer, data);
    }
}
impl MysqlTypeConvertor for CustomTypeConvertor {
    fn mysql_from(data: db_mover::databases::mysql::value::MysqlFromData) -> anyhow::Result<Value> {
        if data.column.column_type == ColumnType::String {
            return Ok(Value::String(CUSTOM_STRING_OVERRIDE.to_string()));
        }
        return DefaultTypeConvertor::mysql_from(data);
    }
    fn mysql_to(
        data: db_mover::databases::mysql::value::MysqlToData<'_>,
    ) -> anyhow::Result<mysql::Value> {
        if data.column.column_type == ColumnType::F32 {
            return Ok(mysql::Value::Float(CUSTOM_F32_OVERRIDE));
        }
        return DefaultTypeConvertor::mysql_to(data);
    }
}

impl TypeConvetor for CustomTypeConvertor {}

#[apply(all_databases_combinations)]
fn custom_type_convertor_from(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    in_db.create_test_table("test");
    out_db.create_test_table("test");
    in_db.fill_test_table("test", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run_with::<CustomTypeConvertor>(args).unwrap();

    for row in out_db.get_all_rows("test") {
        assert_eq!(row.text.as_str(), CUSTOM_STRING_OVERRIDE);
    }
}

#[apply(all_databases_combinations)]
fn custom_type_convertor_to(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    in_db.create_test_table("test");
    out_db.create_test_table("test");
    in_db.fill_test_table("test", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run_with::<CustomTypeConvertor>(args).unwrap();

    for row in out_db.get_all_rows("test") {
        assert_eq!(row.real, CUSTOM_F32_OVERRIDE);
    }
}
