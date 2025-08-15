mod common;
use anyhow::Context;
use db_mover::databases::{
    mysql::type_converter::MysqlTypeConverter,
    postgres::type_converter::PostgresTypeConverter,
    sqlite::type_converter::SqliteTypeConverter,
    table::{ColumnType, Value},
    type_converter::{DefaultTypeConverter, TypeConveter},
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

struct CustomTypeConverter;

static CUSTOM_STRING_OVERRIDE: &str = "CustomTypeConverter override";
static CUSTOM_F32_OVERRIDE: f32 = -1.1;

impl SqliteTypeConverter for CustomTypeConverter {
    fn sqlite_read_value(
        data: db_mover::databases::sqlite::type_converter::SqliteReadInput,
    ) -> anyhow::Result<db_mover::databases::table::Value> {
        if data.column.column_type == ColumnType::String {
            return Ok(Value::String(CUSTOM_STRING_OVERRIDE.to_string()));
        }
        return DefaultTypeConverter::sqlite_read_value(data);
    }

    fn sqlite_write_value(
        data: db_mover::databases::sqlite::type_converter::SqliteWriteInput<'_>,
    ) -> anyhow::Result<rusqlite::types::ToSqlOutput<'_>> {
        if data.column.column_type == ColumnType::F32 {
            return CUSTOM_F32_OVERRIDE
                .to_sql()
                .context("Failed to convert F32 for CustomTypeConverter");
        }
        return DefaultTypeConverter::sqlite_write_value(data);
    }
}
impl PostgresTypeConverter for CustomTypeConverter {
    fn postgres_read_value(
        data: db_mover::databases::postgres::type_converter::PostgresReadInput,
    ) -> anyhow::Result<Value> {
        if data.column.column_type == ColumnType::String {
            return Ok(Value::String(CUSTOM_STRING_OVERRIDE.to_string()));
        }
        return DefaultTypeConverter::postgres_read_value(data);
    }

    fn postgres_write_value(
        writer: &mut postgres::CopyInWriter<'_>,
        data: db_mover::databases::postgres::type_converter::PostgresWriteInput,
    ) -> Result<(), db_mover::databases::traits::WriterError> {
        if data.column.column_type == ColumnType::F32 {
            writer.write_all(&(size_of_val(&CUSTOM_F32_OVERRIDE) as i32).to_be_bytes())?;
            writer.write_all(&CUSTOM_F32_OVERRIDE.to_be_bytes())?;
            return Ok(());
        }
        return DefaultTypeConverter::postgres_write_value(writer, data);
    }
}
impl MysqlTypeConverter for CustomTypeConverter {
    fn mysql_read_value(
        data: db_mover::databases::mysql::type_converter::MysqlReadInput,
    ) -> anyhow::Result<Value> {
        if data.column.column_type == ColumnType::String {
            return Ok(Value::String(CUSTOM_STRING_OVERRIDE.to_string()));
        }
        return DefaultTypeConverter::mysql_read_value(data);
    }
    fn mysql_write_value(
        data: db_mover::databases::mysql::type_converter::MysqlWriteInput<'_>,
    ) -> anyhow::Result<mysql::Value> {
        if data.column.column_type == ColumnType::F32 {
            return Ok(mysql::Value::Float(CUSTOM_F32_OVERRIDE));
        }
        return DefaultTypeConverter::mysql_write_value(data);
    }
}

impl TypeConveter for CustomTypeConverter {}

#[apply(all_databases_combinations)]
fn custom_type_converter_override_from(
    mut in_db: impl TestableDatabase,
    mut out_db: impl TestableDatabase,
) {
    in_db.create_test_table("test");
    out_db.create_test_table("test");
    in_db.fill_test_table("test", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run_with::<CustomTypeConverter>(args).unwrap();

    for row in out_db.get_all_rows("test") {
        assert_eq!(row.text.as_str(), CUSTOM_STRING_OVERRIDE);
    }
}

#[apply(all_databases_combinations)]
fn custom_type_converter_override_to(
    mut in_db: impl TestableDatabase,
    mut out_db: impl TestableDatabase,
) {
    in_db.create_test_table("test");
    out_db.create_test_table("test");
    in_db.fill_test_table("test", 10);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run_with::<CustomTypeConverter>(args).unwrap();

    for row in out_db.get_all_rows("test") {
        assert_eq!(row.real, CUSTOM_F32_OVERRIDE);
    }
}
