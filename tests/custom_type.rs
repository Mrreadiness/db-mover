mod common;
use anyhow::Context;
use db_mover::databases::{
    mysql::type_converter::{MysqlColumn, MysqlTypeConverter},
    postgres::type_converter::{PostgresColumn, PostgresTypeConverter},
    sqlite::type_converter::{SqliteColumn, SqliteTypeConverter},
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

static CUSTOM_STRING: ColumnType = ColumnType::Custom(std::borrow::Cow::Borrowed("custom_string"));

struct CustomTypeConverter;

impl SqliteTypeConverter for CustomTypeConverter {
    fn sqlite_column_type(column: &SqliteColumn) -> anyhow::Result<ColumnType> {
        if column.name == "custom_string_column" {
            return Ok(CUSTOM_STRING.clone());
        }
        return DefaultTypeConverter::sqlite_column_type(column);
    }

    fn sqlite_read_value(
        input: db_mover::databases::sqlite::type_converter::SqliteReadInput,
    ) -> anyhow::Result<db_mover::databases::table::Value> {
        if input.column.column_type == CUSTOM_STRING {
            let data: String = rusqlite::types::FromSql::column_result(input.value)?;
            return Ok(Value::Custom(data.into_bytes().into()));
        }
        return DefaultTypeConverter::sqlite_read_value(input);
    }

    fn sqlite_write_value(
        input: db_mover::databases::sqlite::type_converter::SqliteWriteInput<'_>,
    ) -> anyhow::Result<rusqlite::types::ToSqlOutput<'_>> {
        return match input.value {
            Value::Custom(data) => {
                let string = String::from_utf8(data.to_vec())
                    .context("Failed to conver custom_string from bytes")?;

                Ok(rusqlite::types::ToSqlOutput::from(string))
            }
            _ => DefaultTypeConverter::sqlite_write_value(input),
        };
    }
}
impl PostgresTypeConverter for CustomTypeConverter {
    fn postgres_column_type(column: &PostgresColumn) -> anyhow::Result<ColumnType> {
        if column.name == "custom_string_column" {
            return Ok(CUSTOM_STRING.clone());
        }
        return DefaultTypeConverter::postgres_column_type(column);
    }
    fn postgres_read_value(
        input: db_mover::databases::postgres::type_converter::PostgresReadInput,
    ) -> anyhow::Result<Value> {
        if input.column.column_type == CUSTOM_STRING {
            return Ok(input
                .row
                .get::<_, Option<String>>(input.column_index)
                .map_or(Value::Null, Value::String));
        }
        return DefaultTypeConverter::postgres_read_value(input);
    }

    fn postgres_write_value(
        writer: &mut postgres::CopyInWriter<'_>,
        input: db_mover::databases::postgres::type_converter::PostgresWriteInput,
    ) -> Result<(), db_mover::databases::traits::WriterError> {
        return match input.value {
            Value::Custom(data) => {
                let string = String::from_utf8(data.to_vec())
                    .context("Failed to conver custom_string from bytes")?;
                let bytes = string.as_bytes();
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
                return Ok(());
            }
            _ => DefaultTypeConverter::postgres_write_value(writer, input),
        };
    }
}
impl MysqlTypeConverter for CustomTypeConverter {
    fn mysql_column_type(column: &MysqlColumn) -> anyhow::Result<ColumnType> {
        if column.name == "custom_string_column" {
            return Ok(CUSTOM_STRING.clone());
        }
        return DefaultTypeConverter::mysql_column_type(column);
    }

    fn mysql_read_value(
        input: db_mover::databases::mysql::type_converter::MysqlReadInput,
    ) -> anyhow::Result<Value> {
        if input.column.column_type == CUSTOM_STRING {
            return Ok(Value::String(mysql::from_value_opt(input.value)?));
        }
        return DefaultTypeConverter::mysql_read_value(input);
    }
    fn mysql_write_value(
        input: db_mover::databases::mysql::type_converter::MysqlWriteInput<'_>,
    ) -> anyhow::Result<mysql::Value> {
        return match input.value {
            Value::Custom(data) => {
                let string = String::from_utf8(data.to_vec())
                    .context("Failed to conver custom_string from bytes")?;

                Ok(string.into())
            }
            _ => DefaultTypeConverter::mysql_write_value(input),
        };
    }
}

impl TypeConveter for CustomTypeConverter {}

#[apply(all_databases_combinations)]
fn custom_type(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    in_db.execute("CREATE TABLE test (custom_string_column TEXT)");
    out_db.execute("CREATE TABLE test (custom_string_column TEXT)");
    for _ in 0..10 {
        in_db.execute("INSERT INTO test VALUES ('test_data')");
    }
    let count_query = "SELECT COUNT(1) FROM test WHERE custom_string_column = 'test_data'";
    assert_ne!(
        in_db.query_count(count_query),
        out_db.query_count(count_query)
    );

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run_with::<CustomTypeConverter>(args).unwrap();

    assert_eq!(
        in_db.query_count(count_query),
        out_db.query_count(count_query)
    );
}
