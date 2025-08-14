use crate::databases::table::{Column, ColumnType, Value};
use rusqlite::{
    ToSql,
    types::{FromSql, ToSqlOutput, ValueRef},
};

pub struct SqliteColumn<'a> {
    pub table: &'a str,
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
}

pub struct SqliteReadInput<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: ValueRef<'a>,
}

pub struct SqliteWriteInput<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: &'a Value,
}

pub trait SqliteTypeConverter: Send + 'static {
    fn sqlite_column(column: SqliteColumn) -> anyhow::Result<Column> {
        let column_type = Self::sqlite_column_type(&column)?;
        return Ok(Column {
            name: column.name,
            column_type,
            nullable: column.nullable,
        });
    }

    fn sqlite_column_type(column: &SqliteColumn) -> anyhow::Result<ColumnType> {
        let type_formated = column.column_type.trim().to_lowercase();
        if type_formated.starts_with("varchar")
            || type_formated.starts_with("nvarchar")
            || type_formated.starts_with("nchar")
            || type_formated.starts_with("char")
        {
            return Ok(ColumnType::String);
        }
        let column_type = match type_formated.as_str() {
            "tinyint" | "smallint" | "smallserial" => ColumnType::I16,
            "integer" | "serial" | "int" => ColumnType::I32,
            "bigint" | "bigserial" => ColumnType::I64,
            "float" | "real" => ColumnType::F32,
            "double" | "double precision" => ColumnType::F64,
            "bool" | "boolean" => ColumnType::Bool,
            "character" | "varchar" | "nvarchar" | "char" | "nchar" | "clob" | "text"
            | "bpchar" => ColumnType::String,

            "blob" | "bytea" => ColumnType::Bytes,
            "timestamptz" => ColumnType::Timestamptz,
            "datetime" | "timestamp" => ColumnType::Timestamp,
            "date" => ColumnType::Date,
            "time" => ColumnType::Time,
            "json" | "jsonb" => ColumnType::Json,
            "uuid" => ColumnType::Uuid,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown column type {}",
                    column.column_type
                ));
            }
        };
        return Ok(column_type);
    }

    fn sqlite_read_value(input: SqliteReadInput) -> anyhow::Result<Value> {
        if input.value == ValueRef::Null {
            return Ok(Value::Null);
        }
        let parsed = match input.column.column_type {
            ColumnType::I64 => Value::I64(FromSql::column_result(input.value)?),
            ColumnType::I32 => Value::I32(FromSql::column_result(input.value)?),
            ColumnType::I16 => Value::I16(FromSql::column_result(input.value)?),
            ColumnType::F64 => Value::F64(FromSql::column_result(input.value)?),
            ColumnType::F32 => Value::F32(FromSql::column_result(input.value)?),
            ColumnType::Bool => Value::Bool(FromSql::column_result(input.value)?),
            ColumnType::String => Value::String(FromSql::column_result(input.value)?),
            ColumnType::Bytes => {
                let buff: Vec<u8> = FromSql::column_result(input.value)?;
                Value::Bytes(bytes::Bytes::from(buff))
            }
            ColumnType::Timestamptz => Value::Timestamptz(FromSql::column_result(input.value)?),
            ColumnType::Timestamp => Value::Timestamp(FromSql::column_result(input.value)?),
            ColumnType::Date => Value::Date(FromSql::column_result(input.value)?),
            ColumnType::Time => Value::Time(FromSql::column_result(input.value)?),
            ColumnType::Json => Value::Json(FromSql::column_result(input.value)?),
            ColumnType::Uuid => Value::Uuid(FromSql::column_result(input.value)?),
            ColumnType::Decimal => {
                return Err(anyhow::anyhow!("Decimal is not supported for sqlite"));
            }
        };
        return Ok(parsed);
    }

    fn sqlite_write_value(input: SqliteWriteInput<'_>) -> anyhow::Result<ToSqlOutput<'_>> {
        let output = match input.value {
            Value::Null => ToSqlOutput::from(rusqlite::types::Null),
            Value::I64(val) => val.to_sql()?,
            Value::I32(val) => val.to_sql()?,
            Value::I16(val) => val.to_sql()?,
            Value::F64(val) => val.to_sql()?,
            Value::F32(val) => val.to_sql()?,
            Value::Bool(val) => val.to_sql()?,
            Value::String(val) => val.to_sql()?,
            Value::Bytes(val) => val.to_sql()?,
            Value::Timestamptz(val) => val.to_sql()?,
            Value::Timestamp(val) => val.to_sql()?,
            Value::Date(val) => val.to_sql()?,
            Value::Time(val) => val.to_sql()?,
            Value::Json(val) => val.to_sql()?,
            Value::Uuid(val) => val.to_sql()?,
            Value::Decimal(_) => {
                return Err(anyhow::anyhow!("Decimal is not supported for sqlite"));
            }
        };
        return Ok(output);
    }
}
