use crate::{
    databases::table::{Column, ColumnType, Value},
    type_convetor::DefaultTypeConvertor,
};
use rusqlite::{
    ToSql,
    types::{FromSql, ToSqlOutput, ValueRef},
};

#[allow(dead_code)]
pub struct SqliteFromData<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: ValueRef<'a>,
}

#[allow(dead_code)]
pub struct SqliteToData<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: &'a Value,
}

pub trait SqliteTypeConvertor: Send {
    fn sqlite_from(data: SqliteFromData) -> anyhow::Result<Value> {
        if data.value == ValueRef::Null {
            return Ok(Value::Null);
        }
        let parsed = match data.column.column_type {
            ColumnType::I64 => Value::I64(FromSql::column_result(data.value)?),
            ColumnType::I32 => Value::I32(FromSql::column_result(data.value)?),
            ColumnType::I16 => Value::I16(FromSql::column_result(data.value)?),
            ColumnType::F64 => Value::F64(FromSql::column_result(data.value)?),
            ColumnType::F32 => Value::F32(FromSql::column_result(data.value)?),
            ColumnType::Bool => Value::Bool(FromSql::column_result(data.value)?),
            ColumnType::String => Value::String(FromSql::column_result(data.value)?),
            ColumnType::Bytes => {
                let buff: Vec<u8> = FromSql::column_result(data.value)?;
                Value::Bytes(bytes::Bytes::from(buff))
            }
            ColumnType::Timestamptz => Value::Timestamptz(FromSql::column_result(data.value)?),
            ColumnType::Timestamp => Value::Timestamp(FromSql::column_result(data.value)?),
            ColumnType::Date => Value::Date(FromSql::column_result(data.value)?),
            ColumnType::Time => Value::Time(FromSql::column_result(data.value)?),
            ColumnType::Json => Value::Json(FromSql::column_result(data.value)?),
            ColumnType::Uuid => Value::Uuid(FromSql::column_result(data.value)?),
            ColumnType::Decimal => {
                return Err(anyhow::anyhow!("Decimal is not supported for sqlite"));
            }
        };
        return Ok(parsed);
    }

    fn sqlite_to(data: SqliteToData<'_>) -> anyhow::Result<ToSqlOutput<'_>> {
        let output = match data.value {
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

impl SqliteTypeConvertor for DefaultTypeConvertor {}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Value::I64(val) => val.to_sql(),
            Value::I32(val) => val.to_sql(),
            Value::I16(val) => val.to_sql(),
            Value::F64(val) => val.to_sql(),
            Value::F32(val) => val.to_sql(),
            Value::Bool(val) => val.to_sql(),
            Value::String(val) => val.to_sql(),
            Value::Bytes(val) => val.to_sql(),
            Value::Timestamptz(val) => val.to_sql(),
            Value::Timestamp(val) => val.to_sql(),
            Value::Date(val) => val.to_sql(),
            Value::Time(val) => val.to_sql(),
            Value::Json(val) => val.to_sql(),
            Value::Uuid(val) => val.to_sql(),
            Value::Decimal(_) => {
                return Err(rusqlite::Error::ToSqlConversionFailure(
                    anyhow::anyhow!("Decimal is not supported for sqlite").into(),
                ));
            }
        }
    }
}
