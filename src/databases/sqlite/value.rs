use crate::databases::table::{Column, ColumnType, Value};
use rusqlite::{
    types::{FromSql, ToSqlOutput, ValueRef},
    ToSql,
};

impl TryFrom<(&Column, ValueRef<'_>)> for Value {
    type Error = anyhow::Error;

    fn try_from(value: (&Column, ValueRef<'_>)) -> Result<Self, Self::Error> {
        let (column, val) = value;
        if val == ValueRef::Null {
            return Ok(Value::Null);
        }
        let parsed = match column.column_type {
            ColumnType::I64 => Value::I64(FromSql::column_result(val)?),
            ColumnType::I32 => Value::I32(FromSql::column_result(val)?),
            ColumnType::I16 => Value::I16(FromSql::column_result(val)?),
            ColumnType::F64 => Value::F64(FromSql::column_result(val)?),
            ColumnType::F32 => Value::F32(FromSql::column_result(val)?),
            ColumnType::String => Value::String(FromSql::column_result(val)?),
            ColumnType::Bytes => Value::Bytes(FromSql::column_result(val)?),
            ColumnType::Timestamp => Value::Timestamp(FromSql::column_result(val)?),
        };
        return Ok(parsed);
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Value::I64(val) => val.to_sql(),
            Value::I32(val) => val.to_sql(),
            Value::I16(val) => val.to_sql(),
            Value::F64(val) => val.to_sql(),
            Value::F32(val) => val.to_sql(),
            Value::String(val) => val.to_sql(),
            Value::Bytes(val) => val.to_sql(),
            Value::Timestamp(val) => val.to_sql(),
        }
    }
}
