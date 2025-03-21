use crate::databases::table::{Column, ColumnType, Value};
use rusqlite::{
    types::{FromSql, ValueRef},
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
            ColumnType::F64 => Value::F64(FromSql::column_result(val)?),
            ColumnType::String => Value::String(FromSql::column_result(val)?),
            ColumnType::Bytes => Value::Bytes(FromSql::column_result(val)?),
            ColumnType::Timestamp => Value::Timestamp(FromSql::column_result(val)?),
        };
        return Ok(parsed);
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Value::Null => None::<i32>.to_sql(),
            Value::I64(val) => val.to_sql(),
            Value::F64(val) => val.to_sql(),
            Value::String(val) => val.to_sql(),
            Value::Bytes(val) => val.to_sql(),
            Value::Timestamp(val) => val.to_sql(),
        }
    }
}
