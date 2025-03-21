use crate::databases::table::Value;
use anyhow::Context;
use rusqlite::{types::ValueRef, ToSql};

impl TryFrom<ValueRef<'_>> for Value {
    type Error = anyhow::Error;

    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        let parsed = match value {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(val) => Value::I64(val),
            ValueRef::Real(val) => Value::F64(val),
            ValueRef::Text(val) => {
                let val = std::str::from_utf8(val).context("invalid UTF-8")?;
                Value::String(val.to_string())
            }
            ValueRef::Blob(val) => Value::Bytes(val.to_vec()),
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
        }
    }
}
