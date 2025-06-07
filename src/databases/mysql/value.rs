use chrono::{NaiveDateTime, TimeZone, Utc};

use crate::databases::table::{Column, ColumnType, Value};

impl TryFrom<(&Column, mysql::Value)> for Value {
    type Error = anyhow::Error;

    fn try_from(value: (&Column, mysql::Value)) -> Result<Self, Self::Error> {
        let (column, val) = value;
        if val == mysql::Value::NULL {
            return Ok(Value::Null);
        }
        let parsed = match column.column_type {
            ColumnType::I64 => Value::I64(mysql::from_value_opt(val)?),
            ColumnType::I32 => Value::I32(mysql::from_value_opt(val)?),
            ColumnType::I16 => Value::I16(mysql::from_value_opt(val)?),
            ColumnType::F64 => Value::F64(mysql::from_value_opt(val)?),
            ColumnType::F32 => Value::F32(mysql::from_value_opt(val)?),
            ColumnType::Bool => Value::Bool(mysql::from_value_opt(val)?),
            ColumnType::String => Value::String(mysql::from_value_opt(val)?),
            ColumnType::Bytes => Value::Bytes(mysql::from_value_opt(val)?),
            ColumnType::Timestamp => Value::Timestamp(mysql::from_value_opt(val)?),
            ColumnType::Timestamptz => {
                let dt: NaiveDateTime = mysql::from_value_opt(val)?;
                Value::Timestamptz(Utc.from_utc_datetime(&dt)) // UTC timezone set on connection
            }
            ColumnType::Date => Value::Date(mysql::from_value_opt(val)?),
            ColumnType::Time => Value::Time(mysql::from_value_opt(val)?),
            ColumnType::Json => Value::Json(mysql::from_value_opt(val)?),
            ColumnType::Uuid => Value::Uuid(mysql::from_value_opt(val)?),
        };
        return Ok(parsed);
    }
}
