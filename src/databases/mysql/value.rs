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
            _ => return Err(anyhow::anyhow!("Unsupported mysql type")),
            // ColumnType::Timestamptz => Value::Timestamptz(FromSql::column_result(val)?),
            // ColumnType::Date => Value::Date(FromSql::column_result(val)?),
            // ColumnType::Time => Value::Time(FromSql::column_result(val)?),
            // ColumnType::Json => Value::Json(FromSql::column_result(val)?),
            // ColumnType::Uuid => Value::Uuid(FromSql::column_result(val)?),
        };
        return Ok(parsed);
    }
}
