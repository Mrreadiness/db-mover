use chrono::{NaiveDateTime, TimeZone, Utc};

use crate::databases::table::{Column, ColumnType, Value};

#[derive(Clone, Debug, PartialEq)]
pub struct MysqlTypeOptions {
    pub binary_16_as_uuid: bool,
    pub tinyint_as_bool: bool,
}

impl Default for MysqlTypeOptions {
    fn default() -> Self {
        return MysqlTypeOptions {
            binary_16_as_uuid: true,
            tinyint_as_bool: true,
        };
    }
}

impl ColumnType {
    pub fn try_from_mysql_type(
        type_name: &str,
        options: &MysqlTypeOptions,
    ) -> anyhow::Result<ColumnType> {
        let formated = type_name.trim().to_lowercase();
        if options.binary_16_as_uuid && formated == "binary(16)" {
            return Ok(ColumnType::Uuid);
        }
        if options.tinyint_as_bool && formated == "tinyint(1)" {
            return Ok(ColumnType::Bool);
        }
        if formated.starts_with("char") || formated.starts_with("varchar") {
            return Ok(ColumnType::String);
        }
        if formated.starts_with("binary") || formated.starts_with("varbinary") {
            return Ok(ColumnType::Bytes);
        }
        if formated.starts_with("smallint") {
            return Ok(ColumnType::I16);
        }
        if formated.starts_with("int") {
            return Ok(ColumnType::I32);
        }
        if formated.starts_with("bigint") {
            return Ok(ColumnType::I64);
        }
        return match formated.as_str() {
            "float" => Ok(ColumnType::F32),
            "double" | "real" | "double precision" => Ok(ColumnType::F64),
            "bool" | "boolean" => Ok(ColumnType::Bool),
            "tinytext" | "text" | "mediumtext" | "longtext" => Ok(ColumnType::String),
            "tinyblob" | "blob" | "mediumblob" | "longblob" => Ok(ColumnType::Bytes),
            "timestamp" => Ok(ColumnType::Timestamptz),
            "datetime" => Ok(ColumnType::Timestamp),
            "date" => Ok(ColumnType::Date),
            "time" => Ok(ColumnType::Time),
            "json" => Ok(ColumnType::Json),
            _ => Err(anyhow::anyhow!("Unknown column type {type_name}")),
        };
    }
}

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

impl From<&Value> for mysql::Value {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => mysql::Value::NULL,
            Value::I64(val) => val.into(),
            Value::I32(val) => val.into(),
            Value::I16(val) => val.into(),
            Value::F64(val) => val.into(),
            Value::F32(val) => val.into(),
            Value::Bool(val) => val.into(),
            Value::String(val) => val.into(),
            Value::Bytes(val) => val.into(),
            Value::Timestamptz(val) => val.naive_utc().into(),
            Value::Timestamp(val) => val.into(),
            Value::Date(val) => val.into(),
            Value::Time(val) => val.into(),
            Value::Json(val) => val.into(),
            Value::Uuid(val) => val.into(),
        }
    }
}
