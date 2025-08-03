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

pub struct MysqlColumnData<'a> {
    pub table: &'a str,
    pub column_name: String,
    pub column_type: String,
    pub nullable: bool,
    pub options: &'a MysqlTypeOptions,
    pub has_json_constraint: bool,
}

pub struct MysqlFromData<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: mysql::Value,
}

pub struct MysqlToData<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: &'a Value,
}

pub trait MysqlTypeConvertor: Send + 'static {
    fn mysql_column(data: MysqlColumnData) -> anyhow::Result<Column> {
        let column_type = Self::mysql_column_type(&data)?;
        return Ok(Column {
            name: data.column_name,
            column_type,
            nullable: data.nullable,
        });
    }

    fn mysql_column_type(data: &MysqlColumnData) -> anyhow::Result<ColumnType> {
        let formated = data.column_type.trim().to_lowercase();
        if data.options.binary_16_as_uuid && formated == "binary(16)" {
            return Ok(ColumnType::Uuid);
        }
        if data.options.tinyint_as_bool && formated == "tinyint(1)" {
            return Ok(ColumnType::Bool);
        }
        if data.has_json_constraint {
            return Ok(ColumnType::Json);
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
        if formated.starts_with("numeric") || formated.starts_with("decimal") {
            return Ok(ColumnType::Decimal);
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
            _ => Err(anyhow::anyhow!("Unknown column type {}", data.column_type)),
        };
    }

    fn mysql_from(data: MysqlFromData) -> anyhow::Result<Value> {
        if data.value == mysql::Value::NULL {
            return Ok(Value::Null);
        }
        let parsed = match data.column.column_type {
            ColumnType::I64 => Value::I64(mysql::from_value_opt(data.value)?),
            ColumnType::I32 => Value::I32(mysql::from_value_opt(data.value)?),
            ColumnType::I16 => Value::I16(mysql::from_value_opt(data.value)?),
            ColumnType::F64 => Value::F64(mysql::from_value_opt(data.value)?),
            ColumnType::F32 => Value::F32(mysql::from_value_opt(data.value)?),
            ColumnType::Decimal => Value::Decimal(mysql::from_value_opt(data.value)?),
            ColumnType::Bool => Value::Bool(mysql::from_value_opt(data.value)?),
            ColumnType::String => Value::String(mysql::from_value_opt(data.value)?),
            ColumnType::Bytes => Value::Bytes(bytes::Bytes::from(
                mysql::from_value_opt::<Vec<u8>>(data.value)?,
            )),
            ColumnType::Timestamp => Value::Timestamp(mysql::from_value_opt(data.value)?),
            ColumnType::Timestamptz => {
                let dt: NaiveDateTime = mysql::from_value_opt(data.value)?;
                Value::Timestamptz(Utc.from_utc_datetime(&dt)) // UTC timezone set on connection
            }
            ColumnType::Date => Value::Date(mysql::from_value_opt(data.value)?),
            ColumnType::Time => Value::Time(mysql::from_value_opt(data.value)?),
            ColumnType::Json => Value::Json(mysql::from_value_opt(data.value)?),
            ColumnType::Uuid => Value::Uuid(mysql::from_value_opt(data.value)?),
        };
        return Ok(parsed);
    }

    fn mysql_to(data: MysqlToData<'_>) -> anyhow::Result<mysql::Value> {
        let result = match data.value {
            Value::Null => mysql::Value::NULL,
            Value::I64(val) => val.into(),
            Value::I32(val) => val.into(),
            Value::I16(val) => val.into(),
            Value::F64(val) => val.into(),
            Value::F32(val) => val.into(),
            Value::Decimal(val) => val.into(),
            Value::Bool(val) => val.into(),
            Value::String(val) => val.into(),
            Value::Bytes(val) => val.as_ref().into(),
            Value::Timestamptz(val) => val.naive_utc().into(),
            Value::Timestamp(val) => val.into(),
            Value::Date(val) => val.into(),
            Value::Time(val) => val.into(),
            Value::Json(val) => val.into(),
            Value::Uuid(val) => val.into(),
        };
        return Ok(result);
    }
}
