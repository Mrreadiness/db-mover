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

pub struct MysqlConstraint {
    pub name: String,
    pub constraint_type: String,
    pub clause: Option<String>,
}

pub struct MysqlColumn<'a> {
    pub table: &'a str,
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub options: &'a MysqlTypeOptions,
    pub table_constraints: &'a [MysqlConstraint],
}

pub struct MysqlReadInput<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: mysql::Value,
}

pub struct MysqlWriteInput<'a> {
    pub table: &'a str,
    pub column: &'a Column,
    pub value: &'a Value,
}

pub trait MysqlTypeConverter: Send + 'static {
    fn mysql_column(column: MysqlColumn) -> anyhow::Result<Column> {
        let column_type = Self::mysql_column_type(&column)?;
        return Ok(Column {
            name: column.name,
            column_type,
            nullable: column.nullable,
        });
    }

    fn mysql_column_type(column: &MysqlColumn) -> anyhow::Result<ColumnType> {
        let formated = column.column_type.trim().to_lowercase();
        if column.options.binary_16_as_uuid && formated == "binary(16)" {
            return Ok(ColumnType::Uuid);
        }
        if column.options.tinyint_as_bool && formated == "tinyint(1)" {
            return Ok(ColumnType::Bool);
        }
        let json_constraint = Some(format!("json_valid(`{}`)", column.name));
        if column
            .table_constraints
            .iter()
            .any(|constraint| constraint.clause == json_constraint)
        {
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
            _ => Err(anyhow::anyhow!(
                "Unknown column type {}",
                column.column_type
            )),
        };
    }

    fn mysql_read_value(input: MysqlReadInput) -> anyhow::Result<Value> {
        if input.value == mysql::Value::NULL {
            return Ok(Value::Null);
        }
        let parsed = match input.column.column_type {
            ColumnType::I64 => Value::I64(mysql::from_value_opt(input.value)?),
            ColumnType::I32 => Value::I32(mysql::from_value_opt(input.value)?),
            ColumnType::I16 => Value::I16(mysql::from_value_opt(input.value)?),
            ColumnType::F64 => Value::F64(mysql::from_value_opt(input.value)?),
            ColumnType::F32 => Value::F32(mysql::from_value_opt(input.value)?),
            ColumnType::Decimal => Value::Decimal(mysql::from_value_opt(input.value)?),
            ColumnType::Bool => Value::Bool(mysql::from_value_opt(input.value)?),
            ColumnType::String => Value::String(mysql::from_value_opt(input.value)?),
            ColumnType::Bytes => Value::Bytes(bytes::Bytes::from(
                mysql::from_value_opt::<Vec<u8>>(input.value)?,
            )),
            ColumnType::Timestamp => Value::Timestamp(mysql::from_value_opt(input.value)?),
            ColumnType::Timestamptz => {
                let dt: NaiveDateTime = mysql::from_value_opt(input.value)?;
                Value::Timestamptz(Utc.from_utc_datetime(&dt)) // UTC timezone set on connection
            }
            ColumnType::Date => Value::Date(mysql::from_value_opt(input.value)?),
            ColumnType::Time => Value::Time(mysql::from_value_opt(input.value)?),
            ColumnType::Json => Value::Json(mysql::from_value_opt(input.value)?),
            ColumnType::Uuid => Value::Uuid(mysql::from_value_opt(input.value)?),
            ColumnType::Custom(ref name) => {
                return Err(anyhow::anyhow!(
                    "Custom type '{name}' is not supported by default MysqlTypeConverter"
                ));
            }
        };
        return Ok(parsed);
    }

    fn mysql_write_value(input: MysqlWriteInput) -> anyhow::Result<mysql::Value> {
        let result = match input.value {
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
            Value::Custom(_) => {
                return Err(anyhow::anyhow!(
                    "Custom Value for type {:?} is not supported by default MysqlTypeConverter",
                    input.column.column_type
                ));
            }
        };
        return Ok(result);
    }
}
