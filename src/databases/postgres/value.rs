use std::io::Write;

use anyhow::Context;
use chrono::NaiveDateTime;
use postgres::types::Type;

use crate::databases::{
    table::{Column, ColumnType, Value},
    traits::WriterError,
};

impl TryFrom<Type> for ColumnType {
    type Error = anyhow::Error;

    fn try_from(value: Type) -> Result<Self, Self::Error> {
        let column_type = match value {
            Type::INT8 => ColumnType::I64,
            Type::INT4 => ColumnType::I32,
            Type::INT2 => ColumnType::I16,
            Type::FLOAT8 => ColumnType::F64,
            Type::FLOAT4 => ColumnType::F32,
            Type::VARCHAR | Type::TEXT | Type::BPCHAR => ColumnType::String,
            Type::BYTEA => ColumnType::Bytes,
            Type::TIMESTAMP => ColumnType::Timestamp,
            Type::JSON | Type::JSON_ARRAY | Type::JSONB | Type::JSONB_ARRAY => ColumnType::Json,
            Type::UUID => ColumnType::Uuid,
            _ => return Err(anyhow::anyhow!("Unsupported postgres type {value}")),
        };
        return Ok(column_type);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PostgreColumn {
    pub name: String,
    pub column_type: postgres::types::Type,
    pub nullable: bool,
}

impl TryFrom<PostgreColumn> for Column {
    type Error = anyhow::Error;

    fn try_from(value: PostgreColumn) -> Result<Self, Self::Error> {
        return Ok(Column {
            name: value.name,
            column_type: value.column_type.try_into()?,
            nullable: value.nullable,
        });
    }
}

impl TryFrom<(ColumnType, &postgres::Row, usize)> for Value {
    type Error = anyhow::Error;

    fn try_from(value: (ColumnType, &postgres::Row, usize)) -> Result<Self, Self::Error> {
        let (column_type, row, idx) = value;
        let value = match column_type {
            ColumnType::I64 => row
                .get::<_, Option<i64>>(idx)
                .map_or(Value::Null, Value::I64),
            ColumnType::I32 => row
                .get::<_, Option<i32>>(idx)
                .map_or(Value::Null, Value::I32),
            ColumnType::I16 => row
                .get::<_, Option<i16>>(idx)
                .map_or(Value::Null, Value::I16),
            ColumnType::F64 => row
                .get::<_, Option<f64>>(idx)
                .map_or(Value::Null, Value::F64),
            ColumnType::F32 => row
                .get::<_, Option<f32>>(idx)
                .map_or(Value::Null, Value::F32),
            ColumnType::String => row
                .get::<_, Option<String>>(idx)
                .map_or(Value::Null, Value::String),
            ColumnType::Bytes => row
                .get::<_, Option<Vec<u8>>>(idx)
                .map_or(Value::Null, Value::Bytes),
            ColumnType::Timestamp => row
                .get::<_, Option<NaiveDateTime>>(idx)
                .map_or(Value::Null, Value::Timestamp),
            ColumnType::Json => row
                .get::<_, Option<serde_json::Value>>(idx)
                .map_or(Value::Null, Value::Json),
            ColumnType::Uuid => row
                .get::<_, Option<uuid::Uuid>>(idx)
                .map_or(Value::Null, Value::Uuid),
        };
        return Ok(value);
    }
}

// Microseconds since 2000-01-01 00:00
const POSTGRES_EPOCH_MICROS: i64 = 946684800000000;

impl Value {
    pub(crate) fn write_postgres_bytes(
        &self,
        writer: &mut impl Write,
        column: &PostgreColumn,
    ) -> Result<(), WriterError> {
        match self {
            &Value::Null => {
                writer.write_all(&(-1_i32).to_be_bytes())?;
            }
            &Value::I64(num) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            &Value::I32(num) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            &Value::I16(num) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            &Value::F64(num) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            &Value::F32(num) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            Value::Bytes(bytes) => {
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
            }
            Value::String(string) => {
                let bytes = string.as_bytes();
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
            }
            &Value::Timestamp(dt) => {
                let val = dt.and_utc().timestamp_micros() - POSTGRES_EPOCH_MICROS;
                writer.write_all(&(size_of_val(&val) as i32).to_be_bytes())?;
                writer.write_all(&val.to_be_bytes())?;
            }
            Value::Json(value) => {
                let bytes =
                    serde_json::to_vec(value).context("Failed to serialize json into bytes")?;
                if column.column_type == Type::JSONB || column.column_type == Type::JSONB_ARRAY {
                    let jsonb_version = 1_u8;
                    let len = (bytes.len() + size_of_val(&jsonb_version)) as i32;
                    writer.write_all(&(len).to_be_bytes())?;
                    writer.write_all(&(jsonb_version).to_be_bytes())?;
                } else {
                    writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                }
                writer.write_all(&bytes)?;
            }
            &Value::Uuid(val) => {
                let bytes = val.as_bytes();
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
            }
        };
        return Ok(());
    }
}
