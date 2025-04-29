use std::io::Write;

use chrono::NaiveDateTime;
use postgres::types::Type;

use crate::databases::{
    table::{ColumnType, Value},
    traits::WriterError,
};

impl TryFrom<&Type> for ColumnType {
    type Error = anyhow::Error;

    fn try_from(value: &Type) -> Result<Self, Self::Error> {
        let column_type = match value {
            &Type::INT8 => ColumnType::I64,
            &Type::INT4 => ColumnType::I32,
            &Type::INT2 => ColumnType::I16,
            &Type::FLOAT8 => ColumnType::F64,
            &Type::FLOAT4 => ColumnType::F32,
            &Type::VARCHAR | &Type::TEXT | &Type::BPCHAR => ColumnType::String,
            &Type::BYTEA => ColumnType::Bytes,
            &Type::TIMESTAMP => ColumnType::Timestamp,
            _ => return Err(anyhow::anyhow!("Unsupported postgres type {value}")),
        };
        return Ok(column_type);
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
        };
        return Ok(value);
    }
}

// Microseconds since 2000-01-01 00:00
const POSTGRES_EPOCH_MICROS: i64 = 946684800000000;

impl Value {
    pub(crate) fn write_postgres_bytes(&self, writer: &mut impl Write) -> Result<(), WriterError> {
        if self == &Value::Null {
            writer.write_all(&(-1_i32).to_be_bytes())?;
            return Ok(());
        }
        match self {
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
            _ => {
                return Err(WriterError::Unrecoverable(anyhow::anyhow!(
                    "Unsupported type conversion"
                )));
            }
        };
        return Ok(());
    }
}
