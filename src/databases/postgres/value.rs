use std::io::Write;

use chrono::NaiveDateTime;
use postgres::types::Type;

use crate::databases::table::Value;

impl TryFrom<(&Type, &postgres::Row, usize)> for Value {
    type Error = anyhow::Error;

    fn try_from(value: (&Type, &postgres::Row, usize)) -> Result<Self, Self::Error> {
        let (column_type, row, idx) = value;
        let value = match column_type {
            &Type::INT8 => row
                .get::<_, Option<i64>>(idx)
                .map_or(Value::Null, Value::I64),
            &Type::FLOAT8 => row
                .get::<_, Option<f64>>(idx)
                .map_or(Value::Null, Value::F64),
            &Type::VARCHAR | &Type::TEXT | &Type::BPCHAR => row
                .get::<_, Option<String>>(idx)
                .map_or(Value::Null, Value::String),
            &Type::BYTEA => row
                .get::<_, Option<Vec<u8>>>(idx)
                .map_or(Value::Null, Value::Bytes),
            &Type::TIMESTAMP => row
                .get::<_, Option<NaiveDateTime>>(idx)
                .map_or(Value::Null, Value::Timestamp),
            _ => return Err(anyhow::anyhow!("Unsupported type")),
        };
        return Ok(value);
    }
}

// Microseconds since 2000-01-01 00:00
const POSTGRES_EPOCH_MICROS: i64 = 946684800000000;

impl Value {
    pub(crate) fn write_postgres_bytes(
        &self,
        column_type: &Type,
        writer: &mut impl Write,
    ) -> anyhow::Result<()> {
        if self == &Value::Null {
            writer.write_all(&(-1_i32).to_be_bytes())?;
            return Ok(());
        }
        match (column_type, self) {
            (&Type::INT8, &Value::I64(num)) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::INT4, &Value::I64(num)) => {
                let num = i32::try_from(num)?;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::INT2, &Value::I64(num)) => {
                let num = i16::try_from(num)?;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::FLOAT8, &Value::F64(num)) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::FLOAT4, &Value::F64(num)) => {
                let num = num as f32;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::BYTEA, Value::Bytes(bytes)) => {
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
            }
            (&Type::VARCHAR, Value::String(string))
            | (&Type::TEXT, Value::String(string))
            | (&Type::BPCHAR, Value::String(string)) => {
                let bytes = string.as_bytes();
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
            }
            (&Type::TIMESTAMP, &Value::Timestamp(dt)) => {
                let val = dt.and_utc().timestamp_micros() - POSTGRES_EPOCH_MICROS;
                writer.write_all(&(size_of_val(&val) as i32).to_be_bytes())?;
                writer.write_all(&val.to_be_bytes())?;
            }
            _ => return Err(anyhow::anyhow!("Unsupported type conversion")),
        };
        return Ok(());
    }
}
