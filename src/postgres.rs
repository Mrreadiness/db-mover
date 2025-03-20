use std::io::Write;

use anyhow::Context;
use indicatif::ProgressBar;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::Type;
use postgres::{Client, NoTls};

use crate::row::{Row, Value};
use crate::writer::DBWriter;
use crate::{channel::Sender, reader::DBReader};

pub struct PostgresDB {
    client: Client,
}

impl PostgresDB {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let client = Client::connect(uri, NoTls)?;
        return Ok(Self { client });
    }
}

impl DBReader for PostgresDB {
    fn get_count(&mut self, table: &str) -> anyhow::Result<u32> {
        let query = format!("select count(1) from {table}");
        let size: i64 = self.client.query_one(&query, &[])?.get(0);
        return Ok(size.try_into()?);
    }

    fn start_reading(
        &mut self,
        sender: Sender,
        table: &str,
        progress: ProgressBar,
    ) -> anyhow::Result<()> {
        let query = format!("select * from {table}");
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        let columns = stmt.columns();
        let mut rows = self
            .client
            .query_raw(&stmt, &[] as &[&str; 0])
            .context("Failed to get data from postgres source")?;

        while let Some(row) = rows
            .next()
            .context("Error while reading data from postgres")?
        {
            let mut result: Row = Vec::with_capacity(columns.len());
            for (idx, column) in columns.iter().enumerate() {
                let value = match column.type_() {
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
                    _ => panic!("Type doesn't supported"),
                };
                result.push(value);
            }
            sender
                .send(result)
                .context("Failed to send data to queue")?;
            progress.inc(1);
        }
        progress.finish();
        return Ok(());
    }
}

impl Value {
    fn write_postgres_bytes(
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
                let bytes = string.clone().into_bytes();
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(&bytes)?;
            }
            (&Type::TIME, &Value::I64(num)) => {
                let num = i16::try_from(num)?;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            _ => return Err(anyhow::anyhow!("Unsuppoerted type conversion")),
        };
        return Ok(());
    }
}

// Binary COPY signature (first 15 bytes)
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

impl DBWriter for PostgresDB {
    fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()> {
        let query = format!("select * from {table}");
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        let columns = stmt.columns();

        let query = format!("COPY {table} FROM STDIN WITH BINARY");
        let mut writer = self
            .client
            .copy_in(&query)
            .context("Failed to star writing data into postgres")?;

        writer.write_all(BINARY_SIGNATURE)?;

        // Flags (4 bytes).
        writer.write_all(&0_i32.to_be_bytes())?;

        // Header extension length (4 bytes)
        writer.write_all(&0_i32.to_be_bytes())?;

        for row in batch {
            // Num of fields
            writer.write_all(&(row.len() as i16).to_be_bytes())?;
            for (value, column) in std::iter::zip(row, columns) {
                value.write_postgres_bytes(column.type_(), &mut writer)?;
            }
        }
        writer
            .finish()
            .context("Failed to finish writing to postgres")?;
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_null() {
        let mut buffer = Vec::new();
        Value::Null
            .write_postgres_bytes(&Type::INT4, &mut buffer)
            .unwrap();
        assert_eq!(buffer, (-1_i32).to_be_bytes().to_vec());
    }

    #[test]
    fn test_write_int8() {
        let mut buffer = Vec::new();
        Value::I64(42)
            .write_postgres_bytes(&Type::INT8, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(8_i32).to_be_bytes());
        expected.extend(&42_i64.to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_int4() {
        let mut buffer = Vec::new();
        Value::I64(42)
            .write_postgres_bytes(&Type::INT4, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(4_i32).to_be_bytes());
        expected.extend(&(42_i32).to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_int2() {
        let mut buffer = Vec::new();
        Value::I64(42)
            .write_postgres_bytes(&Type::INT2, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(2_i32).to_be_bytes());
        expected.extend(&(42_i16).to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_float8() {
        let mut buffer = Vec::new();
        Value::F64(3.14)
            .write_postgres_bytes(&Type::FLOAT8, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(8_i32).to_be_bytes());
        expected.extend(&3.14f64.to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_float4() {
        let mut buffer = Vec::new();
        Value::F64(3.14)
            .write_postgres_bytes(&Type::FLOAT4, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(4_i32).to_be_bytes());
        expected.extend(&(3.14f32).to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_bytea() {
        let mut buffer = Vec::new();
        Value::Bytes(vec![1, 2, 3])
            .write_postgres_bytes(&Type::BYTEA, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(3_i32).to_be_bytes());
        expected.extend(&[1, 2, 3]);
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_text() {
        let mut buffer = Vec::new();
        Value::String("hello".into())
            .write_postgres_bytes(&Type::TEXT, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(5_i32).to_be_bytes());
        expected.extend("hello".as_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_varchar() {
        let mut buffer = Vec::new();
        Value::String("world".into())
            .write_postgres_bytes(&Type::VARCHAR, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(5_i32).to_be_bytes());
        expected.extend("world".as_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_unsupported_conversion() {
        let mut buffer = Vec::new();
        let result = Value::I64(42).write_postgres_bytes(&Type::BYTEA, &mut buffer);
        assert!(result.is_err());
    }
}
