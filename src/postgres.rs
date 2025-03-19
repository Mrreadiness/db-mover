use std::io::Write;

use anyhow::Context;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::Type;
use postgres::{Client, NoTls};

use crate::channel::Reciever;
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
    fn start_reading(&mut self, sender: Sender, table: &str) -> anyhow::Result<()> {
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
        }
        return Ok(());
    }
}

// Binary COPY signature (first 15 bytes)
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

impl DBWriter for PostgresDB {
    fn start_writing(&mut self, reciever: Reciever, table: &str) -> anyhow::Result<()> {
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

        while let Ok(row) = reciever.recv() {
            // Num of fields
            writer.write_all(&(row.len() as i16).to_be_bytes())?;
            for value in row {
                match value {
                    Value::String(string) => {
                        let bytes = string.into_bytes();
                        writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                        writer.write_all(&bytes)?;
                    }
                    Value::Bytes(bytes) => {
                        writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                        writer.write_all(&bytes)?;
                    }
                    Value::I64(num) => {
                        writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                        writer.write_all(&num.to_be_bytes())?;
                    }
                    Value::F64(num) => {
                        writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                        writer.write_all(&num.to_be_bytes())?;
                    }
                    Value::Null => writer.write_all(&(-1_i32).to_be_bytes())?,
                };
            }
        }
        writer
            .finish()
            .context("Failed to finish writing to postgres")?;
        return Ok(());
    }
}
