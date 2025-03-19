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

impl DBWriter for PostgresDB {
    fn start_writing(&mut self, reciever: Reciever, table: &str) -> anyhow::Result<()> {
        let query = format!("COPY {table} FROM STDIN");
        let mut writer = self
            .client
            .copy_in(&query)
            .context("Failed to star writing data into postgres")?;
        let mut itoa_buffer = itoa::Buffer::new();
        let mut ryu_buffer = ryu::Buffer::new();
        while let Ok(row) = reciever.recv() {
            let mut serialized_row: Vec<u8> = Vec::with_capacity(row.len() * 8);
            for value in row {
                match value {
                    Value::String(string) => serialized_row.extend(string.into_bytes()),
                    Value::Bytes(bytes) => serialized_row.extend(bytes),
                    Value::I64(num) => serialized_row.extend(itoa_buffer.format(num).as_bytes()),
                    Value::F64(num) => serialized_row.extend(ryu_buffer.format(num).as_bytes()),
                    Value::Null => serialized_row.extend_from_slice(b"\\N"),
                };
                serialized_row.push(b'\t');
            }
            serialized_row.pop();
            serialized_row.push(b'\n');

            writer.write_all(&serialized_row)?;
        }
        writer.finish()?;
        return Ok(());
    }
}
