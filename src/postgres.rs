use anyhow::Context;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::Type;
use postgres::{Client, NoTls};

use crate::row::{Row, Value};
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
