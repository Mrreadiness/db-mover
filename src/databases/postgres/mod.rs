use std::io::Write;

use anyhow::Context;
use indicatif::ProgressBar;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, NoTls};

use crate::channel::Sender;
use crate::databases::table::{Row, Value};
use crate::databases::traits::{DBInfoProvider, DBReader, DBWriter};

use super::table::Table;

mod value;

pub struct PostgresDB {
    client: Client,
}

impl PostgresDB {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let client = Client::connect(uri, NoTls)?;
        return Ok(Self { client });
    }
}

impl DBInfoProvider for PostgresDB {
    fn get_table_info(&mut self, table: &str) -> anyhow::Result<Table> {
        let count_query = format!("select count(1) from {table}");
        let size: i64 = self.client.query_one(&count_query, &[])?.get(0);
        return Ok(Table::new(table.to_string(), size.try_into()?));
    }
}

impl DBReader for PostgresDB {
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
                result.push(Value::try_from((column.type_(), &row, idx))?);
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
            // Count of fields
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
