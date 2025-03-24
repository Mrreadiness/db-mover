use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use indicatif::ProgressBar;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, NoTls};

use crate::channel::Sender;
use crate::databases::table::{Row, Value};
use crate::databases::traits::{DBInfoProvider, DBReader, DBWriter};
use crate::error::Error;

use super::table::Table;

mod value;

pub struct PostgresDB {
    uri: String,
    client: Client,
}

impl PostgresDB {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let client = Client::connect(uri, NoTls)?;
        return Ok(Self {
            client,
            uri: uri.to_string(),
        });
    }
}

impl DBInfoProvider for PostgresDB {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<Table> {
        let mut size = None;
        if !no_count {
            let count_query = format!("select count(1) from {table}");
            size = Some(
                self.client
                    .query_one(&count_query, &[])?
                    .get::<_, i64>(0)
                    .try_into()?,
            );
        }
        return Ok(Table::new(table.to_string(), size));
    }
}

impl DBReader for PostgresDB {
    fn start_reading(
        &mut self,
        sender: Sender,
        table: &str,
        progress: ProgressBar,
        stopped: &AtomicBool,
    ) -> Result<(), Error> {
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
            if stopped.load(Ordering::Relaxed) {
                return Err(Error::Stopped);
            }
            let mut result: Row = Vec::with_capacity(columns.len());
            for (idx, column) in columns.iter().enumerate() {
                result.push(Value::try_from((column.type_(), &row, idx))?);
            }
            sender.send(result).map_err(|_| Error::Stopped)?;
            progress.inc(1);
        }
        progress.finish();
        return Ok(());
    }
}

// Binary COPY signature (first 15 bytes)
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

impl DBWriter for PostgresDB {
    fn opt_clone(&self) -> Option<anyhow::Result<Box<dyn DBWriter>>> {
        return Some(PostgresDB::new(&self.uri).map(|writer| Box::new(writer) as _));
    }

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
        writer.write_all(&(-1_i16).to_be_bytes())?;
        writer
            .finish()
            .context("Failed to finish writing to postgres")?;
        return Ok(());
    }
}
