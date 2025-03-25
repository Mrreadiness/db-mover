use std::io::Write;

use anyhow::Context;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, NoTls};

use crate::databases::table::{Row, Value};
use crate::databases::traits::{DBInfoProvider, DBReader, DBWriter};

use super::table::Table;
use super::traits::ReaderIterator;

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

struct PostgresRowsIter<'a> {
    stmt: postgres::Statement,
    rows: postgres::RowIter<'a>,
}

impl Iterator for PostgresRowsIter<'_> {
    type Item = anyhow::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        let columns = self.stmt.columns();
        return match self
            .rows
            .next()
            .context("Error while reading data from postgres")
        {
            Ok(Some(row)) => {
                let mut result: Row = Vec::with_capacity(columns.len());
                for (idx, column) in columns.iter().enumerate() {
                    match Value::try_from((column.type_(), &row, idx)) {
                        Ok(val) => result.push(val),
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Ok(result))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        };
    }
}

impl DBReader for PostgresDB {
    fn read_iter<'a>(&'a mut self, table: &str) -> anyhow::Result<ReaderIterator<'a>> {
        let query = format!("select * from {table}");
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        let rows = self
            .client
            .query_raw(&stmt, &[] as &[&str; 0])
            .context("Failed to get data from postgres source")?;
        return Ok(Box::new(PostgresRowsIter { stmt, rows }));
    }
}

// Binary COPY signature (first 15 bytes)
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

impl DBWriter for PostgresDB {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        return PostgresDB::new(&self.uri).map(|writer| Box::new(writer) as _);
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
