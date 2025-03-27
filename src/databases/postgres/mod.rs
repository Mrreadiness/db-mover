use std::io::Write;

use anyhow::Context;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, NoTls};

use crate::databases::table::{Row, Value};
use crate::databases::traits::{DBInfoProvider, DBReader, DBWriter};

use super::table::{Column, ColumnType, TableInfo};
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

    fn get_num_rows(&mut self, table: &str) -> anyhow::Result<u64> {
        let count_query = format!("SELECT count(1) FROM {table}");
        return self
            .client
            .query_one(&count_query, &[])?
            .get::<_, i64>(0)
            .try_into()
            .context("Failed to convert i64 to u64");
    }

    fn get_columns(&mut self, table: &str) -> anyhow::Result<Vec<Column>> {
        let mut columns = Vec::new();
        let rows = self
            .client
            .query(
                "SELECT column_name, is_nullable
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position",
                &[&table],
            )
            .context("Failed to query information about table")?;
        for row in rows {
            let is_nullable: &str = row.get(1);
            columns.push(Column {
                name: row.get(0),
                column_type: ColumnType::I64, // Temp default
                nullable: is_nullable == "YES",
            })
        }
        let column_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let query = format!("SELECT {} FROM {}", column_names.join(", "), table);
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        assert!(
            columns.len() == stmt.columns().len(),
            "Broken invariant. Expected to get {} column infos, got {}",
            columns.len(),
            stmt.columns().len()
        );
        for (column, column_info) in std::iter::zip(columns.iter_mut(), stmt.columns()) {
            assert!(
                column.name == column_info.name(),
                "Broken invariant. Expected to get {} column, got {}",
                column.name,
                column_info.name()
            );
            column.column_type = column_info.type_().try_into()?;
        }
        return Ok(columns);
    }
}

impl DBInfoProvider for PostgresDB {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo> {
        let mut num_rows = None;
        if !no_count {
            num_rows = Some(
                self.get_num_rows(table)
                    .context("Failed to get number of rows in the table")?,
            );
        }
        let columns = self
            .get_columns(table)
            .context("Failed to get info about table columns")?;
        return Ok(TableInfo {
            name: table.to_string(),
            num_rows,
            columns,
        });
    }
}

struct PostgresRowsIter<'a> {
    target_format: TableInfo,
    rows: postgres::RowIter<'a>,
}

impl Iterator for PostgresRowsIter<'_> {
    type Item = anyhow::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        return match self
            .rows
            .next()
            .context("Error while reading data from postgres")
        {
            Ok(Some(row)) => {
                let mut result: Row = Vec::with_capacity(self.target_format.columns.len());
                for (idx, column) in self.target_format.columns.iter().enumerate() {
                    match Value::try_from((column.column_type, &row, idx)) {
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
    fn read_iter(&mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'_>> {
        let query = format!(
            "SELECT {} FROM {}",
            target_format.column_names().join(", "),
            target_format.name
        );
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        let rows = self
            .client
            .query_raw(&stmt, &[] as &[&str; 0])
            .context("Failed to get data from postgres source")?;
        return Ok(Box::new(PostgresRowsIter {
            target_format,
            rows,
        }));
    }
}

// Binary COPY signature (first 15 bytes)
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

impl DBWriter for PostgresDB {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        return PostgresDB::new(&self.uri).map(|writer| Box::new(writer) as _);
    }

    fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()> {
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
            for value in row {
                value.write_postgres_bytes(&mut writer)?;
            }
        }
        writer.write_all(&(-1_i16).to_be_bytes())?;
        writer
            .finish()
            .context("Failed to finish writing to postgres")?;
        return Ok(());
    }
}
