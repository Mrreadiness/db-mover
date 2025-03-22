use std::str::FromStr;

use anyhow::Context;
use indicatif::ProgressBar;
use rusqlite::{params_from_iter, Connection, OpenFlags};

use crate::{
    channel::Sender,
    databases::table::{Row, Value},
    databases::traits::{DBInfoProvider, DBReader, DBWriter},
};

use super::table::{Column, Table};

mod value;

pub struct SqliteDB {
    connection: Connection,
}

impl SqliteDB {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let path = uri.replace("sqlite://", "");
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        return Ok(SqliteDB { connection: conn });
    }

    fn get_columns(&mut self, table: &str) -> anyhow::Result<Vec<Column>> {
        let mut stmt = self
            .connection
            .prepare("SELECT name, type, `notnull` FROM pragma_table_info where arg=?")?;
        let mut rows = stmt.query([table])?;
        let mut result = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            result.push(Column {
                name: row.get(0)?,
                column_type: {
                    let type_name: String = row.get(1)?;
                    super::table::ColumnType::from_str(&type_name)?
                },
                nullable: !row.get(2)?,
            });
        }
        return Ok(result);
    }
}

impl DBInfoProvider for SqliteDB {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<Table> {
        let mut size = None;
        if !no_count {
            let query = format!("select count(1) from {table}");
            size = Some(
                self.connection
                    .query_row(&query, [], |row| row.get::<_, u32>(0))?
                    .into(),
            );
        }
        return Ok(Table::new(table.to_string(), size));
    }
}

impl DBReader for SqliteDB {
    fn start_reading(
        &mut self,
        sender: Sender,
        table: &str,
        progress: ProgressBar,
    ) -> anyhow::Result<()> {
        let columns = self.get_columns(table)?;
        let query = format!("select * from {table}");
        let mut stmt = self
            .connection
            .prepare(&query)
            .context("Failed to create read query")?;
        let column_count = stmt.column_count();
        let mut rows = stmt.query([]).context("Failed to read rows")?;
        while let Ok(Some(row)) = rows.next() {
            let mut result: Row = Vec::with_capacity(column_count);
            for (idx, column) in columns.iter().enumerate() {
                let raw = row.get_ref(idx)?;
                result.push(Value::try_from((column, raw))?);
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

impl DBWriter for SqliteDB {
    fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()> {
        let placeholder = format!(
            "({})",
            batch[0].iter().map(|_| "?").collect::<Vec<_>>().join(", ")
        );
        let placeholders = batch
            .iter()
            .map(|_| placeholder.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("insert into {table} values {placeholders}");
        let mut stmt = self
            .connection
            .prepare(&query)
            .context("Failed to create write query")?;
        stmt.execute(params_from_iter(batch.concat().iter()))
            .context("Failed to write data")?;
        return Ok(());
    }
}
