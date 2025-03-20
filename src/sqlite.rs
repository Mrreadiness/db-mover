use anyhow::Context;
use indicatif::ProgressBar;
use rusqlite::{params_from_iter, types::ValueRef, Connection, OpenFlags, ToSql};

use crate::{
    channel::Sender,
    reader::DBReader,
    row::{Row, Value},
    writer::DBWriter,
};

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
}

impl TryFrom<ValueRef<'_>> for Value {
    type Error = anyhow::Error;

    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        let parsed = match value {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(val) => Value::I64(val),
            ValueRef::Real(val) => Value::F64(val),
            ValueRef::Text(val) => {
                let val = std::str::from_utf8(val).context("invalid UTF-8")?;
                Value::String(val.to_string())
            }
            ValueRef::Blob(val) => Value::Bytes(val.to_vec()),
        };
        return Ok(parsed);
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Value::Null => None::<i32>.to_sql(),
            Value::I64(val) => val.to_sql(),
            Value::F64(val) => val.to_sql(),
            Value::String(val) => val.to_sql(),
            Value::Bytes(val) => val.to_sql(),
        }
    }
}

impl DBReader for SqliteDB {
    fn get_count(&mut self, table: &str) -> anyhow::Result<u32> {
        let query = format!("select count(1) from {table}");
        return Ok(self.connection.query_row(&query, [], |row| row.get(0))?);
    }

    fn start_reading(
        &mut self,
        sender: Sender,
        table: &str,
        progress: ProgressBar,
    ) -> anyhow::Result<()> {
        let query = format!("select * from {table}");
        let mut stmt = self
            .connection
            .prepare(&query)
            .context("Failed to create read query")?;
        let column_count = stmt.column_count();
        let mut rows = stmt.query([]).context("Failed to read rows")?;
        while let Ok(Some(row)) = rows.next() {
            let mut result: Row = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                result.push(Value::try_from(
                    row.get_ref(idx).context("Failed to read vaule")?,
                )?);
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
