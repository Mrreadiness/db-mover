use anyhow::Context;
use rusqlite::{params_from_iter, types::ValueRef, Connection, OpenFlags, ToSql};

use crate::{
    channel::{Reciever, Sender},
    reader::DBReader,
    row::Row,
    row::Value,
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

    fn write_batch(&self, batch: &[Row], table: &str) -> anyhow::Result<()> {
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
    fn start_reading(&mut self, sender: Sender, table: &str) -> anyhow::Result<()> {
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
        }
        return Ok(());
    }
}

impl DBWriter for SqliteDB {
    fn start_writing(&self, reciever: Reciever, table: &str) -> anyhow::Result<()> {
        let batch_size = 100_000;
        let mut batch: Vec<Row> = Vec::with_capacity(batch_size);
        while let Ok(row) = reciever.recv() {
            batch.push(row);
            if batch.len() == batch_size {
                self.write_batch(&batch, table)?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.write_batch(&batch, table)?;
        }
        return Ok(());
    }
}
