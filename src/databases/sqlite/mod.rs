use std::str::FromStr;

use anyhow::Context;
use rusqlite::{params_from_iter, Connection, OpenFlags};

use crate::databases::{
    table::{Row, Value},
    traits::{DBInfoProvider, DBReader, DBWriter},
};

use super::{
    table::{Column, TableInfo},
    traits::ReaderIterator,
};

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
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo> {
        let mut num_rows = None;
        if !no_count {
            let query = format!("select count(1) from {table}");
            num_rows = Some(
                self.connection
                    .query_row(&query, [], |row| row.get::<_, u32>(0))
                    .context("Failed to get number of rows in the table")?
                    .into(),
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

#[ouroboros::self_referencing]
struct SqliteRowsIter<'a> {
    columns: Vec<Column>,
    stmt: rusqlite::Statement<'a>,

    #[borrows(mut stmt)]
    #[covariant]
    rows: rusqlite::Rows<'this>,
}

impl Iterator for SqliteRowsIter<'_> {
    type Item = anyhow::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(
            |fields| match fields.rows.next().context("Failed to read a row") {
                Ok(Some(row)) => {
                    let columns = fields.columns;
                    let mut result: Row = Vec::with_capacity(columns.len());
                    for (idx, column) in columns.iter().enumerate() {
                        match row
                            .get_ref(idx)
                            .context("Failed to read data from the row")
                            .and_then(|raw| Value::try_from((column, raw)))
                        {
                            Ok(value) => result.push(value),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    return Some(Ok(result));
                }
                Ok(None) => return None,
                Err(e) => return Some(Err(e)),
            },
        )
    }
}

impl DBReader for SqliteDB {
    fn read_iter<'a>(&'a mut self, table: &str) -> anyhow::Result<ReaderIterator<'a>> {
        let columns = self.get_columns(table)?;
        let query = format!("select * from {table}");
        let stmt = self
            .connection
            .prepare(&query)
            .context("Failed to create read query")?;
        let iterator = SqliteRowsIterTryBuilder {
            columns,
            stmt,
            rows_builder: |stmt| {
                return stmt.query([]).context("Failed to read rows");
            },
        }
        .try_build()?;
        return Ok(Box::new(iterator));
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
