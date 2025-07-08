use std::str::FromStr;

use anyhow::Context;
use rusqlite::{Connection, OpenFlags, params_from_iter};
use tracing::debug;

use crate::databases::{
    table::{Row, Value},
    traits::{DBInfoProvider, DBReader, DBWriter},
};

use super::{
    table::{Column, TableInfo},
    traits::{ReaderIterator, WriterError},
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
        debug!("Connected to sqlite {uri}");
        return Ok(SqliteDB { connection: conn });
    }

    fn get_columns(&mut self, table: &str) -> anyhow::Result<Vec<Column>> {
        let mut stmt = self.connection.prepare(
            "SELECT name, type, `notnull` FROM pragma_table_info WHERE arg=? ORDER BY cid",
        )?;
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

    fn write_batch_impl(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()> {
        let trx = self
            .connection
            .transaction()
            .context("Failed to open transaction")?;
        {
            let placeholder = format!(
                "({})",
                batch[0].iter().map(|_| "?").collect::<Vec<_>>().join(", ")
            );
            let query = format!("INSERT INTO {table} VALUES {placeholder}");
            let mut stmt = trx
                .prepare(&query)
                .context("Failed to create write query")?;
            for row in batch {
                stmt.execute(params_from_iter(row.iter()))
                    .context("Failed to write data")?;
            }
        }
        trx.commit().context("Failed to commit")?;
        return Ok(());
    }
}

impl DBInfoProvider for SqliteDB {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo> {
        let mut num_rows = None;
        if !no_count {
            let query = format!("SELECT count(1) FROM {table}");
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

    fn get_tables(&mut self) -> anyhow::Result<Vec<String>> {
        let mut stmt = self
            .connection
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .context("Failed to create query for reading table list")?;
        let mut rows = stmt.query([]).context("Failed to query table list")?;
        let mut tables = Vec::new();
        while let Some(row) = rows.next().context("Failed to fetch table list")? {
            tables.push(row.get(0).context("Failed to parse table name")?);
        }
        return Ok(tables);
    }
}

#[ouroboros::self_referencing]
struct SqliteRowsIter<'a> {
    target_format: TableInfo,
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
                    let columns = &fields.target_format.columns;
                    let mut result: Row = Vec::with_capacity(columns.len());
                    for (idx, column) in columns.iter().enumerate() {
                        match row
                            .get_ref(idx)
                            .context("Failed to read data from the row")
                            .and_then(|raw| {
                                Value::try_from((column, raw)).context("Failed to parse input data")
                            }) {
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
    fn read_iter(&mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'_>> {
        let query = format!(
            "SELECT {} FROM {}",
            target_format.column_names().join(", "),
            target_format.name
        );
        let stmt = self
            .connection
            .prepare(&query)
            .context("Failed to create read query")?;
        let iterator = SqliteRowsIterTryBuilder {
            target_format,
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
    fn write_batch(&mut self, batch: &[Row], table: &TableInfo) -> Result<(), WriterError> {
        // SQLite is not network dependent, assume that all errors are Unrecoverable
        return self
            .write_batch_impl(batch, &table.name)
            .map_err(WriterError::Unrecoverable);
    }

    fn recover(&mut self) -> anyhow::Result<()> {
        return Ok(());
    }
}
