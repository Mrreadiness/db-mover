use std::str::FromStr;

use anyhow::Context;
use rusqlite::{params_from_iter, Connection, OpenFlags};
use tracing::debug;

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
        debug!("Connected to sqlite {uri}");
        return Ok(SqliteDB { connection: conn });
    }

    fn get_columns(&mut self, table: &str) -> anyhow::Result<Vec<Column>> {
        let mut stmt = self
            .connection
            .prepare("SELECT name, type, `notnull` FROM pragma_table_info WHERE arg=?")?;
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
    fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()> {
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
