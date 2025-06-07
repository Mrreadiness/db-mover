use std::str::FromStr;

use anyhow::Context;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts};
use tracing::debug;

use crate::databases::table::{Row, Value};
use crate::databases::traits::{DBInfoProvider, DBReader};

use super::table::{Column, ColumnType, TableInfo};
use super::traits::ReaderIterator;

mod value;

pub struct MysqlDB {
    connection: Conn,
}

impl MysqlDB {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let connection = Self::connect(uri)?;
        debug!("Connected to mysql {uri}");
        return Ok(Self { connection });
    }

    fn connect(uri: &str) -> Result<Conn, mysql::Error> {
        let opts = Opts::from_url(uri)?;
        return Conn::new(opts);
    }

    fn get_num_rows(&mut self, table: &str) -> anyhow::Result<u64> {
        let count_query = format!("SELECT count(1) FROM {table}");
        return self
            .connection
            .query_first(count_query)?
            .context("Unable to get count of rows for table");
    }
}

impl TryFrom<mysql::Row> for Column {
    type Error = anyhow::Error;

    fn try_from(value: mysql::Row) -> Result<Self, Self::Error> {
        let name = value
            .get_opt(0)
            .context("Value expected")?
            .context("Couldn't parse column name")?;
        let column_type: String = value
            .get_opt(1)
            .context("Value expected")?
            .context("Couldn't parse column type")?;
        let nullable: String = value
            .get_opt(2)
            .context("Value expected")?
            .context("Couldn't parse column nullable")?;
        return Ok(Column {
            name,
            column_type: ColumnType::from_str(&column_type)?, // TODO: mysql specific
            nullable: nullable.as_str() == "YES",
        });
    }
}

impl DBInfoProvider for MysqlDB {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo> {
        let mut num_rows = None;
        if !no_count {
            num_rows = Some(
                self.get_num_rows(table)
                    .context("Failed to get number of rows in the table")?,
            );
        }

        let info_rows: Vec<mysql::Row> = self.connection.query(format!(r"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
                                                                         FROM INFORMATION_SCHEMA.COLUMNS 
                                                                         WHERE table_name = '{table}' AND TABLE_SCHEMA = database()"))?;
        let mut columns = Vec::with_capacity(info_rows.len());
        for row in info_rows {
            columns.push(Column::try_from(row)?);
        }

        return Ok(TableInfo {
            name: table.to_string(),
            num_rows,
            columns,
        });
    }
}

struct MysqlRowsIter<'a> {
    target_format: TableInfo,
    rows: mysql::QueryResult<'a, 'a, 'a, mysql::Text>,
}

impl Iterator for MysqlRowsIter<'_> {
    type Item = anyhow::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        return match self.rows.next() {
            Some(Ok(row)) => {
                let mut result: Row = Vec::with_capacity(self.target_format.columns.len());
                let values = row.unwrap();
                assert_eq!(values.len(), self.target_format.columns.len());
                for (column, value) in std::iter::zip(&self.target_format.columns, values) {
                    match Value::try_from((column, value)) {
                        Ok(val) => result.push(val),
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Ok(result))
            }
            Some(Err(err)) => Some(Err(err).context("Error while reading data from mysql")),
            None => None,
        };
    }
}

impl DBReader for MysqlDB {
    fn read_iter(&mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'_>> {
        let query = format!(
            "SELECT {} FROM {}",
            target_format.column_names().join(", "),
            target_format.name
        );
        let rows = self
            .connection
            .query_iter(query)
            .context("Failed to get data from mysql source")?;
        return Ok(Box::new(MysqlRowsIter {
            target_format,
            rows,
        }));
    }
}
