use anyhow::Context;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, TxOpts, params};
use tracing::debug;
pub use value::MysqlTypeOptions;

use crate::databases::table::{Row, Value};
use crate::databases::traits::{DBInfoProvider, DBReader};

use super::table::{Column, ColumnType, TableInfo};
use super::traits::{DBWriter, ReaderIterator, WriterError};

mod value;

pub struct MysqlDB {
    uri: String,
    connection: Conn,
    type_options: MysqlTypeOptions,
}

impl MysqlDB {
    pub fn new(uri: &str, type_options: MysqlTypeOptions) -> anyhow::Result<Self> {
        let connection = Self::connect(uri)?;
        debug!("Connected to mysql {uri}");
        return Ok(Self {
            uri: uri.to_string(),
            connection,
            type_options,
        });
    }

    fn connect(uri: &str) -> Result<Conn, anyhow::Error> {
        let opts = Opts::from_url(uri)?;
        let mut conn = Conn::new(opts)?;
        conn.query_drop("SET time_zone = 'UTC'")
            .context("Failed to set UTC timezone")?;
        return Ok(conn);
    }

    fn get_num_rows(&mut self, table: &str) -> anyhow::Result<u64> {
        let count_query = format!("SELECT count(1) FROM {table}");
        return self
            .connection
            .query_first(count_query)?
            .context("Unable to get count of rows for table");
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

        let info_rows: Vec<mysql::Row> = self.connection.exec(r"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE 
                                                                FROM INFORMATION_SCHEMA.COLUMNS 
                                                                WHERE table_name = :table AND TABLE_SCHEMA = database()
                                                                ORDER BY ORDINAL_POSITION", params! {table})?;
        let mut columns = Vec::with_capacity(info_rows.len());
        for row in info_rows {
            let name = row
                .get_opt(0)
                .context("Value expected")?
                .context("Couldn't parse column name")?;
            let column_type: String = row
                .get_opt(1)
                .context("Value expected")?
                .context("Couldn't parse column type")?;
            let nullable: String = row
                .get_opt(2)
                .context("Value expected")?
                .context("Couldn't parse column nullable")?;
            columns.push(Column {
                name,
                column_type: ColumnType::try_from_mysql_type(&column_type, &self.type_options)?,
                nullable: nullable.as_str() == "YES",
            });
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

impl DBWriter for MysqlDB {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        return MysqlDB::new(&self.uri, self.type_options.clone())
            .map(|writer| Box::new(writer) as _);
    }

    fn write_batch(&mut self, batch: &[Row], table: &str) -> Result<(), WriterError> {
        let mut trx = self
            .connection
            .start_transaction(TxOpts::default())
            .context("Failed to start mysql stransaction")?;
        let placeholder = format!(
            "({})",
            batch[0].iter().map(|_| "?").collect::<Vec<_>>().join(", ")
        );
        trx.exec_batch(
            format!("INSERT INTO {table} VALUES {placeholder}"),
            batch
                .iter()
                .map(|row| row.iter().map(mysql::Value::from).collect::<Vec<_>>()),
        )
        .context("Unable to insert values into mysql")?;
        trx.commit().context("Failed to commit mysql transaction")?;

        return Ok(());
    }

    fn recover(&mut self) -> anyhow::Result<()> {
        debug!("Trying to reconnect to the mysql");
        self.connection = Self::connect(&self.uri)?;
        debug!("Successfully reconnected to the mysql");
        return Ok(());
    }
}
