use std::collections::HashMap;

use anyhow::Context;
use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, params};
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
    is_mariadb: bool,
    type_options: MysqlTypeOptions,
    stmt_cache: HashMap<(String, usize, usize), mysql::Statement>,
}

impl MysqlDB {
    pub fn new(uri: &str, type_options: MysqlTypeOptions) -> anyhow::Result<Self> {
        let mut connection = Self::connect(uri)?;
        debug!("Connected to mysql {uri}");
        let version: String = connection
            .query_first("SELECT VERSION()")
            .context("Unable to fetch database version")?
            .unwrap();
        return Ok(Self {
            uri: uri.to_string(),
            connection,
            is_mariadb: version.contains("MariaDB"),
            type_options,
            stmt_cache: HashMap::new(),
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

    fn get_stmt(
        &mut self,
        table_name: &str,
        values_per_row: usize,
        rows: usize,
    ) -> anyhow::Result<mysql::Statement> {
        let key = (table_name.to_owned(), values_per_row, rows);
        return match self.stmt_cache.get(&key) {
            Some(stmt) => Ok(stmt.to_owned()),
            None => {
                let placeholder = generate_placeholders(values_per_row, rows);
                let stmt = self
                    .connection
                    .prep(format!("INSERT INTO {table_name} VALUES {placeholder}"))
                    .context("Unable to prepare insert query")?;
                self.stmt_cache.insert(key, stmt.clone());
                Ok(stmt)
            }
        };
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
            let mut column_type: String = row
                .get_opt(1)
                .context("Value expected")?
                .context("Couldn't parse column type")?;
            let nullable: String = row
                .get_opt(2)
                .context("Value expected")?
                .context("Couldn't parse column nullable")?;
            if column_type == "longtext" && self.is_mariadb {
                let num_json_constraints: usize = self.connection.exec_first(
                    r"SELECT count(1) FROM INFORMATION_SCHEMA.check_constraints
                    WHERE CONSTRAINT_SCHEMA = database() AND TABLE_NAME = :table AND CHECK_CLAUSE = :clause",
                    params! {table, "clause" => format!("json_valid(`{name}`)")},
                ).context("Failed to check json constraint")?.unwrap();
                if num_json_constraints > 0 {
                    column_type = String::from("json");
                }
            }
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

    fn get_tables(&mut self) -> anyhow::Result<Vec<String>> {
        let rows: Vec<mysql::Row> = self.connection.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = database()",
        )?;
        return rows
            .iter()
            .map(|row| {
                row.get_opt(0)
                    .context("Value expected")?
                    .context("Couldn't parse table name")
            })
            .collect::<anyhow::Result<Vec<String>>>();
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

    fn write_batch(&mut self, batch: &[Row], table: &TableInfo) -> Result<(), WriterError> {
        let stmt = self.get_stmt(&table.name, batch[0].len(), batch.len())?;
        let mut values = Vec::with_capacity(batch[0].len() * batch.len());
        for row in batch {
            for value in row {
                values.push(mysql::Value::from(value));
            }
        }
        self.connection
            .exec_drop(stmt, mysql::Params::Positional(values))
            .context("Unable to insert values into mysql")?;

        return Ok(());
    }

    fn recover(&mut self) -> anyhow::Result<()> {
        debug!("Trying to reconnect to the mysql");
        self.connection = Self::connect(&self.uri)?;
        debug!("Successfully reconnected to the mysql");
        return Ok(());
    }
}

fn generate_placeholders(values_per_row: usize, rows: usize) -> String {
    use std::fmt::Write;

    let block_inner = std::iter::repeat_n("?", values_per_row).join(", ");
    let mut result = String::with_capacity(rows * (block_inner.len() + 3));
    for i in 0..rows {
        if i > 0 {
            result.push(',');
        }
        result.push('(');
        result.write_str(&block_inner).unwrap();
        result.push(')');
    }
    result
}
