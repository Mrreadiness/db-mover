use std::collections::HashMap;

use anyhow::Context;
use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, params};
use tracing::debug;
pub use type_converter::MysqlTypeOptions;

use crate::databases::mysql::type_converter::{
    MysqlColumn, MysqlConstraint, MysqlTypeConverter, MysqlWriteInput,
};
use crate::databases::table::Row;
use crate::databases::traits::{DBInfoProvider, DBReader};
use crate::databases::type_converter::DefaultTypeConverter;

use super::table::TableInfo;
use super::traits::{DBWriter, ReaderIterator, WriterError};

pub mod type_converter;

pub struct MysqlDB<TypeConverterT: MysqlTypeConverter = DefaultTypeConverter> {
    uri: String,
    connection: Conn,
    type_options: MysqlTypeOptions,
    stmt_cache: HashMap<(String, usize, usize), mysql::Statement>,
    _type_converter: std::marker::PhantomData<TypeConverterT>,
}

impl<TypeConverterT: MysqlTypeConverter> MysqlDB<TypeConverterT> {
    pub fn new(uri: &str, type_options: MysqlTypeOptions) -> anyhow::Result<Self> {
        let connection = Self::connect(uri)?;
        debug!("Connected to mysql {uri}");
        return Ok(Self {
            uri: uri.to_string(),
            connection,
            type_options,
            stmt_cache: HashMap::new(),
            _type_converter: std::marker::PhantomData,
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

    fn get_table_constraints(&mut self, table: &str) -> anyhow::Result<Vec<MysqlConstraint>> {
        return self
            .connection
            .exec(
                r"SELECT
                    tc.CONSTRAINT_NAME,
                    tc.CONSTRAINT_TYPE,
                    cc.CHECK_CLAUSE
                  FROM
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                  LEFT JOIN
                    INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
                    ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    AND cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                  WHERE tc.CONSTRAINT_SCHEMA = database() and tc.TABLE_NAME = :table",
                params! {table},
            )?
            .into_iter()
            .map(|row: mysql::Row| {
                let name: String = row
                    .get_opt(0)
                    .context("Value expected")?
                    .context("Couldn't parse constraint name")?;
                let constraint_type: String = row
                    .get_opt(1)
                    .context("Value expected")?
                    .context("Couldn't parse constraint type")?;
                let clause: Option<String> = row
                    .get_opt(2)
                    .context("Value expected")?
                    .context("Couldn't parse constraint clause")?;
                Ok(MysqlConstraint {
                    name,
                    constraint_type,
                    clause,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .context("Failed to get infomration about table constraints");
    }
}

impl<TypeConverterT: MysqlTypeConverter> DBInfoProvider for MysqlDB<TypeConverterT> {
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
        let constraints = self.get_table_constraints(table)?;
        let mut columns = Vec::with_capacity(info_rows.len());
        for row in info_rows {
            let column_name = row
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
            columns.push(TypeConverterT::mysql_column(MysqlColumn {
                table,
                name: column_name,
                column_type,
                nullable: nullable.as_str() == "YES",
                options: &self.type_options,
                table_constraints: &constraints,
            })?);
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

struct MysqlRowsIter<'a, TypeConverterT: MysqlTypeConverter> {
    target_format: TableInfo,
    rows: mysql::QueryResult<'a, 'a, 'a, mysql::Text>,

    type_converter: std::marker::PhantomData<TypeConverterT>,
}

impl<TypeConverterT: MysqlTypeConverter> Iterator for MysqlRowsIter<'_, TypeConverterT> {
    type Item = anyhow::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        return match self.rows.next() {
            Some(Ok(row)) => {
                let mut result: Row = Vec::with_capacity(self.target_format.columns.len());
                let values = row.unwrap();
                assert_eq!(values.len(), self.target_format.columns.len());
                for (column, value) in std::iter::zip(&self.target_format.columns, values) {
                    match TypeConverterT::mysql_read_value(type_converter::MysqlReadInput {
                        table: &self.target_format.name,
                        column,
                        value,
                    }) {
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

impl<TypeConverterT: MysqlTypeConverter> DBReader for MysqlDB<TypeConverterT> {
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
            type_converter: std::marker::PhantomData::<TypeConverterT>,
        }));
    }
}

impl<TypeConverterT: MysqlTypeConverter> DBWriter for MysqlDB<TypeConverterT> {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        let new: MysqlDB<TypeConverterT> = MysqlDB::new(&self.uri, self.type_options.clone())?;
        return Ok(Box::new(new));
    }

    fn write_batch(&mut self, batch: &[Row], table: &TableInfo) -> Result<(), WriterError> {
        let stmt = self.get_stmt(&table.name, batch[0].len(), batch.len())?;
        let mut values = Vec::with_capacity(batch[0].len() * batch.len());
        for row in batch {
            for (value, column) in std::iter::zip(row, &table.columns) {
                values.push(
                    TypeConverterT::mysql_write_value(MysqlWriteInput {
                        table: &table.name,
                        column,
                        value,
                    })
                    .context("Faild to convert data for mysql")?,
                );
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
