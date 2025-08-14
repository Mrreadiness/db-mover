use std::collections::HashMap;
use std::io::Write;

use anyhow::Context;
use itertools::izip;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, NoTls};
use tracing::debug;
use type_converter::PostgresColumn;

use crate::databases::postgres::type_converter::{
    PostgresReadInput, PostgresTypeConverter, PostgresWriteInput,
};
use crate::databases::table::Row;
use crate::databases::traits::{DBInfoProvider, DBReader, DBWriter};
use crate::databases::type_converter::DefaultTypeConverter;

use super::table::{Column, TableInfo};
use super::traits::{ReaderIterator, WriterError};

pub mod type_converter;

pub struct PostgresDB<TypeConverterT: PostgresTypeConverter = DefaultTypeConverter> {
    uri: String,
    client: Client,
    table_columns_cache: HashMap<String, Vec<PostgresColumn>>,
    _type_converter: std::marker::PhantomData<TypeConverterT>,
}

impl<TypeConverterT: PostgresTypeConverter> PostgresDB<TypeConverterT> {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let client = Self::connect(uri)?;
        debug!("Connected to postgres {uri}");
        return Ok(Self {
            client,
            uri: uri.to_string(),
            table_columns_cache: HashMap::default(),
            _type_converter: std::marker::PhantomData,
        });
    }

    fn connect(uri: &str) -> Result<Client, postgres::Error> {
        return Client::connect(uri, NoTls);
    }

    fn get_num_rows(&mut self, table: &str) -> anyhow::Result<u64> {
        let count_query = format!("SELECT count(1) FROM {table}");
        return self
            .client
            .query_one(&count_query, &[])?
            .get::<_, i64>(0)
            .try_into()
            .context("Failed to convert i64 to u64");
    }

    fn get_columns(&mut self, table: &str) -> anyhow::Result<Vec<PostgresColumn>> {
        let mut columns = Vec::new();
        let rows = self
            .client
            .query(
                "SELECT column_name, is_nullable
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = current_schema
            ORDER BY ordinal_position",
                &[&table],
            )
            .context("Failed to query information about table")?;
        for row in rows {
            let is_nullable: &str = row.get(1);
            columns.push(PostgresColumn {
                table: table.to_string(),
                name: row.get(0),
                column_type: postgres::types::Type::UNKNOWN, // Temp default
                nullable: is_nullable == "YES",
            })
        }
        let column_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let query = format!("SELECT {} FROM {}", column_names.join(", "), table);
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        assert!(
            columns.len() == stmt.columns().len(),
            "Broken invariant. Expected to get {} column infos, got {}",
            columns.len(),
            stmt.columns().len()
        );
        for (column, column_info) in std::iter::zip(columns.iter_mut(), stmt.columns()) {
            assert!(
                column.name == column_info.name(),
                "Broken invariant. Expected to get {} column, got {}",
                column.name,
                column_info.name()
            );
            column.column_type = column_info.type_().clone();
        }
        return Ok(columns);
    }

    fn get_columns_cached(&mut self, table: &str) -> anyhow::Result<&Vec<PostgresColumn>> {
        if !self.table_columns_cache.contains_key(table) {
            let columns = self.get_columns(table)?;
            self.table_columns_cache.insert(table.to_string(), columns);
        }
        return Ok(self.table_columns_cache.get(table).expect("Can't be empty"));
    }
}

impl<TypeConverterT: PostgresTypeConverter> DBInfoProvider for PostgresDB<TypeConverterT> {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo> {
        let mut num_rows = None;
        if !no_count {
            num_rows = Some(
                self.get_num_rows(table)
                    .context("Failed to get number of rows in the table")?,
            );
        }
        let postgres_columns = self
            .get_columns_cached(table)
            .context("Failed to get info about table columns")?;
        let columns = postgres_columns
            .iter()
            .map(TypeConverterT::postgres_column)
            .collect::<anyhow::Result<Vec<Column>>>()
            .context("Failed to parse info about table columns")?;
        return Ok(TableInfo {
            name: table.to_string(),
            num_rows,
            columns,
        });
    }

    fn get_tables(&mut self) -> anyhow::Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema",
                &[]
            )
            .context("Failed to query tables")?;
        return rows
            .iter()
            .map(|row| {
                row.try_get::<_, String>(0)
                    .context("Failed to read table name")
            })
            .collect::<anyhow::Result<Vec<String>>>();
    }
}

struct PostgresRowsIter<'a, TypeConverterT: PostgresTypeConverter> {
    target_format: TableInfo,
    rows: postgres::RowIter<'a>,
    _type_converter: std::marker::PhantomData<TypeConverterT>,
}

impl<TypeConverterT: PostgresTypeConverter> Iterator for PostgresRowsIter<'_, TypeConverterT> {
    type Item = anyhow::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        return match self
            .rows
            .next()
            .context("Error while reading data from postgres")
        {
            Ok(Some(row)) => {
                let mut result: Row = Vec::with_capacity(self.target_format.columns.len());
                for (idx, column) in self.target_format.columns.iter().enumerate() {
                    match TypeConverterT::postgres_read_value(PostgresReadInput {
                        table: &self.target_format.name,
                        column,
                        row: &row,
                        column_index: idx,
                    }) {
                        Ok(val) => result.push(val),
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Ok(result))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        };
    }
}

impl<TypeConverterT: PostgresTypeConverter> DBReader for PostgresDB<TypeConverterT> {
    fn read_iter(&mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'_>> {
        let query = format!(
            "SELECT {} FROM {}",
            target_format.column_names().join(", "),
            target_format.name
        );
        let stmt = self
            .client
            .prepare(&query)
            .context("Failed to prepare select statement")?;
        let rows = self
            .client
            .query_raw(&stmt, &[] as &[&str; 0])
            .context("Failed to get data from postgres source")?;
        return Ok(Box::new(PostgresRowsIter {
            target_format,
            rows,
            _type_converter: std::marker::PhantomData::<TypeConverterT>,
        }));
    }
}

// Binary COPY signature (first 15 bytes)
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

impl<TypeConverterT: PostgresTypeConverter> DBWriter for PostgresDB<TypeConverterT> {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        let new: PostgresDB<TypeConverterT> = PostgresDB::new(&self.uri)?;
        return Ok(Box::new(new));
    }

    fn write_batch(&mut self, batch: &[Row], table: &TableInfo) -> Result<(), WriterError> {
        let postgres_columns = self
            .get_columns_cached(&table.name)
            .context("Failed to get info about table columns")?;
        let columns = postgres_columns
            .iter()
            .map(TypeConverterT::postgres_column)
            .collect::<anyhow::Result<Vec<Column>>>()
            .context("Failed to parse info about table columns")?;
        let actual_column_types = postgres_columns
            .iter()
            .map(|c| c.column_type.clone())
            .collect::<Vec<postgres::types::Type>>();
        let query = format!("COPY {} FROM STDIN WITH BINARY", table.name);
        let mut writer = self
            .client
            .copy_in(&query)
            .context("Failed to start writing data into postgres")?;

        writer.write_all(BINARY_SIGNATURE)?;

        // Flags (4 bytes).
        writer.write_all(&0_i32.to_be_bytes())?;

        // Header extension length (4 bytes)
        writer.write_all(&0_i32.to_be_bytes())?;

        for row in batch {
            // Number of fields
            writer.write_all(&(row.len() as i16).to_be_bytes())?;
            assert_eq!(
                columns.len(),
                row.len(),
                "Number of columns should be equal number of value in a row"
            );
            for (value, column, actual_column_type) in izip!(row, &columns, &actual_column_types) {
                TypeConverterT::postgres_write_value(
                    &mut writer,
                    PostgresWriteInput {
                        table: &table.name,
                        value,
                        column,
                        actual_column_type: actual_column_type.clone(),
                    },
                )?;
            }
        }
        writer.write_all(&(-1_i16).to_be_bytes())?;
        writer
            .finish()
            .context("Failed to finish writing to postgres")?;
        return Ok(());
    }

    fn recover(&mut self) -> anyhow::Result<()> {
        debug!("Trying to reconnect to the postgres");
        self.client = Self::connect(&self.uri)?;
        debug!("Successfully reconnected to the postgres");
        return Ok(());
    }
}
