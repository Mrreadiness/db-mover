use crate::databases::mysql::{MysqlDB, MysqlTypeOptions};
use crate::databases::postgres::PostgresDB;
use crate::databases::sqlite::SqliteDB;
use crate::databases::traits::{DBReader, DBWriter};
use crate::uri::URI;
use anyhow::Context;
use clap::Parser;
use tracing::Level;

#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
#[command(next_line_help = true)]
pub struct Args {
    /// Input URI of database
    #[arg(long, short)]
    pub input: URI,

    /// Output URI of database
    #[arg(long, short)]
    pub output: URI,

    /// List of tables
    #[arg(long, short)]
    pub table: Vec<String>,

    /// Size of queue between reader and writers
    #[arg(long, default_value_t = 100_000)]
    pub queue_size: usize,

    /// Number of writer workers (if supported by database)
    #[arg(long, default_value_t = 1)]
    pub writer_workers: usize,

    /// Size of batches used by writer
    #[arg(long, default_value_t = 10_000)]
    pub batch_write_size: usize,

    /// Number of retries to write a batch. Exponential retry is used, with start value 500ms and
    /// factor of 2.
    #[arg(long, default_value_t = 5)]
    pub batch_write_retries: usize,

    /// Disable output
    #[clap(long, action)]
    pub quiet: bool,

    /// Log level
    #[arg(long, default_value_t = Level::INFO)]
    pub log_level: Level,

    /// Disable the COUNT query used for progress tracking.
    /// Progress will be shown but without prognoses.
    /// Only for input table.
    #[clap(long, action)]
    pub no_count: bool,

    /// Check compatibility without moving a data
    #[clap(long, action)]
    pub dry_run: bool,

    /// Disable assumption that binary(16) is UUID for MySQL
    #[clap(long, action)]
    pub no_mysql_binary_16_as_uuid: bool,
}

impl Args {
    pub fn new(input: URI, output: URI) -> Self {
        return Args {
            input,
            output,
            table: Vec::new(),
            queue_size: 100_000,
            writer_workers: 1,
            batch_write_size: 10_000,
            batch_write_retries: 1,
            quiet: true,
            log_level: Level::INFO,
            no_count: false,
            dry_run: false,
            no_mysql_binary_16_as_uuid: false,
        };
    }

    fn build_sqlite(&self, uri: &str) -> anyhow::Result<Box<SqliteDB>> {
        return Ok(Box::new(
            SqliteDB::new(uri).context("Unable to connect to the sqlite")?,
        ));
    }

    fn build_postgres(&self, uri: &str) -> anyhow::Result<Box<PostgresDB>> {
        return Ok(Box::new(
            PostgresDB::new(uri).context("Unable to connect to the postgres")?,
        ));
    }

    fn build_mysql(&self, uri: &str) -> anyhow::Result<Box<MysqlDB>> {
        let options = MysqlTypeOptions {
            binary_16_as_uuid: !self.no_mysql_binary_16_as_uuid,
            ..Default::default()
        };
        let db = MysqlDB::new(uri, options).context("Unable to connect to the mysql")?;
        return Ok(Box::new(db));
    }

    pub fn create_reader(&self) -> anyhow::Result<Box<dyn DBReader>> {
        let reader: Box<dyn DBReader> = match &self.input {
            URI::Sqlite(uri) => self.build_sqlite(uri)?,
            URI::Postgres(uri) => self.build_postgres(uri)?,
            URI::Mysql(uri) => self.build_mysql(uri)?,
        };
        return Ok(reader);
    }

    pub fn create_writer(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        let writer: Box<dyn DBWriter> = match &self.output {
            URI::Sqlite(uri) => self.build_sqlite(uri)?,
            URI::Postgres(uri) => self.build_postgres(uri)?,
            URI::Mysql(uri) => self.build_mysql(uri)?,
        };
        return Ok(writer);
    }
}
