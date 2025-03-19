use std::str::FromStr;

use anyhow::Context;

use crate::postgres::PostgresDB;
use crate::reader::DBReader;
use crate::sqlite::SqliteDB;
use crate::writer::DBWriter;

#[derive(Debug, Clone)]
pub enum URI {
    Sqlite(String),
    Postgres(String),
}

impl URI {
    pub fn create_reader(&self) -> anyhow::Result<Box<dyn DBReader>> {
        let reader: Box<dyn DBReader> = match self {
            URI::Sqlite(uri) => {
                Box::new(SqliteDB::new(uri).context("Unable to connect to the sqlite")?)
            }
            URI::Postgres(uri) => {
                Box::new(PostgresDB::new(uri).context("Unable to connect to the postgres")?)
            }
        };
        return Ok(reader);
    }

    pub fn create_writer(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        let writer: Box<dyn DBWriter> = match self {
            URI::Sqlite(uri) => {
                Box::new(SqliteDB::new(uri).context("Unable to connect to the sqlite")?)
            }
            URI::Postgres(uri) => {
                Box::new(PostgresDB::new(uri).context("Unable to connect to the postgres")?)
            }
        };
        return Ok(writer);
    }
}

impl FromStr for URI {
    type Err = String;

    fn from_str(s: &str) -> Result<URI, Self::Err> {
        if s.starts_with("sqlite://") {
            return Ok(URI::Sqlite(s.to_owned()));
        }
        if s.starts_with("postgres://") || s.starts_with("postgresql://") {
            return Ok(URI::Postgres(s.to_owned()));
        }
        return Err("Unknown URI format".to_string());
    }
}
