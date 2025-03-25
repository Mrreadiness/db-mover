use std::str::FromStr;

use anyhow::Context;

use crate::databases::postgres::PostgresDB;
use crate::databases::sqlite::SqliteDB;
use crate::databases::traits::{DBReader, DBWriter};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_from_str_sqlite() {
        let uri = URI::from_str("sqlite://test.db");
        assert!(matches!(uri, Ok(URI::Sqlite(_))));
    }

    #[test]
    fn test_uri_from_str_postgres() {
        let uri = URI::from_str("postgres://user:pass@localhost/db");
        assert!(matches!(uri, Ok(URI::Postgres(_))));
    }

    #[test]
    fn test_uri_from_str_invalid() {
        let uri = URI::from_str("invalid://test");
        assert!(uri.is_err());
    }
}
