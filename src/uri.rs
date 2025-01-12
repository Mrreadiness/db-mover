use std::str::FromStr;

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
    pub fn create_reader(&self) -> Box<dyn DBReader> {
        match self {
            URI::Sqlite(uri) => {
                return Box::new(SqliteDB::new(uri).expect("Unable to connect to the sqlite"));
            }
            URI::Postgres(uri) => {
                return Box::new(PostgresDB::new(uri).expect("Unable to connect to the postgres"));
            }
        }
    }

    pub fn create_writer(&self) -> Box<dyn DBWriter> {
        match self {
            URI::Sqlite(uri) => {
                return Box::new(SqliteDB::new(uri).expect("Unable to connect to the sqlite"));
            }
            _ => panic!("FIXME"),
        }
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
