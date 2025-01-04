use std::str::FromStr;

use crate::reader::DBReader;
use crate::sqlite::SqliteDB;
use crate::writer::DBWriter;

#[derive(Debug, Clone)]
pub enum URI {
    Sqlite(String),
}

impl URI {
    pub fn create_reader(&self) -> impl DBReader {
        match self {
            URI::Sqlite(uri) => {
                return SqliteDB::new(uri).expect("Unable to connect to the sqlite")
            }
        }
    }

    pub fn create_writer(&self) -> impl DBWriter {
        match self {
            URI::Sqlite(uri) => {
                return SqliteDB::new(uri).expect("Unable to connect to the sqlite")
            }
        }
    }
}

impl FromStr for URI {
    type Err = String;

    fn from_str(s: &str) -> Result<URI, Self::Err> {
        if s.starts_with("sqlite://") {
            return Ok(URI::Sqlite(s.to_owned()));
        }
        return Err("Unknown URI format".to_string());
    }
}
