use rusqlite::{Connection, OpenFlags};

use crate::{channel::{Reciever, Sender}, reader::DBReader, writer::DBWriter};

pub struct SqliteDB {
    connection: Connection,
}

impl SqliteDB {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let path = uri.replace("sqlite://", "");
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        return Ok(SqliteDB { connection: conn });
    }
}

impl DBReader for SqliteDB {
    fn start_reading(&self, sender: Sender, table: &str) {}
}

impl DBWriter for SqliteDB {
    fn start_writing(&self, reciever: Reciever, table: &str) {}
}
