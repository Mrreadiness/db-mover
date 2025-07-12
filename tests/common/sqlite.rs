use std::path::PathBuf;

use db_mover::uri::URI;
use fake::{Fake, Faker};
use rusqlite::{Connection, OpenFlags, params};
use tempfile::TempDir;

use super::{gen_database_name, row::TestRow, testable_database::TestableDatabase};

pub struct TestSqliteDatabase {
    pub path: PathBuf,
    pub conn: Connection,
    tmp_dir: Option<TempDir>,
}

impl TestSqliteDatabase {
    pub fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join(gen_database_name());
        return Self::from_path(path, Some(tmp_dir));
    }

    pub fn from_path(path: PathBuf, tmp_dir: Option<TempDir>) -> Self {
        let conn = Connection::open_with_flags(
            path.clone(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .expect("Failed to create test sqlite database");

        return Self {
            conn,
            path,
            tmp_dir,
        };
    }

    pub fn get_uri_raw(&self) -> String {
        return format!("sqlite://{}", self.path.to_str().unwrap());
    }
}

impl TestableDatabase for TestSqliteDatabase {
    fn get_uri(&self) -> URI {
        return URI::Sqlite(self.get_uri_raw());
    }

    fn execute(&mut self, query: impl AsRef<str>) {
        self.conn.execute(query.as_ref(), []).unwrap();
    }

    fn create_test_table(&mut self, table_name: &str) {
        let query = format!(
            "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY NOT NULL, real_field REAL, text_field TEXT, blob_field BLOB, timestamp_field DATETIME)"
        );
        self.conn.execute(&query, []).unwrap();
    }

    fn fill_test_table(&mut self, table_name: &str, num_rows: usize) {
        let trx = self.conn.transaction().unwrap();
        {
            let query = format!("INSERT INTO {table_name} VALUES (?1, ?2, ?3, ?4, ?5)");
            let mut stmt = trx.prepare(&query).unwrap();

            for i in 1..num_rows + 1 {
                let row: TestRow = Faker.fake();
                stmt.execute(params![i, row.real, row.text, row.blob, row.timestamp])
                    .unwrap();
            }
        }
        trx.commit().unwrap();
    }

    fn get_all_rows(&mut self, table_name: &str) -> Vec<TestRow> {
        let query = format!("SELECT * FROM {table_name} ORDER BY id");

        let mut stmt = self.conn.prepare(&query).unwrap();
        let mut rows = Vec::new();
        for row in stmt.query_map([], |row| Ok(TestRow::from(row))).unwrap() {
            rows.push(row.unwrap());
        }
        return rows;
    }

    fn query_count(&mut self, query: impl AsRef<str>) -> u32 {
        let mut stmt = self.conn.prepare(query.as_ref()).unwrap();
        return stmt.query_one([], |row| row.get(0)).unwrap();
    }
}
