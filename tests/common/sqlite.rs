use std::path::PathBuf;

use db_mover::uri::URI;
use fake::{Fake, Faker};
use rusqlite::{params, Connection, OpenFlags};
use tempfile::TempDir;

use super::{gen_database_name, row::TestRow, testable_database::TestableDatabase};

pub struct TestSqliteDatabase {
    pub path: PathBuf,
    pub conn: Connection,
    tmp_dir: TempDir,
}

impl TestSqliteDatabase {
    pub fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join(gen_database_name());
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
}

impl TestableDatabase for TestSqliteDatabase {
    fn get_uri(&self) -> URI {
        return URI::Sqlite(format!("sqlite://{}", self.path.to_str().unwrap()));
    }

    fn create_test_table(&mut self, table_name: &str) {
        let query = format!(
            "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, real REAL, text TEXT, blob BLOB, timestamp DATETIME)"
        );
        self.conn.execute(&query, []).unwrap();
    }

    fn fill_test_table(&mut self, table_name: &str, num_rows: usize) {
        let query = format!("INSERT INTO {table_name} VALUES (?1, ?2, ?3, ?4, ?5)");
        let mut stmt = self.conn.prepare(&query).unwrap();

        for i in 1..num_rows + 1 {
            let row: TestRow = Faker.fake();
            stmt.execute(params![i, row.real, row.text, row.blob, row.timestamp])
                .unwrap();
        }
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
}
