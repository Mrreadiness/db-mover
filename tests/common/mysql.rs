use fake::{Fake, Faker};
use testcontainers::{Container, runners::SyncRunner};

use mysql::{Conn, Opts, TxOpts, params, prelude::Queryable};

use super::{row::TestRow, testable_database::TestableDatabase};

pub struct TestMysqlDatabase {
    pub uri: String,
    pub connection: Conn,
    container: Container<testcontainers_modules::mysql::Mysql>,
}

impl TestMysqlDatabase {
    pub fn new() -> Self {
        let container = testcontainers_modules::mysql::Mysql::default()
            .start()
            .unwrap();

        let uri = format!(
            "mysql://root@{}:{}/test",
            container.get_host().unwrap(),
            container.get_host_port_ipv4(3306).unwrap(),
        );
        let opts = Opts::from_url(&uri).unwrap();
        let connection = Conn::new(opts).unwrap();

        return Self {
            uri,
            connection,
            container,
        };
    }
}

impl TestableDatabase for TestMysqlDatabase {
    fn get_uri(&self) -> db_mover::uri::URI {
        return db_mover::uri::URI::Mysql(self.uri.clone());
    }

    fn execute(&mut self, query: impl AsRef<str>) {
        self.connection.query_drop(query.as_ref()).unwrap();
    }

    fn create_test_table(&mut self, name: &str) {
        let query = format!(
            "CREATE TABLE {name} (id BIGINT PRIMARY KEY, real_field FLOAT, text_field TEXT, blob_field BLOB, timestamp_field TIMESTAMP)"
        );
        self.connection
            .query_drop(&query)
            .expect("Failed to create table");
    }

    fn fill_test_table(&mut self, name: &str, num_rows: usize) {
        let mut trx = self
            .connection
            .start_transaction(TxOpts::default())
            .unwrap();

        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let mut row: TestRow = Faker.fake();
            row.id = i as i64;
            rows.push(row);
        }
        for chunk in rows.chunks(100) {
            trx.exec_batch( // TODO: optimize
                format!("INSERT INTO {name} VALUES (:id, :real_field, :text_field, :blob_field, :timestamp_field)"),
                chunk.into_iter().map(|row| {
                    params! {
                        "id" => row.id,
                        "real_field" => row.real,
                        "text_field" => row.text.clone(),
                        "blob_field" => row.blob.clone(),
                        "timestamp_field" => row.timestamp.clone(),
                    }
                }),
            )
            .unwrap();
        }
        trx.commit().unwrap();
    }

    fn get_all_rows(&mut self, table_name: &str) -> Vec<TestRow> {
        let query = format!("SELECT * FROM {table_name} ORDER BY id");

        return self
            .connection
            .query_map(query, |row: mysql::Row| row.into())
            .unwrap();
    }

    fn query_count(&mut self, query: impl AsRef<str>) -> u32 {
        return self.connection.query_first(query).unwrap().unwrap();
    }
}
