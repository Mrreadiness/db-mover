use itertools::Itertools;
use std::sync::LazyLock;

use fake::{Fake, Faker};
use testcontainers::{Container, ImageExt, runners::SyncRunner};

use mysql::{Conn, Opts, TxOpts, params, prelude::Queryable};

use super::{
    gen_database_name, rm_container_by_name, row::TestRow, testable_database::TestableDatabase,
};

static MYSQL_CONTAINER: LazyLock<Container<testcontainers_modules::mysql::Mysql>> =
    LazyLock::new(|| {
        let name = "db_mover_tests_mysql";
        rm_container_by_name(name);
        testcontainers_modules::mysql::Mysql::default()
            .with_container_name(name)
            .start()
            .unwrap()
    });

static MARIADB_CONTAINER: LazyLock<Container<testcontainers_modules::mariadb::Mariadb>> =
    LazyLock::new(|| {
        let name = "db_mover_tests_mariadb";
        rm_container_by_name(name);
        testcontainers_modules::mariadb::Mariadb::default()
            .with_container_name(name)
            .start()
            .unwrap()
    });

pub struct TestMysqlDatabase {
    pub uri: String,
    pub connection: Conn,
}

impl TestMysqlDatabase {
    pub fn new(host: String, port: u16) -> Self {
        let new_db_name = gen_database_name();
        let base_uri = format!("mysql://root@{}:{}/test", host, port,);
        let mut base_connection = Conn::new(Opts::from_url(&base_uri).unwrap()).unwrap();
        base_connection
            .query_drop(format!("CREATE DATABASE {new_db_name}"))
            .unwrap();

        let uri = format!("mysql://root@{}:{}/{new_db_name}", host, port,);
        let opts = Opts::from_url(&uri).unwrap();
        let connection = Conn::new(opts).unwrap();

        return Self { uri, connection };
    }

    pub fn new_mysql() -> Self {
        return Self::new(
            MYSQL_CONTAINER.get_host().unwrap().to_string(),
            MYSQL_CONTAINER.get_host_port_ipv4(3306).unwrap(),
        );
    }

    pub fn new_mariadb() -> Self {
        return Self::new(
            MARIADB_CONTAINER.get_host().unwrap().to_string(),
            MARIADB_CONTAINER.get_host_port_ipv4(3306).unwrap(),
        );
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
            "CREATE TABLE {name} (id BIGINT PRIMARY KEY, real_field FLOAT, text_field TEXT, blob_field BLOB, timestamp_field DATETIME)"
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
        for chunk in rows.into_iter().chunks(100).into_iter() {
            trx.exec_batch(
                format!("INSERT INTO {name} VALUES (:id, :real_field, :text_field, :blob_field, :timestamp_field)"),
                chunk.map(|row| {
                    params! {
                        "id" => row.id,
                        "real_field" => row.real,
                        "text_field" => row.text,
                        "blob_field" => row.blob,
                        "timestamp_field" => row.timestamp,
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
