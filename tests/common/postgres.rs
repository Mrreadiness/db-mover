use fake::{Fake, Faker};
use postgres::{Client, NoTls};

use super::{gen_database_name, row::TestRow, testable_database::TestableDatabase};

pub struct TestPostresDatabase {
    name: String,
    pub uri: String,
    pub client: Client,
    base_client: Client,
}

impl TestPostresDatabase {
    pub fn new() -> Self {
        let base_uri =
            std::env::var("POSTGRES_URI").expect("POSTGRES_URI env is required to run this test");

        let db_name_separtor = base_uri
            .rfind("/")
            .expect("Failed to find database name separtor in the URI");
        let (base_uri_without_db, _) = base_uri.split_at(db_name_separtor);
        let name = gen_database_name();
        let uri = format!("{base_uri_without_db}/{name}");

        let mut base_client = Client::connect(&base_uri, NoTls)
            .expect("Unable to connect to postgres database for tests");
        let query = format!("CREATE DATABASE {name}");
        base_client
            .execute(&query, &[])
            .expect("Unable to create database in postgres to run tests");

        let client = Client::connect(&uri, NoTls)
            .expect("Unable to connect to the database created for tests");

        return Self {
            name,
            uri,
            client,
            base_client,
        };
    }
}

impl TestableDatabase for TestPostresDatabase {
    fn get_uri(&self) -> db_mover::uri::URI {
        return db_mover::uri::URI::Postgres(self.uri.clone());
    }

    fn create_test_table(&mut self, name: &str) {
        let query = format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, real REAL, text TEXT, blob BYTEA, timestamp TIMESTAMP)"
        );
        self.client
            .execute(&query, &[])
            .expect("Failed to create table");
    }

    fn fill_test_table(&mut self, name: &str, num_rows: usize) {
        let query = format!("INSERT INTO {name} VALUES ($1, $2, $3, $4, $5)");
        let stmt = self.client.prepare(&query).unwrap();

        for i in 1..num_rows + 1 {
            let row: TestRow = Faker.fake();
            self.client
                .execute(
                    &stmt,
                    &[&(i as i64), &row.real, &row.text, &row.blob, &row.timestamp],
                )
                .unwrap();
        }
    }

    fn get_all_rows(&mut self, table_name: &str) -> Vec<TestRow> {
        let query = format!("SELECT * FROM {table_name} ORDER BY id");

        let stmt = self.client.prepare(&query).unwrap();
        return self
            .client
            .query(&stmt, &[])
            .unwrap()
            .into_iter()
            .map(|row| row.into())
            .collect();
    }
}

impl Drop for TestPostresDatabase {
    fn drop(&mut self) {
        let query = format!("DROP DATABASE {} WITH (FORCE)", self.name);
        self.base_client.execute(&query, &[]).unwrap();
    }
}
