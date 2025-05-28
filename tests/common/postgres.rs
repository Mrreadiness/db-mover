use fake::{Fake, Faker};
use testcontainers::{Container, ImageExt, runners::SyncRunner};

use postgres::{Client, NoTls};

use super::{row::TestRow, testable_database::TestableDatabase};

pub struct TestPostresDatabase {
    pub uri: String,
    pub client: Client,
    container: Container<testcontainers_modules::postgres::Postgres>,
}

impl TestPostresDatabase {
    pub fn new() -> Self {
        let container = testcontainers_modules::postgres::Postgres::default()
            .with_tag("17-alpine")
            .start()
            .unwrap();

        let uri = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().unwrap(),
            container.get_host_port_ipv4(5432).unwrap(),
        );

        let client = Client::connect(&uri, NoTls)
            .expect("Unable to connect to the database created for tests");

        return Self {
            uri,
            client,
            container,
        };
    }

    pub fn new_client(&self) -> Client {
        return Client::connect(&self.uri, NoTls)
            .expect("Unable to connect to the database created for tests");
    }

    pub fn reconect(&mut self) {
        self.client = self.new_client();
    }
}

fn generate_placeholders(blocks: usize) -> String {
    (0..blocks)
        .map(|i| {
            let start = i * 5 + 1;
            let params = (start..start + 5)
                .map(|n| format!("${}", n))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({})", params)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

impl TestableDatabase for TestPostresDatabase {
    fn get_uri(&self) -> db_mover::uri::URI {
        return db_mover::uri::URI::Postgres(self.uri.clone());
    }

    fn execute(&mut self, query: impl AsRef<str>) {
        self.client.execute(query.as_ref(), &[]).unwrap();
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
        let mut trx = self.client.transaction().unwrap();

        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let mut row: TestRow = Faker.fake();
            row.id = i as i64;
            rows.push(row);
        }
        for chunk in rows.chunks(100) {
            let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
            for row in chunk.iter() {
                params.push(&row.id);
                params.push(&row.real);
                params.push(&row.text);
                params.push(&row.blob);
                params.push(&row.timestamp);
            }

            let placeholders = generate_placeholders(chunk.len());
            let query = format!("INSERT INTO {name} VALUES {placeholders}");

            trx.execute(&query, params.as_slice()).unwrap();
        }
        trx.commit().unwrap();
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

    fn query_count(&mut self, query: impl AsRef<str>) -> u32 {
        return self
            .client
            .query_one(query.as_ref(), &[])
            .unwrap()
            .get::<_, i64>(0)
            .try_into()
            .unwrap();
    }
}
