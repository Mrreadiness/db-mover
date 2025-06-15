use std::sync::LazyLock;

use fake::{Fake, Faker};
use itertools::Itertools;
use testcontainers::{Container, ImageExt, runners::SyncRunner};

use postgres::{Client, NoTls};

use super::{
    gen_database_name, rm_container_by_name, row::TestRow, testable_database::TestableDatabase,
};

pub struct TestPostresDatabase {
    pub uri: String,
    pub client: Client,
}

static POSTGRES_CONTAINER: LazyLock<Container<testcontainers_modules::postgres::Postgres>> =
    LazyLock::new(|| {
        let name = "db_mover_tests_postgres";
        rm_container_by_name(name);

        testcontainers_modules::postgres::Postgres::default()
            .with_tag("17-alpine")
            .with_container_name(name)
            .start()
            .unwrap()
    });

impl TestPostresDatabase {
    pub fn new() -> Self {
        let base_uri = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            POSTGRES_CONTAINER.get_host().unwrap(),
            POSTGRES_CONTAINER.get_host_port_ipv4(5432).unwrap(),
        );

        let mut base_client = Client::connect(&base_uri, NoTls)
            .expect("Unable to connect to the database created for tests");
        let new_db_name = gen_database_name();
        let create_db_query = format!("CREATE DATABASE {new_db_name}");
        base_client.execute(&create_db_query, &[]).unwrap();

        let uri = format!(
            "postgres://postgres:postgres@{}:{}/{new_db_name}",
            POSTGRES_CONTAINER.get_host().unwrap(),
            POSTGRES_CONTAINER.get_host_port_ipv4(5432).unwrap(),
        );
        let client = Client::connect(&uri, NoTls)
            .expect("Unable to connect to the database created for tests");

        return Self { uri, client };
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
            let params = (start..start + 5).map(|n| format!("${}", n)).join(", ");
            format!("({})", params)
        })
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
            "CREATE TABLE {name} (id BIGINT PRIMARY KEY, real_field REAL, text_field TEXT, blob_field BYTEA, timestamp_field TIMESTAMP)"
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
