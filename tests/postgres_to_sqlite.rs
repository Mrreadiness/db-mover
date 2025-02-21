use db_mover;

use fake::{Fake, Faker};
use postgres::{Client, NoTls};
use rand::distr::{slice::Choose, Distribution};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use tempfile;

#[derive(PartialEq, Debug)]
struct TestRow {
    id: i64,
    real: f64,
    text: String,
    blob: Vec<u8>,
}

impl From<postgres::Row> for TestRow {
    fn from(row: postgres::Row) -> Self {
        return Self {
            id: row.get(0),
            real: row.get(1),
            text: row.get(2),
            blob: row.get(3),
        };
    }
}

impl From<&rusqlite::Row<'_>> for TestRow {
    fn from(row: &rusqlite::Row<'_>) -> Self {
        return Self {
            id: row.get_unwrap(0),
            real: row.get_unwrap(1),
            text: row.get_unwrap(2),
            blob: row.get_unwrap(3),
        };
    }
}

fn gen_database_name() -> String {
    let chars = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let chars_dist = Choose::new(&chars).unwrap();
    return chars_dist.sample_iter(&mut rand::rng()).take(10).collect();
}

struct TestPostresDatabase {
    name: String,
    uri: String,
    client: Client,
    base_client: Client,
}

impl TestPostresDatabase {
    fn new() -> Self {
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

    fn create_test_table(&mut self, name: &str) {
        let query = format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, real DOUBLE PRECISION, text TEXT, blob BYTEA)"
        );
        self.client
            .execute(&query, &[])
            .expect("Failed to create table");
    }

    fn fill_test_table(&mut self, name: &str, num_rows: usize) {
        let query = format!("INSERT INTO {name} VALUES ($1, $2, $3, $4)");
        let stmt = self.client.prepare(&query).unwrap();

        for i in 1..num_rows + 1 {
            let data = Faker.fake::<(f64, String, Vec<u8>)>();
            self.client
                .execute(&stmt, &[&(i as i64), &data.0, &data.1, &data.2])
                .unwrap();
        }
    }
}

impl Drop for TestPostresDatabase {
    fn drop(&mut self) {
        let query = format!("DROP DATABASE {} WITH (FORCE)", self.name);
        self.base_client.execute(&query, &[]).unwrap();
    }
}

fn create_postgres_db() -> TestPostresDatabase {
    let mut in_db = TestPostresDatabase::new();
    in_db.create_test_table("test");
    in_db.create_test_table("test1");
    return in_db;
}

fn create_sqlite_db<T: AsRef<Path>>(path: T) -> anyhow::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    create_table_sqlite(&conn, "test")?;
    create_table_sqlite(&conn, "test1")?;
    return Ok(conn);
}

fn create_table_sqlite(conn: &Connection, table_name: &str) -> anyhow::Result<()> {
    let query = format!(
        "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, real REAL, text TEXT, blob BLOB)"
    );
    conn.execute(&query, [])?;
    return Ok(());
}

fn assert_tables_equal(client: &mut Client, conn: &Connection, table_name: &str) {
    let query = format!("SELECT * FROM {table_name} ORDER BY id");

    let stmt1 = client.prepare(&query).unwrap();
    let rows1: Vec<TestRow> = client
        .query(&stmt1, &[])
        .unwrap()
        .into_iter()
        .map(|row| row.into())
        .collect();

    let mut stmt2 = conn.prepare(&query).unwrap();
    let mut rows2 = Vec::new();
    for row in stmt2.query_map([], |row| Ok(TestRow::from(row))).unwrap() {
        rows2.push(row.unwrap());
    }

    assert_eq!(rows1, rows2, "Rows are not equal");
}

fn assert_dbs_equal(postgres: &mut TestPostresDatabase, conn: &Connection) {
    assert_tables_equal(&mut postgres.client, conn, "test");
    assert_tables_equal(&mut postgres.client, conn, "test1");
}

#[test]
fn empty() -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let output_db_path = tmp_dir.path().join("output.db");

    let mut in_db = create_postgres_db();
    let out_conn = create_sqlite_db(&output_db_path)?;
    assert_dbs_equal(&mut in_db, &out_conn);

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec![],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert_dbs_equal(&mut in_db, &out_conn);

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert_dbs_equal(&mut in_db, &out_conn);

    return Ok(());
}

#[test]
fn one_table() -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let output_db_path = tmp_dir.path().join("output.db");

    let mut in_db = create_postgres_db();
    in_db.fill_test_table("test", 1000);
    let out_conn = create_sqlite_db(&output_db_path)?;
    assert_tables_equal(&mut in_db.client, &out_conn, "test1");

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert_dbs_equal(&mut in_db, &out_conn);
    return Ok(());
}

#[test]
fn multiple_tables() -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let output_db_path = tmp_dir.path().join("output.db");

    let mut in_db = create_postgres_db();
    in_db.fill_test_table("test", 1000);
    in_db.fill_test_table("test1", 100);
    let out_conn = create_sqlite_db(&output_db_path)?;

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Postgres(in_db.uri.clone()),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned(), "test1".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert_dbs_equal(&mut in_db, &out_conn);
    return Ok(());
}
