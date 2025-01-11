use db_mover;

use criterion::{criterion_group, criterion_main, Criterion};
use fake::{Fake, Faker};
use rusqlite::{params, Connection, OpenFlags};
use std::{fs::remove_file, path::Path};
use tempfile;

fn create_sqlite_db<T: AsRef<Path>>(path: T) -> anyhow::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    create_table(&conn, "test")?;
    return Ok(conn);
}

fn create_table(conn: &Connection, table_name: &str) -> anyhow::Result<()> {
    let query = format!(
        "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, real REAL, text TEXT, blob BLOB)"
    );
    conn.execute(&query, [])?;
    return Ok(());
}

fn fill_table(conn: &Connection, table_name: &str, num_rows: usize) -> anyhow::Result<()> {
    let query = format!("INSERT INTO {table_name} VALUES (?1, ?2, ?3, ?4)");
    let mut stmt = conn.prepare(&query)?;

    for i in 1..num_rows + 1 {
        let data = Faker.fake::<(f64, String, Vec<u8>)>();
        stmt.execute(params![i, data.0, data.1, data.2])?;
    }
    return Ok(());
}

fn sqlite_to_sqlite_benchmark(c: &mut Criterion) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let input_db_path = tmp_dir.path().join("input.db");
    let output_db_path = tmp_dir.path().join("output.db");

    let in_conn = create_sqlite_db(&input_db_path).unwrap();
    fill_table(&in_conn, "test", 10000).unwrap();

    c.bench_function("main sqlite to sqlite", |b| {
        b.iter(|| {
            let args = db_mover::args::Args {
                input: db_mover::uri::URI::Sqlite(format!(
                    "sqlite://{}",
                    input_db_path.to_str().unwrap()
                )),
                output: db_mover::uri::URI::Sqlite(format!(
                    "sqlite://{}",
                    output_db_path.to_str().unwrap()
                )),
                table: vec!["test".to_owned()],
                queue_size: Some(100_000),
            };
            create_sqlite_db(&output_db_path).unwrap();

            db_mover::run(args);

            remove_file(&output_db_path).unwrap();
        })
    });
}

criterion_group!(benches, sqlite_to_sqlite_benchmark);
criterion_main!(benches);
