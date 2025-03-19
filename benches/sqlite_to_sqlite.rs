use db_mover::{self, reader::DBReader, writer::DBWriter};

use criterion::{criterion_group, criterion_main, Criterion};
use rusqlite::{Connection, OpenFlags};
use std::{
    fs::remove_file,
    path::{Path, PathBuf},
};
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

fn benchmark(c: &mut Criterion) {
    let input_db_path = PathBuf::from("benches/data/input.db");
    let tmp_dir = tempfile::tempdir().unwrap();
    let output_db_path = tmp_dir.path().join("output.db");

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Sqlite(format!("sqlite://{}", input_db_path.to_str().unwrap())),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned()],
        queue_size: Some(10_000),
    };

    c.bench_function("sqlite to sqlite", |b| {
        b.iter(|| {
            create_sqlite_db(&output_db_path).unwrap();

            db_mover::run(args.clone()).unwrap();

            remove_file(&output_db_path).unwrap();
        })
    });
}

fn benchmark_reader(c: &mut Criterion) {
    let input_db_path = PathBuf::from("benches/data/input.db");

    c.bench_function("sqlite reader", |b| {
        b.iter(|| {
            let (sender, _reciver) = db_mover::channel::create_channel(None);
            let mut reader =
                db_mover::sqlite::SqliteDB::new(input_db_path.to_str().unwrap()).unwrap();
            reader.start_reading(sender, "test").unwrap();
        })
    });
}

fn benchmark_writer(c: &mut Criterion) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let output_db_path = tmp_dir.path().join("output.db");

    let mut data = Vec::new();
    {
        let input_db_path = PathBuf::from("benches/data/input.db");
        let (sender, reciver) = db_mover::channel::create_channel(None);
        let mut reader = db_mover::sqlite::SqliteDB::new(input_db_path.to_str().unwrap()).unwrap();
        reader.start_reading(sender, "test").unwrap();
        for row in reciver.iter() {
            data.push(row);
        }
    }

    c.bench_function("sqlite writer", |b| {
        b.iter_batched(
            || {
                let (sender, reciver) = db_mover::channel::create_channel(Some(data.len()));
                for el in &data {
                    sender.send(el.to_owned()).unwrap();
                }
                return reciver;
            },
            |reciver| {
                create_sqlite_db(&output_db_path).unwrap();
                let mut writer =
                    db_mover::sqlite::SqliteDB::new(output_db_path.to_str().unwrap()).unwrap();

                writer.start_writing(reciver, "test").unwrap();

                remove_file(&output_db_path).unwrap();
            },
            criterion::BatchSize::LargeInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark, benchmark_reader, benchmark_writer,
}
criterion_main!(benches);
