use db_mover::{self, reader::DBReader, writer::DBWriter};

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

fn benchmark(c: &mut Criterion, num_rows: usize) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let input_db_path = tmp_dir.path().join("input.db");
    let output_db_path = tmp_dir.path().join("output.db");

    let in_conn = create_sqlite_db(&input_db_path).unwrap();
    fill_table(&in_conn, "test", num_rows).unwrap();

    let name = format!("sqlite to sqlite {num_rows}");
    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Sqlite(format!("sqlite://{}", input_db_path.to_str().unwrap())),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned()],
        queue_size: Some(10_000),
    };

    c.bench_function(&name, |b| {
        b.iter(|| {
            create_sqlite_db(&output_db_path).unwrap();

            db_mover::run(args.clone());

            remove_file(&output_db_path).unwrap();
        })
    });
}

fn benchmark_10_000(c: &mut Criterion) {
    benchmark(c, 10_000);
}

fn benchmark_100_000(c: &mut Criterion) {
    benchmark(c, 100_000);
}

fn benchmark_reader(c: &mut Criterion, num_rows: usize) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let input_db_path = tmp_dir.path().join("input.db");

    let in_conn = create_sqlite_db(&input_db_path).unwrap();
    fill_table(&in_conn, "test", num_rows).unwrap();

    let name = format!("sqlite reader {num_rows}");

    c.bench_function(&name, |b| {
        b.iter(|| {
            let (sender, _reciver) = db_mover::channel::create_channel(Some(100_000));
            let reader = db_mover::sqlite::SqliteDB::new(input_db_path.to_str().unwrap()).unwrap();
            reader.start_reading(sender, "test");
        })
    });
}

fn benchmark_reader_10_000(c: &mut Criterion) {
    benchmark_reader(c, 10_000);
}

fn benchmark_reader_100_000(c: &mut Criterion) {
    benchmark_reader(c, 100_000);
}

fn benchmark_writer(c: &mut Criterion, num_rows: usize) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let output_db_path = tmp_dir.path().join("output.db");

    let name = format!("sqlite writer {num_rows}");
    let mut data = Vec::with_capacity(num_rows);
    use db_mover::row::Value;
    for i in 1..num_rows + 1 {
        let row = Faker.fake::<(f64, String, Vec<u8>)>();
        data.push(vec![
            Value::I64(i as i64),
            Value::F64(row.0),
            Value::String(row.1),
            Value::Bytes(row.2),
        ]);
    }

    c.bench_function(&name, |b| {
        b.iter_batched(
            || {
                let (sender, reciver) = db_mover::channel::create_channel(Some(num_rows));
                for el in &data {
                    sender.send(el.to_owned()).unwrap();
                }
                return reciver;
            },
            |reciver| {
                create_sqlite_db(&output_db_path).unwrap();
                let writer =
                    db_mover::sqlite::SqliteDB::new(output_db_path.to_str().unwrap()).unwrap();

                writer.start_writing(reciver, "test");

                remove_file(&output_db_path).unwrap();
            },
            criterion::BatchSize::LargeInput,
        )
    });
}

fn benchmark_writer_10_000(c: &mut Criterion) {
    benchmark_writer(c, 10_000);
}

fn benchmark_writer_100_000(c: &mut Criterion) {
    benchmark_writer(c, 100_000);
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark_10_000, benchmark_100_000,
}
criterion_group! {
    name = benches_reader;
    config = Criterion::default();
    targets = benchmark_reader_10_000, benchmark_reader_100_000,
}
criterion_group! {
    name = benches_writer;
    config = Criterion::default();
    targets = benchmark_writer_10_000, benchmark_writer_100_000,
}
criterion_main!(benches, benches_reader, benches_writer);
