use db_mover;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

#[path = "../tests/common/mod.rs"]
mod common;

use common::testable_database::TestableDatabase;

const NUM_ROWS: usize = 1_000_000;

fn sqlite_to_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_to_sqlite");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, num_rows| {
        let mut input = common::sqlite::TestSqliteDatabase::new();
        input.create_test_table("test");
        input.fill_test_table("test", *num_rows);
        b.iter(|| {
            let mut output = common::sqlite::TestSqliteDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());
            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn sqlite_to_postgres(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_to_postgres");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, num_rows| {
        let mut input = common::sqlite::TestSqliteDatabase::new();
        input.create_test_table("test");
        input.fill_test_table("test", *num_rows);
        b.iter(|| {
            let mut output = common::postgres::TestPostresDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn postgres_to_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("postgres_to_sqlite");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, num_rows| {
        let mut input = common::postgres::TestPostresDatabase::new();
        input.create_test_table("test");
        input.fill_test_table("test", *num_rows);
        b.iter(|| {
            let mut output = common::sqlite::TestSqliteDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn postgres_to_postgres(c: &mut Criterion) {
    let mut group = c.benchmark_group("postgres_to_postgres");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, num_rows| {
        let mut input = common::postgres::TestPostresDatabase::new();
        input.create_test_table("test");
        input.fill_test_table("test", *num_rows);
        b.iter(|| {
            let mut output = common::postgres::TestPostresDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = sqlite_to_sqlite, sqlite_to_postgres, postgres_to_sqlite, postgres_to_postgres
}
criterion_main!(benches);
