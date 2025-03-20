use db_mover;

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../tests/common/mod.rs"]
mod common;

use common::testable_database::TestableDatabase;

const NUM_ROWS: usize = 100_0;

fn sqlite_to_sqlite(c: &mut Criterion) {
    let mut input = common::sqlite::TestSqliteDatabase::new();
    input.create_test_table("test");
    input.fill_test_table("test", NUM_ROWS);

    c.bench_function("sqlite to sqlite", |b| {
        b.iter(|| {
            let mut output = common::sqlite::TestSqliteDatabase::new();
            output.create_test_table("test");
            let args = db_mover::args::Args {
                input: input.get_uri(),
                output: output.get_uri(),
                table: vec!["test".to_owned()],
                queue_size: Some(10_000),
                batch_write_size: 1_000,
                batch_write_retries: 1,
            };

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn sqlite_to_postgres(c: &mut Criterion) {
    let mut input = common::sqlite::TestSqliteDatabase::new();
    input.create_test_table("test");
    input.fill_test_table("test", NUM_ROWS);

    c.bench_function("sqlite to postgres", |b| {
        b.iter(|| {
            let mut output = common::postgres::TestPostresDatabase::new();
            output.create_test_table("test");
            let args = db_mover::args::Args {
                input: input.get_uri(),
                output: output.get_uri(),
                table: vec!["test".to_owned()],
                queue_size: Some(10_000),
                batch_write_size: 1_000,
                batch_write_retries: 1,
            };

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn postgres_to_sqlite(c: &mut Criterion) {
    let mut input = common::postgres::TestPostresDatabase::new();
    input.create_test_table("test");
    input.fill_test_table("test", NUM_ROWS);

    c.bench_function("postgres to sqlite", |b| {
        b.iter(|| {
            let mut output = common::sqlite::TestSqliteDatabase::new();
            output.create_test_table("test");
            let args = db_mover::args::Args {
                input: input.get_uri(),
                output: output.get_uri(),
                table: vec!["test".to_owned()],
                queue_size: Some(10_000),
                batch_write_size: 1_000,
                batch_write_retries: 1,
            };

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn postgres_to_postgres(c: &mut Criterion) {
    let mut input = common::postgres::TestPostresDatabase::new();
    input.create_test_table("test");
    input.fill_test_table("test", NUM_ROWS);

    c.bench_function("postgres to postgres", |b| {
        b.iter(|| {
            let mut output = common::postgres::TestPostresDatabase::new();
            output.create_test_table("test");
            let args = db_mover::args::Args {
                input: input.get_uri(),
                output: output.get_uri(),
                table: vec!["test".to_owned()],
                queue_size: Some(10_000),
                batch_write_size: 1_000,
                batch_write_retries: 1,
            };

            db_mover::run(args.clone()).unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = sqlite_to_sqlite, sqlite_to_postgres, postgres_to_sqlite, postgres_to_postgres
}
criterion_main!(benches);
