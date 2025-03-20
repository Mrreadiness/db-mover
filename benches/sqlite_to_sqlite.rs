use db_mover;

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../tests/common/mod.rs"]
mod common;

use common::testable_database::TestableDatabase;

fn benchmark(c: &mut Criterion) {
    let mut input = common::sqlite::TestSqliteDatabase::new();
    input.create_test_table("test");
    input.fill_test_table("test", 100_000);

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

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark
}
criterion_main!(benches);
