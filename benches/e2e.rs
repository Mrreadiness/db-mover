use std::{env, sync::LazyLock};

use db_mover::{self, uri::URI};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[path = "../tests/common/mod.rs"]
mod common;

use common::{
    mysql::TestMysqlDatabase, postgres::TestPostresDatabase, sqlite::TestSqliteDatabase,
    testable_database::TestableDatabase,
};

const NUM_ROWS: usize = 1_000_000;

struct LazyTestDatabaseFactory<T: TestableDatabase> {
    internal: T,
    initialized: bool,
}

static BASE_DATASET_DB: LazyLock<URI> = LazyLock::new(|| {
    let db_path = env::temp_dir().join(format!("db_mover_benchmark_data_{NUM_ROWS}"));
    if !db_path.exists() {
        println!("\nCreating new dataset for benchmarks {db_path:?}");
        let mut db = TestSqliteDatabase::from_path(db_path.clone(), None);
        db.create_test_table("test");
        db.fill_test_table("test", NUM_ROWS);
    } else {
        println!(
            "\nReusing dataset for benchmarks {db_path:?}. Delete it for generating a new one."
        );
    }

    return URI::Sqlite(format!("sqlite://{}", db_path.to_str().unwrap()));
});

impl<T: TestableDatabase> LazyTestDatabaseFactory<T> {
    fn new(db: T) -> Self {
        return Self {
            internal: db,
            initialized: false,
        };
    }

    fn init(&mut self) {
        if !self.initialized {
            self.internal.create_test_table("test");
            let mut args =
                db_mover::args::Args::new(BASE_DATASET_DB.clone(), self.internal.get_uri());
            args.table.push("test".to_string());
            db_mover::run(args).unwrap();
            self.initialized = true;
        }
    }

    fn get_uri(&self) -> URI {
        return self.internal.get_uri();
    }
}

fn sqlite_to_sqlite(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestSqliteDatabase::new());
    let mut group = c.benchmark_group("sqlite_to_sqlite");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestSqliteDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());
            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn sqlite_to_postgres(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestSqliteDatabase::new());
    let mut group = c.benchmark_group("sqlite_to_postgres");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestPostresDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn postgres_to_sqlite(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestPostresDatabase::new());
    let mut group = c.benchmark_group("postgres_to_sqlite");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestSqliteDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn postgres_to_postgres(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestPostresDatabase::new());
    let mut group = c.benchmark_group("postgres_to_postgres");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestPostresDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn sqlite_to_mysql(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestSqliteDatabase::new());
    let mut group = c.benchmark_group("sqlite_to_mysql");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestMysqlDatabase::new_mysql();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.writer_workers = 4;
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn mysql_to_sqlite(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestMysqlDatabase::new_mysql());
    let mut group = c.benchmark_group("mysql_to_sqlite");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestSqliteDatabase::new();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn sqlite_to_mariadb(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestSqliteDatabase::new());
    let mut group = c.benchmark_group("sqlite_to_mariadb");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestMysqlDatabase::new_mariadb();
            output.create_test_table("test");
            let mut args = db_mover::args::Args::new(input.get_uri(), output.get_uri());
            args.writer_workers = 4;
            args.table.push("test".to_string());

            db_mover::run(args.clone()).unwrap();
        })
    });
}

fn mariadb_to_sqlite(c: &mut Criterion) {
    let mut input = LazyTestDatabaseFactory::new(TestMysqlDatabase::new_mariadb());
    let mut group = c.benchmark_group("mariadb_to_sqlite");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.bench_with_input(NUM_ROWS.to_string(), &NUM_ROWS, |b, _num_rows| {
        input.init();
        b.iter(|| {
            let mut output = TestSqliteDatabase::new();
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
    targets = sqlite_to_sqlite, sqlite_to_postgres, postgres_to_sqlite, postgres_to_postgres, sqlite_to_mysql, mysql_to_sqlite, sqlite_to_mariadb, mariadb_to_sqlite,
}
criterion_main!(benches);
