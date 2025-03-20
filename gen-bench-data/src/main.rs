use clap::Parser;
use fake::{Fake, Faker};
use rusqlite::{params, Connection, OpenFlags};
use std::{
    fs::remove_file,
    path::{Path, PathBuf},
};

#[derive(Parser, Clone)]
pub struct Args {
    /// Number of rows in dataset
    #[arg(long, default_value_t = 10_000)]
    pub size: usize,

    /// Override database if already exists
    #[arg(long = "override", default_value_t = false)]
    pub override_db: bool,

    /// Path to output directory
    #[arg(long, default_value = "./benches/data/")]
    pub path: PathBuf,
}

fn create_sqlite_db<T: AsRef<Path>>(path: T) -> Connection {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap();
    return conn;
}

fn create_table(conn: &Connection, table_name: &str) {
    let query = format!(
        "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, real REAL, text TEXT, blob BLOB)"
    );
    conn.execute(&query, []).unwrap();
}

fn fill_table(conn: &Connection, table_name: &str, num_rows: usize) {
    let query = format!("INSERT INTO {table_name} VALUES (?1, ?2, ?3, ?4)");
    let mut stmt = conn.prepare(&query).unwrap();

    let data = Faker.fake::<(f64, String, Vec<u8>)>();
    for i in 1..num_rows + 1 {
        stmt.execute(params![i, data.0, data.1, data.2]).unwrap();
    }
}

fn main() {
    let args = Args::parse();
    if args.override_db {
        let _ = remove_file(args.path.join("input.db"));
    }
    let conn = create_sqlite_db(args.path.join("input.db"));
    create_table(&conn, "test");
    fill_table(&conn, "test", args.size);
}
