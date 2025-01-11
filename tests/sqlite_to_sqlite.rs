use db_mover;

use fake::{Fake, Faker};
use rusqlite::{params, Connection, OpenFlags, Row};
use std::path::Path;
use tempfile;

#[derive(PartialEq)]
struct TestRow {
    id: i64,
    real: f64,
    text: String,
    blob: Vec<u8>,
}

fn create_sqlite_db<T: AsRef<Path>>(path: T) -> anyhow::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    create_table(&conn, "test")?;
    create_table(&conn, "test1")?;
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

fn from_row(row: &Row<'_>) -> rusqlite::Result<TestRow> {
    return Ok(TestRow {
        id: row.get_unwrap(0),
        real: row.get_unwrap(1),
        text: row.get_unwrap(2),
        blob: row.get_unwrap(3),
    });
}

fn are_tables_equal(
    conn1: &Connection,
    conn2: &Connection,
    table_name: &str,
) -> anyhow::Result<bool> {
    let query = format!("SELECT * FROM {table_name} ORDER BY id");
    let mut stmt1 = conn1.prepare(&query)?;
    let mut stmt2 = conn2.prepare(&query)?;
    let mut rows1 = stmt1.query_map([], from_row)?;
    let mut rows2 = stmt2.query_map([], from_row)?;

    while let Some(row1) = rows1.next() {
        let row1 = row1?;
        if let Some(row2) = rows2.next() {
            let row2 = row2?;
            if row1 != row2 {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }
    if rows2.next().is_none() {
        return Ok(true);
    }
    return Ok(false);
}

fn are_dbs_equal(conn1: &Connection, conn2: &Connection) -> anyhow::Result<bool> {
    return Ok(are_tables_equal(conn1, conn2, "test")? && are_tables_equal(conn1, conn2, "test1")?);
}

#[test]
fn empty() -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let input_db_path = tmp_dir.path().join("input.db");
    let output_db_path = tmp_dir.path().join("output.db");

    let in_conn = create_sqlite_db(&input_db_path)?;
    let out_conn = create_sqlite_db(&output_db_path)?;
    assert!(are_dbs_equal(&in_conn, &out_conn)?);

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Sqlite(format!("sqlite://{}", input_db_path.to_str().unwrap())),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec![],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert!(are_dbs_equal(&in_conn, &out_conn)?);

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Sqlite(format!("sqlite://{}", input_db_path.to_str().unwrap())),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert!(are_dbs_equal(&in_conn, &out_conn)?);
    return Ok(());
}

#[test]
fn one_table() -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let input_db_path = tmp_dir.path().join("input.db");
    let output_db_path = tmp_dir.path().join("output.db");

    let in_conn = create_sqlite_db(&input_db_path)?;
    fill_table(&in_conn, "test", 1000)?;
    let out_conn = create_sqlite_db(&output_db_path)?;
    assert!(!are_dbs_equal(&in_conn, &out_conn)?);

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Sqlite(format!("sqlite://{}", input_db_path.to_str().unwrap())),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert!(are_dbs_equal(&in_conn, &out_conn)?);
    return Ok(());
}

#[test]
fn multiple_tables() -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let input_db_path = tmp_dir.path().join("input.db");
    let output_db_path = tmp_dir.path().join("output.db");

    let in_conn = create_sqlite_db(&input_db_path)?;
    fill_table(&in_conn, "test", 1000)?;
    fill_table(&in_conn, "test1", 100)?;
    let out_conn = create_sqlite_db(&output_db_path)?;
    assert!(!are_dbs_equal(&in_conn, &out_conn)?);

    let args = db_mover::args::Args {
        input: db_mover::uri::URI::Sqlite(format!("sqlite://{}", input_db_path.to_str().unwrap())),
        output: db_mover::uri::URI::Sqlite(format!(
            "sqlite://{}",
            output_db_path.to_str().unwrap()
        )),
        table: vec!["test".to_owned(), "test1".to_owned()],
        queue_size: Some(100_000),
    };
    db_mover::run(args);

    assert!(are_dbs_equal(&in_conn, &out_conn)?);
    return Ok(());
}
