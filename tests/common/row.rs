use chrono::NaiveDateTime;
use fake::Dummy;

#[derive(PartialEq, Debug, Dummy)]
pub struct TestRow {
    pub id: i64,
    pub real: f32,
    pub text: String,
    pub blob: Vec<u8>,
    pub timestamp: NaiveDateTime,
}

impl From<postgres::Row> for TestRow {
    fn from(row: postgres::Row) -> Self {
        return Self {
            id: row.get(0),
            real: row.get(1),
            text: row.get(2),
            blob: row.get(3),
            timestamp: row.get(4),
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
            timestamp: row.get_unwrap(4),
        };
    }
}
