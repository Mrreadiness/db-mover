use chrono::NaiveDateTime;
use fake::Dummy;

#[derive(Debug, Dummy, Clone)]
pub struct TestRow {
    pub id: i64,
    pub real: f32,
    pub text: String,
    #[dummy(expr = "fake::vec![u8; 0..200]")]
    pub blob: Vec<u8>,
    #[dummy(
        expr = "chrono::NaiveDate::from_ymd_opt(2016, 7, 8).unwrap().and_hms_opt(0, 0, 0).unwrap()"
    )]
    pub timestamp: NaiveDateTime,
}

fn approx_equal_f32(a: f32, b: f32, epsilon: f32) -> bool {
    (a - b).abs() < epsilon
}

impl PartialEq for TestRow {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && approx_equal_f32(self.real, other.real, 1e-6)
            && self.text == other.text
            && self.blob == other.blob
            && self.timestamp == other.timestamp
    }
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

impl From<mysql::Row> for TestRow {
    fn from(row: mysql::Row) -> Self {
        return Self {
            id: row.get(0).unwrap(),
            real: row.get(1).unwrap(),
            text: row.get(2).unwrap(),
            blob: row.get(3).unwrap(),
            timestamp: row.get(4).unwrap(),
        };
    }
}
