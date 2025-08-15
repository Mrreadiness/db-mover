use std::borrow::Cow;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Bytes(bytes::Bytes),
    I64(i64),
    I32(i32),
    I16(i16),
    F64(f64),
    F32(f32),
    Decimal(Decimal),
    Bool(bool),
    Timestamptz(DateTime<Utc>),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Json(serde_json::Value),
    Uuid(uuid::Uuid),
    Null,
    Custom(bytes::Bytes),
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    String,
    Bytes,
    I64,
    I32,
    I16,
    F64,
    F32,
    Decimal,
    Bool,
    Timestamptz,
    Timestamp,
    Date,
    Time,
    Uuid,
    Json,
    Custom(Cow<'static, str>),
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub name: String,
    pub num_rows: Option<u64>,
    pub columns: Vec<Column>,
}

impl TableInfo {
    pub fn column_names(&self) -> Vec<&str> {
        return self.columns.iter().map(|c| c.name.as_str()).collect();
    }
}
