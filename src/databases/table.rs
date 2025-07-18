use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;

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
}

pub type Row = Vec<Value>;

#[derive(Clone, Copy, Debug, PartialEq)]
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
}

impl FromStr for ColumnType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ColumnType, Self::Err> {
        let formated = s.trim().to_lowercase();
        if formated.starts_with("varchar")
            || formated.starts_with("nvarchar")
            || formated.starts_with("nchar")
            || formated.starts_with("char")
        {
            return Ok(ColumnType::String);
        }
        return match formated.as_str() {
            "tinyint" | "smallint" | "smallserial" => Ok(ColumnType::I16),
            "integer" | "serial" | "int" => Ok(ColumnType::I32),
            "bigint" | "bigserial" => Ok(ColumnType::I64),
            "float" | "real" => Ok(ColumnType::F32),
            "double" | "double precision" => Ok(ColumnType::F64),
            "bool" | "boolean" => Ok(ColumnType::Bool),
            "character" | "varchar" | "nvarchar" | "char" | "nchar" | "clob" | "text"
            | "bpchar" => Ok(ColumnType::String),

            "blob" | "bytea" => Ok(ColumnType::Bytes),
            "timestamptz" => Ok(ColumnType::Timestamptz),
            "datetime" | "timestamp" => Ok(ColumnType::Timestamp),
            "date" => Ok(ColumnType::Date),
            "time" => Ok(ColumnType::Time),
            "json" | "jsonb" => Ok(ColumnType::Json),
            "uuid" => Ok(ColumnType::Uuid),
            _ => Err(anyhow::anyhow!("Unknown column type {s}")),
        };
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
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
