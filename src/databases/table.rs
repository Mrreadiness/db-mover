use chrono::{NaiveDate, NaiveDateTime};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    I32(i32),
    I16(i16),
    F64(f64),
    F32(f32),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
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
    Timestamp,
    Date,
    Uuid,
    Json,
}

impl FromStr for ColumnType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ColumnType, Self::Err> {
        let foramted = s.trim().to_lowercase();
        if foramted.starts_with("varchar")
            | foramted.starts_with("nvarchar")
            | foramted.starts_with("nchar")
            | foramted.starts_with("char")
        {
            return Ok(ColumnType::String);
        }
        return match foramted.as_str() {
            "tinyint" | "smallint" => Ok(ColumnType::I16),
            "integer" => Ok(ColumnType::I32),
            "bigint" => Ok(ColumnType::I64),
            "float" | "real" => Ok(ColumnType::F32),
            "double" | "double precision" | "numeric" | "decimal" => Ok(ColumnType::F64),
            "character" | "varchar" | "nvarchar" | "char" | "nchar" | "clob" | "text"
            | "bpchar" => Ok(ColumnType::String),

            "blob" | "bytea" => Ok(ColumnType::Bytes),
            "datetime" | "timestamp" | "timestamptz" => Ok(ColumnType::Timestamp),
            "date" => Ok(ColumnType::Date),
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
