use chrono::NaiveDateTime;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    F64(f64),
    Timestamp(NaiveDateTime),
    Null,
}

pub type Row = Vec<Value>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ColumnType {
    String,
    Bytes,
    I64,
    F64,
    Timestamp,
}

impl FromStr for ColumnType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ColumnType, Self::Err> {
        let foramted = s.trim().to_lowercase();
        if foramted.starts_with("varchar")
            | foramted.starts_with("nvarchar")
            | foramted.starts_with("nchar")
        {
            return Ok(ColumnType::String);
        }
        return match foramted.as_str() {
            "tinyint" | "smallint" | "integer" | "bigint" => Ok(ColumnType::I64),
            "float" | "real" | "double" | "double precision" | "numeric" | "decimal" => {
                Ok(ColumnType::F64)
            }
            "character" | "varchar" | "nvarchar" | "char" | "nchar" | "clob" | "text" => {
                Ok(ColumnType::String)
            }

            "blob" | "bytea" => Ok(ColumnType::Bytes),
            "datetime" | "timestamp" | "timestamptz" => Ok(ColumnType::Timestamp),
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
