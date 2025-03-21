#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    F64(f64),
    Null,
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    String,
    Bytes,
    I64,
    F64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    name: String,
    column_type: ColumnType,
    nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    name: String,
    num_rows: u64,
}
