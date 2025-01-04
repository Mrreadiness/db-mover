#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    F64(f64),
    Null,
}

pub type Row = Vec<Value>;
