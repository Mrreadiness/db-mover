use std::io::Write;

use postgres::types::Type;

use crate::databases::table::Value;

impl Value {
    pub(crate) fn write_postgres_bytes(
        &self,
        column_type: &Type,
        writer: &mut impl Write,
    ) -> anyhow::Result<()> {
        if self == &Value::Null {
            writer.write_all(&(-1_i32).to_be_bytes())?;
            return Ok(());
        }
        match (column_type, self) {
            (&Type::INT8, &Value::I64(num)) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::INT4, &Value::I64(num)) => {
                let num = i32::try_from(num)?;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::INT2, &Value::I64(num)) => {
                let num = i16::try_from(num)?;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::FLOAT8, &Value::F64(num)) => {
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::FLOAT4, &Value::F64(num)) => {
                let num = num as f32;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            (&Type::BYTEA, Value::Bytes(bytes)) => {
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(bytes)?;
            }
            (&Type::VARCHAR, Value::String(string))
            | (&Type::TEXT, Value::String(string))
            | (&Type::BPCHAR, Value::String(string)) => {
                let bytes = string.clone().into_bytes();
                writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
                writer.write_all(&bytes)?;
            }
            (&Type::TIME, &Value::I64(num)) => {
                let num = i16::try_from(num)?;
                writer.write_all(&(size_of_val(&num) as i32).to_be_bytes())?;
                writer.write_all(&num.to_be_bytes())?;
            }
            _ => return Err(anyhow::anyhow!("Unsuppoerted type conversion")),
        };
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_null() {
        let mut buffer = Vec::new();
        Value::Null
            .write_postgres_bytes(&Type::INT4, &mut buffer)
            .unwrap();
        assert_eq!(buffer, (-1_i32).to_be_bytes().to_vec());
    }

    #[test]
    fn test_write_int8() {
        let mut buffer = Vec::new();
        Value::I64(42)
            .write_postgres_bytes(&Type::INT8, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(8_i32).to_be_bytes());
        expected.extend(&42_i64.to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_int4() {
        let mut buffer = Vec::new();
        Value::I64(42)
            .write_postgres_bytes(&Type::INT4, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(4_i32).to_be_bytes());
        expected.extend(&(42_i32).to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_int2() {
        let mut buffer = Vec::new();
        Value::I64(42)
            .write_postgres_bytes(&Type::INT2, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(2_i32).to_be_bytes());
        expected.extend(&(42_i16).to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_float8() {
        let mut buffer = Vec::new();
        Value::F64(3.14)
            .write_postgres_bytes(&Type::FLOAT8, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(8_i32).to_be_bytes());
        expected.extend(&3.14f64.to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_float4() {
        let mut buffer = Vec::new();
        Value::F64(3.14)
            .write_postgres_bytes(&Type::FLOAT4, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(4_i32).to_be_bytes());
        expected.extend(&(3.14f32).to_be_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_bytea() {
        let mut buffer = Vec::new();
        Value::Bytes(vec![1, 2, 3])
            .write_postgres_bytes(&Type::BYTEA, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(3_i32).to_be_bytes());
        expected.extend(&[1, 2, 3]);
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_text() {
        let mut buffer = Vec::new();
        Value::String("hello".into())
            .write_postgres_bytes(&Type::TEXT, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(5_i32).to_be_bytes());
        expected.extend("hello".as_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_write_varchar() {
        let mut buffer = Vec::new();
        Value::String("world".into())
            .write_postgres_bytes(&Type::VARCHAR, &mut buffer)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend(&(5_i32).to_be_bytes());
        expected.extend("world".as_bytes());
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_unsupported_conversion() {
        let mut buffer = Vec::new();
        let result = Value::I64(42).write_postgres_bytes(&Type::BYTEA, &mut buffer);
        assert!(result.is_err());
    }
}
