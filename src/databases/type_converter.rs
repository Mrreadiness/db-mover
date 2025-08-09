use crate::databases::mysql::value::MysqlTypeConverter;
use crate::databases::postgres::value::PostgresTypeConverter;
use crate::databases::sqlite::value::SqliteTypeConverter;

pub trait TypeConveter:
    PostgresTypeConverter + MysqlTypeConverter + SqliteTypeConverter + 'static
{
}

pub struct DefaultTypeConverter;

impl PostgresTypeConverter for DefaultTypeConverter {}
impl MysqlTypeConverter for DefaultTypeConverter {}
impl SqliteTypeConverter for DefaultTypeConverter {}

impl TypeConveter for DefaultTypeConverter {}
