use crate::databases::mysql::type_converter::MysqlTypeConverter;
use crate::databases::postgres::type_converter::PostgresTypeConverter;
use crate::databases::sqlite::type_converter::SqliteTypeConverter;

pub trait TypeConveter:
    PostgresTypeConverter + MysqlTypeConverter + SqliteTypeConverter + 'static
{
}

pub struct DefaultTypeConverter;

impl PostgresTypeConverter for DefaultTypeConverter {}
impl MysqlTypeConverter for DefaultTypeConverter {}
impl SqliteTypeConverter for DefaultTypeConverter {}

impl TypeConveter for DefaultTypeConverter {}
