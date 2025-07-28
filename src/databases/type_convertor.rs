use crate::databases::mysql::value::MysqlTypeConvertor;
use crate::databases::postgres::value::PostgresTypeConvertor;
use crate::databases::sqlite::value::SqliteTypeConvertor;

pub trait TypeConvetor:
    PostgresTypeConvertor + MysqlTypeConvertor + SqliteTypeConvertor + 'static
{
}

pub struct DefaultTypeConvertor;

impl PostgresTypeConvertor for DefaultTypeConvertor {}
impl MysqlTypeConvertor for DefaultTypeConvertor {}
impl SqliteTypeConvertor for DefaultTypeConvertor {}

impl TypeConvetor for DefaultTypeConvertor {}
