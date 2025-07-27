use crate::databases::mysql::value::MysqlTypeConvertor;
use crate::databases::sqlite::value::SqliteTypeConvertor;

pub trait TypeConvetor: MysqlTypeConvertor + SqliteTypeConvertor + 'static {}

pub struct DefaultTypeConvertor;

impl MysqlTypeConvertor for DefaultTypeConvertor {}
impl SqliteTypeConvertor for DefaultTypeConvertor {}

impl TypeConvetor for DefaultTypeConvertor {}
