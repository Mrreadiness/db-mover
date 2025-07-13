use crate::databases::sqlite::value::SqliteTypeConvertor;

pub trait TypeConvetor: SqliteTypeConvertor + 'static {}

pub struct DefaultTypeConvertor;

impl TypeConvetor for DefaultTypeConvertor {}
