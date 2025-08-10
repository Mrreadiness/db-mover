use anyhow::Context;

use crate::{
    args::Args,
    databases::{
        mysql::{MysqlDB, MysqlTypeOptions},
        postgres::PostgresDB,
        sqlite::SqliteDB,
        traits::{DBReader, DBWriter},
        type_converter::{DefaultTypeConverter, TypeConveter},
    },
    uri::URI,
};

pub struct DBFactory<TypeConverterT: TypeConveter = DefaultTypeConverter> {
    _type_converter: std::marker::PhantomData<TypeConverterT>,
}

impl<TypeConverterT: TypeConveter> Default for DBFactory<TypeConverterT> {
    fn default() -> Self {
        return Self {
            _type_converter: std::marker::PhantomData,
        };
    }
}

impl<TypeConverterT: TypeConveter> DBFactory<TypeConverterT> {
    fn build_sqlite(&self, uri: &str) -> anyhow::Result<Box<SqliteDB<TypeConverterT>>> {
        return Ok(Box::new(
            SqliteDB::new(uri).context("Unable to connect to the sqlite")?,
        ));
    }

    fn build_postgres(&self, uri: &str) -> anyhow::Result<Box<PostgresDB<TypeConverterT>>> {
        return Ok(Box::new(
            PostgresDB::new(uri).context("Unable to connect to the postgres")?,
        ));
    }

    fn build_mysql(&self, uri: &str, args: &Args) -> anyhow::Result<Box<MysqlDB<TypeConverterT>>> {
        let options = MysqlTypeOptions {
            binary_16_as_uuid: !args.no_mysql_binary_16_as_uuid,
            ..Default::default()
        };
        let db = MysqlDB::new(uri, options).context("Unable to connect to the mysql")?;
        return Ok(Box::new(db));
    }

    pub fn create_reader(&self, args: &Args) -> anyhow::Result<Box<dyn DBReader>> {
        let reader: Box<dyn DBReader> = match &args.input {
            URI::Sqlite(uri) => self.build_sqlite(uri)?,
            URI::Postgres(uri) => self.build_postgres(uri)?,
            URI::Mysql(uri) => self.build_mysql(uri, args)?,
        };
        return Ok(reader);
    }

    pub fn create_writer(&self, args: &Args) -> anyhow::Result<Box<dyn DBWriter>> {
        let writer: Box<dyn DBWriter> = match &args.output {
            URI::Sqlite(uri) => self.build_sqlite(uri)?,
            URI::Postgres(uri) => self.build_postgres(uri)?,
            URI::Mysql(uri) => self.build_mysql(uri, args)?,
        };
        return Ok(writer);
    }
}
