use anyhow::Context;
use tracing::info;

use crate::databases::{
    factory::DBFactory,
    type_converter::{DefaultTypeConverter, TypeConveter},
};

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod retry;
pub mod table_migrator;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    return run_with::<DefaultTypeConverter>(args);
}

pub fn run_with<T: TypeConveter>(args: args::Args) -> anyhow::Result<()> {
    let factory: DBFactory<T> = DBFactory::default();
    let tables = get_tables(&args, &factory)?;
    for table in &tables {
        let reader = factory.create_reader(&args)?;
        let writer = factory.create_writer(&args)?;
        info!("Processing table \"{table}\"");
        let migrator = table_migrator::TableMigrator::new(reader, writer, table, (&args).into())?;
        if !args.dry_run {
            migrator.run()?;
            info!("Table \"{table}\" moved");
        }
    }
    return Ok(());
}

fn get_tables(
    args: &args::Args,
    factory: &DBFactory<impl TypeConveter>,
) -> anyhow::Result<Vec<String>> {
    let tables = match args.table.len() {
        0 => {
            let mut reader = factory.create_reader(args)?;
            reader
                .get_tables()
                .context("Failed to get list of tables from input database")?
        }
        _ => args.table.clone(),
    };
    info!(
        "Tables to move: {}",
        tables
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let writer_tables = {
        let mut writer = factory.create_writer(args)?;
        writer
            .get_tables()
            .context("Failed to get list of tables from output database")?
    };
    for table in &tables {
        if !writer_tables.contains(table) {
            return Err(anyhow::anyhow!(
                "Table \"{table}\" not found in the output database"
            ));
        }
    }
    return Ok(tables);
}
