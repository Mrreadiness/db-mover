use anyhow::Context;
use tracing::info;

use crate::type_convertor::{DefaultTypeConvertor, TypeConvetor};

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod retry;
pub mod table_migrator;
pub mod type_convertor;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    return run_with::<DefaultTypeConvertor>(args);
}

pub fn run_with<T: TypeConvetor>(args: args::Args) -> anyhow::Result<()> {
    let tables = get_tables::<T>(&args)?;
    for table in &tables {
        let reader = args.create_reader::<T>()?;
        let writer = args.create_writer::<T>()?;
        info!("Processing table \"{table}\"");
        let migrator = table_migrator::TableMigrator::new(reader, writer, table, (&args).into())?;
        if !args.dry_run {
            migrator.run()?;
            info!("Table \"{table}\" moved");
        }
    }
    return Ok(());
}

fn get_tables<T: TypeConvetor>(args: &args::Args) -> anyhow::Result<Vec<String>> {
    let tables = match args.table.len() {
        0 => {
            let mut reader = args.create_reader::<T>()?;
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
        let mut writer = args.create_writer::<T>()?;
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
