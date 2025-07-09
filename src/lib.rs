use anyhow::Context;
use tracing::info;

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod retry;
pub mod table_migrator;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    let tables = get_tables(&args)?;
    for table in &tables {
        let reader = args.create_reader()?;
        let writer = args.create_writer()?;
        info!("Processing table \"{table}\"");
        let migrator = table_migrator::TableMigrator::new(reader, writer, table, (&args).into())?;
        if !args.dry_run {
            migrator.run()?;
            info!("Table \"{table}\" moved");
        }
    }
    return Ok(());
}

fn get_tables(args: &args::Args) -> anyhow::Result<Vec<String>> {
    let tables = match args.table.len() {
        0 => {
            let mut reader = args.create_reader()?;
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
        let mut writer = args.create_writer()?;
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
