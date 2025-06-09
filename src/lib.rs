use tracing::info;

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod retry;
pub mod table_migrator;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    for table in &args.table {
        let reader = args.create_reader()?;
        let writer = args.create_writer()?;
        info!("Processing table {table}");
        let migrator = table_migrator::TableMigrator::new(reader, writer, table, (&args).into())?;
        if !args.dry_run {
            migrator.run()?;
            info!("Table {table} moved");
        }
    }
    return Ok(());
}
