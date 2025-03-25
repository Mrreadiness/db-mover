pub mod args;
pub mod channel;
pub mod databases;
pub mod error;
pub mod progress;
pub mod table_migrator;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    for table in &args.table {
        let migrator = table_migrator::TableMigrator::new(&args, table.to_owned())?;
        migrator.run()?;
    }
    return Ok(());
}
