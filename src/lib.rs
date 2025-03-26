pub mod args;
pub mod channel;
pub mod databases;
pub mod error;
pub mod progress;
pub mod table_migrator;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    for table in &args.table {
        let reader = args.input.create_reader()?;
        let writer = args.output.create_writer()?;
        let migrator = table_migrator::TableMigrator::new(reader, writer, table, (&args).into())?;
        migrator.run()?;
    }
    return Ok(());
}
