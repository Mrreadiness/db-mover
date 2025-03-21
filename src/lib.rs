use anyhow::Context;

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    let mut writer = args.output.create_writer()?;
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let mut reader = args.input.create_reader()?;
        let table_size = reader.get_count(table)?.into();
        let tracker = progress::TableMigrationProgress::new(&args, table, table_size);

        let reader_handle = std::thread::spawn({
            let table = table.clone();
            move || {
                return reader.start_reading(sender, &table, tracker.reader);
            }
        });
        writer
            .start_writing(
                reciever,
                table,
                args.batch_write_size,
                args.batch_write_retries,
                tracker.writer,
            )
            .context("Writer failed")?;
        let reader_result = reader_handle.join().expect("Reader panicked");
        reader_result.context("Reader failed")?;
    }
    return Ok(());
}
