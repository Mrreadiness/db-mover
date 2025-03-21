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
        let table_info = reader
            .get_table_info(table)
            .context("Unable to get information about source table")?;
        let writer_table_info = writer
            .get_table_info(table)
            .context("Unable to get information about destination table")?;
        if writer_table_info.num_rows > 0 {
            return Err(anyhow::anyhow!("Destination table should be empty"));
        }
        let tracker = progress::TableMigrationProgress::new(&args, table, table_info.num_rows);

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
