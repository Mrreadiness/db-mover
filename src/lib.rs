use anyhow::Context;

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let mut reader = args.input.create_reader()?;
        let mut writer = args.output.create_writer()?;
        let table_info = reader
            .get_table_info(table, args.no_count)
            .context("Unable to get information about source table")?;
        let writer_table_info = writer
            .get_table_info(table, false)
            .context("Unable to get information about destination table")?;
        if writer_table_info.num_rows != Some(0) {
            return Err(anyhow::anyhow!("Destination table should be empty"));
        }
        let tracker = progress::TableMigrationProgress::new(&args, table, table_info.num_rows);

        std::thread::scope(|s| {
            s.spawn(|| {
                return reader
                    .start_reading(sender, table, tracker.reader)
                    .context("Reader failed");
            });
            if args.writer_workers > 1 {
                s.spawn({
                    let mut new_writer = match writer.opt_clone() {
                        Some(Ok(new_writer)) => new_writer,
                        Some(Err(e)) => return Err(e),
                        None => {
                            return Err(anyhow::anyhow!(
                                "This type of databases doesn't support mutiple writers"
                            ))
                        }
                    };
                    let reciever = reciever.clone();
                    let progress = tracker.writer.clone();
                    move || {
                        return new_writer
                            .start_writing(
                                reciever,
                                table,
                                args.batch_write_size,
                                args.batch_write_retries,
                                progress,
                            )
                            .context("Writer failed");
                    }
                });
            } else {
                s.spawn(|| {
                    return writer
                        .start_writing(
                            reciever,
                            table,
                            args.batch_write_size,
                            args.batch_write_retries,
                            tracker.writer,
                        )
                        .context("Writer failed");
                });
            }
            return Ok(());
        })?;
    }
    return Ok(());
}
