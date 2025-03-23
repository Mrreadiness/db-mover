use anyhow::Context;
use databases::traits::{DBReader, DBWriter};

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod uri;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    for table in &args.table {
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
        process_table(&args, reader, writer, tracker, table)?;
    }
    return Ok(());
}

fn process_table(
    args: &args::Args,
    mut reader: Box<dyn DBReader>,
    mut writer: Box<dyn DBWriter>,
    tracker: progress::TableMigrationProgress,
    table: &str,
) -> anyhow::Result<()> {
    return std::thread::scope(|s| {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let mut handles = Vec::new();
        handles.push(s.spawn(|| {
            return reader
                .start_reading(sender, table, tracker.reader)
                .context("Reader failed");
        }));
        if args.writer_workers > 1 {
            handles.push(s.spawn({
                let mut new_writer = match writer.opt_clone() {
                    Some(Ok(new_writer)) => new_writer,
                    Some(Err(e)) => return Err(e),
                    None => {
                        return Err(anyhow::anyhow!(
                            "This type of databases doesn't support mutiple writers"
                        ))
                    }
                };
                move || {
                    return new_writer
                        .start_writing(
                            reciever.clone(),
                            table,
                            args.batch_write_size,
                            args.batch_write_retries,
                            tracker.writer.clone(),
                        )
                        .context("Writer failed");
                }
            }));
        } else {
            handles.push(s.spawn(|| {
                return writer
                    .start_writing(
                        reciever,
                        table,
                        args.batch_write_size,
                        args.batch_write_retries,
                        tracker.writer,
                    )
                    .context("Writer failed");
            }));
        }
        let mut got_error = false;
        for handle in handles {
            let _ = handle.join().unwrap().map_err(|err| {
                tracing::error!("{}", err);
                got_error = true;
                return err;
            });
        }
        if got_error {
            return Err(anyhow::anyhow!("Stopped because of the error"));
        }
        return Ok(());
    });
}
