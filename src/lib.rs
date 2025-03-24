use std::sync::atomic::Ordering;

use anyhow::Context;
use databases::traits::{DBReader, DBWriter};

pub mod args;
pub mod channel;
pub mod databases;
pub mod error;
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
    let stopped = std::sync::atomic::AtomicBool::new(false);
    return std::thread::scope(|s| {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let mut handles = Vec::new();
        handles.push(s.spawn(|| {
            return match reader.start_reading(sender, table, tracker.reader, &stopped) {
                Ok(()) | Err(error::Error::Stopped) => Ok(()),
                Err(error::Error::Other(e)) => {
                    stopped.store(true, Ordering::Relaxed);
                    Err(e.context("Reader failed"))
                }
            };
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
                let stopped = &stopped;
                move || {
                    return match new_writer.start_writing(
                        reciever.clone(),
                        table,
                        args.batch_write_size,
                        args.batch_write_retries,
                        tracker.writer.clone(),
                        stopped,
                    ) {
                        Ok(()) | Err(error::Error::Stopped) => Ok(()),
                        Err(error::Error::Other(e)) => {
                            stopped.store(true, Ordering::Relaxed);
                            Err(e.context("Writer failed"))
                        }
                    };
                }
            }));
        } else {
            handles.push(s.spawn(|| {
                return match writer.start_writing(
                    reciever,
                    table,
                    args.batch_write_size,
                    args.batch_write_retries,
                    tracker.writer,
                    &stopped,
                ) {
                    Ok(()) | Err(error::Error::Stopped) => Ok(()),
                    Err(error::Error::Other(e)) => {
                        stopped.store(true, Ordering::Relaxed);
                        Err(e.context("Writer failed"))
                    }
                };
            }));
        }
        // Only first (original) error expected
        for handle in handles {
            handle.join().unwrap()?;
        }
        return Ok(());
    });
}
