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
        let migrator = TableMigrator::new(&args, table.to_owned())?;
        migrator.run()?;
    }
    return Ok(());
}

struct TableMigrator {
    reader: Box<dyn DBReader>,
    writers: Vec<Box<dyn DBWriter>>,
    tracker: progress::TableMigrationProgress,
    table: String,
    sender: channel::Sender,
    reciever: channel::Reciever,
    batch_write_size: usize,
    batch_write_retries: usize,
    stopped: std::sync::atomic::AtomicBool,
}

impl TableMigrator {
    fn new(args: &args::Args, table: String) -> anyhow::Result<TableMigrator> {
        let mut reader = args.input.create_reader()?;
        let mut writer = args.output.create_writer()?;
        let table_info = reader
            .get_table_info(&table, args.no_count)
            .context("Unable to get information about source table")?;
        let writer_table_info = writer
            .get_table_info(&table, false)
            .context("Unable to get information about destination table")?;
        if writer_table_info.num_rows != Some(0) {
            return Err(anyhow::anyhow!("Destination table should be empty"));
        }
        let mut writers = Vec::new();
        if args.writer_workers > 1 {
            for _ in 0..args.writer_workers {
                writers.push(writer.opt_clone()?);
            }
        } else {
            writers.push(writer);
        }
        let (sender, reciever) = channel::create_channel(args.queue_size);
        return Ok(TableMigrator {
            reader,
            writers,
            tracker: progress::TableMigrationProgress::new(&table, table_info.num_rows, args.quiet),
            table,
            sender,
            reciever,
            batch_write_size: args.batch_write_size,
            batch_write_retries: args.batch_write_retries,
            stopped: std::sync::atomic::AtomicBool::new(false),
        });
    }
    fn start_reader(
        mut reader: Box<dyn DBReader>,
        tracker: &progress::TableMigrationProgress,
        table: &str,
        sender: channel::Sender,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> anyhow::Result<()> {
        return match reader.start_reading(sender, table, tracker.reader.clone(), stopped) {
            Ok(()) | Err(error::Error::Stopped) => Ok(()),
            Err(error::Error::Other(e)) => {
                stopped.store(true, Ordering::Relaxed);
                Err(e.context("Reader failed"))
            }
        };
    }

    fn start_writer(
        mut writer: Box<dyn DBWriter>,
        reciever: channel::Reciever,
        tracker: &progress::TableMigrationProgress,
        table: &str,
        batch_write_size: usize,
        batch_write_retries: usize,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> anyhow::Result<()> {
        return match writer.start_writing(
            reciever,
            table,
            batch_write_size,
            batch_write_retries,
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

    fn run(self) -> anyhow::Result<()> {
        return std::thread::scope(|s| {
            let mut handles = Vec::new();
            handles.push(s.spawn(|| {
                return Self::start_reader(
                    self.reader,
                    &self.tracker,
                    &self.table,
                    self.sender,
                    &self.stopped,
                );
            }));
            for writer in self.writers {
                handles.push(s.spawn(|| {
                    return Self::start_writer(
                        writer,
                        self.reciever.clone(),
                        &self.tracker,
                        &self.table,
                        self.batch_write_size,
                        self.batch_write_retries,
                        &self.stopped,
                    );
                }));
            }
            // Only first (original) error expected
            for handle in handles {
                handle.join().unwrap()?;
            }
            return Ok(());
        });
    }
}
