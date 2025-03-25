use std::sync::atomic::Ordering;

use anyhow::Context;

use crate::{
    args::Args,
    channel,
    databases::{
        table::Row,
        traits::{DBReader, DBWriter},
    },
    error::Error,
    progress::TableMigrationProgress,
};

pub struct TableMigrator {
    reader: Box<dyn DBReader>,
    writers: Vec<Box<dyn DBWriter>>,
    tracker: TableMigrationProgress,
    table: String,
    sender: channel::Sender,
    reciever: channel::Reciever,
    batch_write_size: usize,
    batch_write_retries: usize,
    stopped: std::sync::atomic::AtomicBool,
}

impl TableMigrator {
    pub fn new(args: &Args, table: String) -> anyhow::Result<TableMigrator> {
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
            tracker: TableMigrationProgress::new(&table, table_info.num_rows, args.quiet),
            table,
            sender,
            reciever,
            batch_write_size: args.batch_write_size,
            batch_write_retries: args.batch_write_retries,
            stopped: std::sync::atomic::AtomicBool::new(false),
        });
    }
    fn start_reading(
        mut reader: Box<dyn DBReader>,
        sender: channel::Sender,
        tracker: &TableMigrationProgress,
        table: &str,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> Result<(), Error> {
        let iterator = reader.read_iter(table)?;
        for result in iterator {
            if stopped.load(Ordering::Relaxed) {
                return Err(Error::Stopped);
            }
            let row = result?;
            sender.send(row).map_err(|_| Error::Stopped)?;
        }
        tracker.reader.finish();
        return Ok(());
    }

    fn start_writing(
        mut writer: Box<dyn DBWriter>,
        reciever: channel::Reciever,
        tracker: &TableMigrationProgress,
        table: &str,
        batch_size: usize,
        batch_retries: usize,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> Result<(), Error> {
        let mut batch: Vec<Row> = Vec::with_capacity(batch_size);
        while let Ok(row) = reciever.recv() {
            if stopped.load(Ordering::Relaxed) {
                return Err(Error::Stopped);
            }
            batch.push(row);
            if batch.len() == batch_size {
                writer.write_batch_with_retry(&batch, table, batch_retries)?;
                tracker.writer.inc(batch.len().try_into().unwrap());
                batch.clear();
            }
        }
        if !batch.is_empty() {
            writer.write_batch_with_retry(&batch, table, batch_retries)?;
            tracker.writer.inc(batch.len().try_into().unwrap());
        }
        tracker.writer.finish();
        return Ok(());
    }

    pub fn run(self) -> anyhow::Result<()> {
        let process_result = |r: Result<(), Error>| match r {
            Ok(()) | Err(Error::Stopped) => Ok(()),
            Err(Error::Other(e)) => {
                self.stopped.store(true, Ordering::SeqCst);
                Err(e)
            }
        };
        return std::thread::scope(|s| {
            let mut handles = Vec::new();
            handles.push(s.spawn(|| {
                return process_result(Self::start_reading(
                    self.reader,
                    self.sender,
                    &self.tracker,
                    &self.table,
                    &self.stopped,
                ));
            }));
            for writer in self.writers {
                handles.push(s.spawn(|| {
                    return process_result(Self::start_writing(
                        writer,
                        self.reciever.clone(),
                        &self.tracker,
                        &self.table,
                        self.batch_write_size,
                        self.batch_write_retries,
                        &self.stopped,
                    ));
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
