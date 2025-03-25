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
            tracker.reader.inc(1);
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

#[cfg(test)]
mod tests {
    use crate::databases::table::Table;
    use crate::databases::traits::{DBInfoProvider, ReaderIterator};

    use super::*;
    use mockall::{mock, predicate::*};
    use std::sync::atomic::AtomicBool;

    mock! {
        RowsIter {}

        impl Iterator for RowsIter {
            type Item = anyhow::Result<Row>;

            fn next(&mut self) -> Option<<Self as Iterator>::Item>;
        }
    }

    mock! {
        DB {}

        impl DBInfoProvider for DB {
            fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<Table>;
        }
        impl DBReader for DB {
            fn read_iter<'a>(&'a mut self, table: &str) -> anyhow::Result<ReaderIterator<'a>>;
        }
        impl DBWriter for DB {
            fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()>;
        }
    }
    #[test]
    fn test_reading() {
        let mut db_mock = MockDB::new();
        db_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            let mut count = 0;
            rows.expect_next().returning(move || {
                if count == 5 {
                    return None;
                }
                count += 1;
                return Some(Ok(Row::default()));
            });
            Ok(Box::new(rows))
        });
        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new("test", None, true);
        let stopped = AtomicBool::new(false);

        let result =
            TableMigrator::start_reading(Box::new(db_mock), sender, &tracker, "test", &stopped);
        assert!(matches!(result, Ok(())));
        assert_eq!(tracker.reader.position(), 5);
        assert_eq!(receiver.len(), 5);
    }

    #[test]
    fn test_reading_stops_on_signal() {
        let mut db_mock = MockDB::new();
        db_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            rows.expect_next().returning(|| Some(Ok(Row::default())));
            Ok(Box::new(rows))
        });
        let (sender, _receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new("test", None, true);
        let stopped = AtomicBool::new(true);

        let result =
            TableMigrator::start_reading(Box::new(db_mock), sender, &tracker, "test", &stopped);
        assert!(matches!(result, Err(Error::Stopped)));
    }

    #[test]
    fn test_reading_stops_on_dropped_writers() {
        let mut db_mock = MockDB::new();
        db_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            rows.expect_next().returning(|| Some(Ok(Row::default())));
            Ok(Box::new(rows))
        });
        let (sender, receiver) = channel::create_channel(10);
        drop(receiver);
        let tracker = TableMigrationProgress::new("test", None, true);
        let stopped = AtomicBool::new(true);

        let result =
            TableMigrator::start_reading(Box::new(db_mock), sender, &tracker, "test", &stopped);
        assert!(matches!(result, Err(Error::Stopped)));
    }

    #[test]
    fn test_writing_one_batch() {
        let mut db_mock = MockDB::new();
        let num_rows = 5;
        let batch_size = 10;
        db_mock
            .expect_write_batch()
            .times(1)
            .returning(|_, _| Ok(())); // one batch
        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new("test", None, true);
        for _ in 0..num_rows {
            sender.send(Row::default()).unwrap();
        }
        drop(sender);
        let stopped = AtomicBool::new(false);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            "test",
            batch_size,
            0,
            &stopped,
        );
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_writing_multiple_batches() {
        let mut db_mock = MockDB::new();
        let num_rows = 5;
        let batch_size = 1;
        db_mock
            .expect_write_batch()
            .times(5)
            .returning(|_, _| Ok(())); // one batch
        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new("test", None, true);
        for _ in 0..num_rows {
            sender.send(Row::default()).unwrap();
        }
        drop(sender);
        let stopped = AtomicBool::new(false);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            "test",
            batch_size,
            0,
            &stopped,
        );
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_writing_stops_on_signal() {
        let db_mock = MockDB::new();
        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new("test", None, true);
        sender.send(Row::default()).unwrap();
        let stopped = AtomicBool::new(true);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            "test",
            10,
            3,
            &stopped,
        );
        assert!(matches!(result, Err(Error::Stopped)));
    }

    #[test]
    fn test_writing_stops_on_dropped_reader() {
        let db_mock = MockDB::new();
        let (sender, receiver) = channel::create_channel(10);
        drop(sender);
        let tracker = TableMigrationProgress::new("test", None, true);
        let stopped = AtomicBool::new(true);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            "test",
            10,
            3,
            &stopped,
        );
        assert!(matches!(result, Ok(())));
    }
}
