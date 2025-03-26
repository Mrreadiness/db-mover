use std::sync::atomic::Ordering;

use anyhow::Context;

use crate::{
    args::Args,
    channel,
    databases::{
        table::{Row, TableInfo},
        traits::{DBReader, DBWriter},
    },
    error::Error,
    progress::TableMigrationProgress,
};

#[derive(Clone, Debug, PartialEq)]
pub struct TableMigratorSettings {
    queue_size: usize,
    quiet: bool,
    no_count: bool,
    writer_workers: usize,
    batch_write_size: usize,
    batch_write_retries: usize,
}

impl From<&Args> for TableMigratorSettings {
    fn from(args: &Args) -> Self {
        return Self {
            queue_size: args.queue_size,
            quiet: args.quiet,
            no_count: args.no_count,
            writer_workers: args.writer_workers,
            batch_write_size: args.batch_write_size,
            batch_write_retries: args.batch_write_retries,
        };
    }
}

pub struct TableMigrator {
    reader: Box<dyn DBReader>,
    writers: Vec<Box<dyn DBWriter>>,
    tracker: TableMigrationProgress,
    target_format: TableInfo,
    sender: channel::Sender,
    reciever: channel::Reciever,
    stopped: std::sync::atomic::AtomicBool,
    settings: TableMigratorSettings,
}

impl TableMigrator {
    pub fn new(
        mut reader: Box<dyn DBReader>,
        mut writer: Box<dyn DBWriter>,
        table: &str,
        settings: TableMigratorSettings,
    ) -> anyhow::Result<TableMigrator> {
        let reader_table_info = reader
            .get_table_info(table, settings.no_count)
            .context("Unable to get information about source table")?;
        let writer_table_info = writer
            .get_table_info(table, false)
            .context("Unable to get information about destination table")?;
        if writer_table_info.num_rows != Some(0) {
            return Err(anyhow::anyhow!("Destination table should be empty"));
        }
        let mut writers = Vec::new();
        if settings.writer_workers > 1 {
            for _ in 0..settings.writer_workers {
                writers.push(writer.opt_clone()?);
            }
        } else {
            writers.push(writer);
        }
        let tracker =
            TableMigrationProgress::new(table, reader_table_info.num_rows, settings.quiet);
        let (sender, reciever) = channel::create_channel(settings.queue_size);
        return Ok(TableMigrator {
            reader,
            writers,
            tracker,
            target_format: writer_table_info,
            sender,
            reciever,
            stopped: std::sync::atomic::AtomicBool::new(false),
            settings,
        });
    }
    fn start_reading(
        mut reader: Box<dyn DBReader>,
        sender: channel::Sender,
        tracker: &TableMigrationProgress,
        target_format: TableInfo,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> Result<(), Error> {
        let iterator = reader.read_iter(target_format)?;
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
                    self.target_format.clone(),
                    &self.stopped,
                ));
            }));
            for writer in self.writers {
                handles.push(s.spawn(|| {
                    return process_result(Self::start_writing(
                        writer,
                        self.reciever.clone(),
                        &self.tracker,
                        &self.target_format.name,
                        self.settings.batch_write_size,
                        self.settings.batch_write_retries,
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
    use crate::databases::table::TableInfo;
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
            fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo>;
        }
        impl DBReader for DB {
            fn read_iter<'a>(&'a mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'a>>;
        }
        impl DBWriter for DB {
            fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()>;
        }
    }

    impl Default for TableMigratorSettings {
        fn default() -> Self {
            Self {
                queue_size: 10,
                quiet: true,
                no_count: false,
                writer_workers: 1,
                batch_write_size: 10,
                batch_write_retries: 0,
            }
        }
    }

    const NUM_ROWS: u64 = 5;
    const TABLE_NAME: &str = "test";

    impl TableInfo {
        fn default_in() -> Self {
            Self {
                name: TABLE_NAME.to_string(),
                num_rows: None,
                columns: Vec::new(),
            }
        }

        fn default_out() -> Self {
            Self {
                name: TABLE_NAME.to_string(),
                num_rows: Some(0),
                columns: Vec::new(),
            }
        }
    }

    #[test]
    fn test_reading() {
        let mut db_mock = MockDB::new();
        db_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            let mut count = 0;
            rows.expect_next().returning(move || {
                if count == NUM_ROWS {
                    return None;
                }
                count += 1;
                return Some(Ok(Row::default()));
            });
            Ok(Box::new(rows))
        });
        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        let stopped = AtomicBool::new(false);

        let result = TableMigrator::start_reading(
            Box::new(db_mock),
            sender,
            &tracker,
            TableInfo::default_out(),
            &stopped,
        );
        assert!(matches!(result, Ok(())));
        assert_eq!(tracker.reader.position(), NUM_ROWS);
        assert_eq!(receiver.len() as u64, NUM_ROWS);
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
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        let stopped = AtomicBool::new(true);

        let result = TableMigrator::start_reading(
            Box::new(db_mock),
            sender,
            &tracker,
            TableInfo::default_out(),
            &stopped,
        );
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
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        let stopped = AtomicBool::new(true);

        let result = TableMigrator::start_reading(
            Box::new(db_mock),
            sender,
            &tracker,
            TableInfo::default_out(),
            &stopped,
        );
        assert!(matches!(result, Err(Error::Stopped)));
    }

    #[test]
    fn test_writing_one_batch() {
        let mut db_mock = MockDB::new();
        let batch_size = 10;
        db_mock.expect_write_batch().times(1).returning(|rows, _| {
            assert_eq!(rows.len() as u64, NUM_ROWS);
            Ok(())
        });
        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        for _ in 0..NUM_ROWS {
            sender.send(Row::default()).unwrap();
        }
        drop(sender);
        let stopped = AtomicBool::new(false);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            TABLE_NAME,
            batch_size,
            0,
            &stopped,
        );
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_writing_multiple_batches() {
        let mut db_mock = MockDB::new();
        let batch_size = 1;
        db_mock
            .expect_write_batch()
            .times(NUM_ROWS as usize)
            .returning(|rows, _| {
                assert_eq!(rows.len(), 1);
                Ok(())
            });

        let (sender, receiver) = channel::create_channel(10);
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        for _ in 0..NUM_ROWS {
            sender.send(Row::default()).unwrap();
        }
        drop(sender);
        let stopped = AtomicBool::new(false);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            TABLE_NAME,
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
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        sender.send(Row::default()).unwrap();
        let stopped = AtomicBool::new(true);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            TABLE_NAME,
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
        let tracker = TableMigrationProgress::new(TABLE_NAME, None, true);
        let stopped = AtomicBool::new(true);

        let result = TableMigrator::start_writing(
            Box::new(db_mock),
            receiver,
            &tracker,
            TABLE_NAME,
            10,
            3,
            &stopped,
        );
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_run_success() {
        let mut reader_mock = MockDB::new();
        let mut writer_mock = MockDB::new();

        reader_mock
            .expect_get_table_info()
            .returning(|_, _| Ok(TableInfo::default_in()));

        writer_mock
            .expect_get_table_info()
            .returning(|_, _| Ok(TableInfo::default_out()));

        reader_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            let mut count = 0;
            rows.expect_next().returning(move || {
                if count == NUM_ROWS {
                    return None;
                }
                count += 1;
                Some(Ok(Row::default()))
            });
            Ok(Box::new(rows))
        });

        writer_mock
            .expect_write_batch()
            .times(1)
            .returning(|rows, _| {
                assert_eq!(rows.len() as u64, NUM_ROWS);
                Ok(())
            });

        let settings = TableMigratorSettings::default();
        let migrator = TableMigrator::new(
            Box::new(reader_mock),
            Box::new(writer_mock),
            TABLE_NAME,
            settings,
        )
        .expect("Failed to create TableMigrator");

        let result = migrator.run();
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_reader_error() {
        let mut reader_mock = MockDB::new();
        let mut writer_mock = MockDB::new();

        reader_mock
            .expect_get_table_info()
            .returning(|_, _| Ok(TableInfo::default_in()));

        writer_mock
            .expect_get_table_info()
            .returning(|_, _| Ok(TableInfo::default_out()));

        reader_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            rows.expect_next()
                .returning(move || Some(Err(anyhow::anyhow!("Test error"))));
            Ok(Box::new(rows))
        });

        let settings = TableMigratorSettings::default();
        let migrator = TableMigrator::new(
            Box::new(reader_mock),
            Box::new(writer_mock),
            TABLE_NAME,
            settings,
        )
        .expect("Failed to create TableMigrator");

        let result = migrator.run();
        assert!(result.is_err());
        let error = result.unwrap_err();

        let root_cause = error.root_cause();
        assert_eq!(format!("{}", root_cause), "Test error");
    }

    #[test]
    fn test_run_writer_error() {
        let mut reader_mock = MockDB::new();
        let mut writer_mock = MockDB::new();

        reader_mock
            .expect_get_table_info()
            .returning(|_, _| Ok(TableInfo::default_in()));

        writer_mock
            .expect_get_table_info()
            .returning(|_, _| Ok(TableInfo::default_out()));

        reader_mock.expect_read_iter().returning(|_| {
            let mut rows = MockRowsIter::new();
            let mut count = 0;
            rows.expect_next().returning(move || {
                if count == NUM_ROWS {
                    return None;
                }
                count += 1;
                Some(Ok(Row::default()))
            });
            Ok(Box::new(rows))
        });

        writer_mock
            .expect_write_batch()
            .times(1)
            .returning(|_, _| Err(anyhow::anyhow!("Test error")));

        let settings = TableMigratorSettings::default();
        let migrator = TableMigrator::new(
            Box::new(reader_mock),
            Box::new(writer_mock),
            TABLE_NAME,
            settings,
        )
        .expect("Failed to create TableMigrator");

        let result = migrator.run();
        assert!(result.is_err());
        let error = result.unwrap_err();

        let root_cause = error.root_cause();
        assert_eq!(format!("{}", root_cause), "Test error");
    }
}
