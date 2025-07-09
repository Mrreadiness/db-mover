use std::thread::sleep;
use thiserror::Error;
use tracing::error;

use crate::retry::ExponentialRetry;

use super::table::{Row, TableInfo};

pub trait DBInfoProvider: Send {
    fn get_tables(&mut self) -> anyhow::Result<Vec<String>>;

    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo>;
}

pub type ReaderIterator<'a> = Box<dyn Iterator<Item = anyhow::Result<Row>> + 'a>;

pub trait DBReader: Send + DBInfoProvider {
    fn read_iter(&mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'_>>;
}

#[derive(Error, Debug)]
pub enum WriterError {
    #[error(transparent)]
    Unrecoverable(anyhow::Error),
    #[error(transparent)]
    Recoverable(#[from] anyhow::Error),
}

impl From<std::io::Error> for WriterError {
    fn from(err: std::io::Error) -> Self {
        WriterError::Recoverable(err.into())
    }
}

pub trait DBWriter: Send + DBInfoProvider {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        return Err(anyhow::anyhow!(
            "This type of databases doesn't support mutiple writers"
        ));
    }

    fn write_batch(&mut self, batch: &[Row], table: &TableInfo) -> Result<(), WriterError>;

    fn write_batch_with_retry(
        &mut self,
        batch: &[Row],
        table: &TableInfo,
        mut retry: ExponentialRetry,
    ) -> anyhow::Result<()> {
        return match self.write_batch(batch, table) {
            Err(WriterError::Recoverable(err)) => match retry.next() {
                Some(duration) => {
                    error!("Got error: {err:?}. Retry after: {duration:?}");
                    sleep(duration);
                    self.try_recover(&mut retry)?;
                    return self.write_batch_with_retry(batch, table, retry);
                }
                None => Err(err),
            },
            Err(WriterError::Unrecoverable(err)) => Err(err),
            Ok(()) => Ok(()),
        };
    }

    fn try_recover(&mut self, retry: &mut ExponentialRetry) -> anyhow::Result<()> {
        return match self.recover() {
            Ok(_) => Ok(()),
            Err(err) => match retry.next() {
                Some(duration) => {
                    error!("Got error while recovering: {err:?}. Retry after: {duration:?}");
                    sleep(duration);
                    return self.try_recover(retry);
                }
                None => Err(err),
            },
        };
    }

    // Recover actions in case of Recoverable error
    fn recover(&mut self) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::databases::table::TableInfo;
    use crate::databases::traits::WriterError;

    use super::*;
    use mockall::{mock, predicate::*};

    mock! {
        DB {}

        impl DBInfoProvider for DB {
            fn get_tables(&mut self) -> anyhow::Result<Vec<String>>;

            fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo>;
        }
        impl DBWriter for DB {
            fn write_batch(&mut self, batch: &[Row], table: &TableInfo) -> Result<(), WriterError>;
            fn recover(&mut self) -> anyhow::Result<()>;
        }
    }

    fn make_table_info() -> TableInfo {
        return TableInfo {
            name: "test".to_string(),
            columns: Vec::new(),
            num_rows: None,
        };
    }

    #[test]
    fn test_writer_recoverable_error() {
        let mut writer = MockDB::new();

        let expected_retries = 3;

        writer
            .expect_write_batch()
            .times(expected_retries + 1) // Plus original call
            .returning(|_, _| Err(WriterError::Recoverable(anyhow::anyhow!("Test error"))));

        writer
            .expect_recover()
            .times(expected_retries)
            .returning(|| Ok(()));
        let table_info = make_table_info();

        let result = writer.write_batch_with_retry(
            &[],
            &table_info,
            ExponentialRetry::with_base_duration(expected_retries, Duration::from_millis(1)),
        );
        assert!(result.is_err());
        let error = result.unwrap_err();

        let root_cause = error.root_cause();
        assert_eq!(format!("{}", root_cause), "Test error");
    }

    #[test]
    fn test_writer_unrecoverable_error() {
        let mut writer = MockDB::new();

        writer
            .expect_write_batch()
            .times(1)
            .returning(|_, _| Err(WriterError::Unrecoverable(anyhow::anyhow!("Test error"))));

        writer.expect_recover().times(0).returning(|| Ok(()));
        let table_info = make_table_info();

        let result = writer.write_batch_with_retry(
            &[],
            &table_info,
            ExponentialRetry::with_base_duration(3, Duration::from_millis(1)),
        );
        assert!(result.is_err());
        let error = result.unwrap_err();

        let root_cause = error.root_cause();
        assert_eq!(format!("{}", root_cause), "Test error");
    }

    #[test]
    fn test_writer_try_recover_failed() {
        let mut writer = MockDB::new();

        let expected_retries = 3;

        writer
            .expect_write_batch()
            .times(1)
            .returning(|_, _| Err(WriterError::Recoverable(anyhow::anyhow!("Test error"))));

        writer
            .expect_recover()
            .times(expected_retries)
            .returning(|| Err(anyhow::anyhow!("Test recover error")));
        let table_info = make_table_info();

        let result = writer.write_batch_with_retry(
            &[],
            &table_info,
            ExponentialRetry::with_base_duration(expected_retries, Duration::from_millis(1)),
        );
        assert!(result.is_err());
        let error = result.unwrap_err();

        let root_cause = error.root_cause();
        assert_eq!(format!("{}", root_cause), "Test recover error");
    }
}
