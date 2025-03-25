use std::sync::atomic::{AtomicBool, Ordering};

use indicatif::ProgressBar;
use tracing::error;

use crate::channel::Sender;
use crate::error::Error;
use crate::{channel::Reciever, databases::table::Row};

use super::table::Table;

pub trait DBInfoProvider: Send {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<Table>;
}

pub type ReaderIterator<'a> = Box<dyn Iterator<Item = anyhow::Result<Row>> + 'a>;

pub trait DBReader: Send + DBInfoProvider {
    fn read_iter<'a>(&'a mut self, table: &str) -> anyhow::Result<ReaderIterator<'a>>;

    fn start_reading(
        &mut self,
        sender: Sender,
        table: &str,
        progress: ProgressBar,
        stopped: &AtomicBool,
    ) -> Result<(), Error> {
        let iterator = self.read_iter(table)?;
        for result in iterator {
            let row = result?;
            if stopped.load(Ordering::Relaxed) {
                return Err(Error::Stopped);
            }
            sender.send(row).map_err(|_| Error::Stopped)?;
            progress.inc(1);
        }
        progress.finish();
        return Ok(());
    }
}

pub trait DBWriter: Send + DBInfoProvider {
    fn opt_clone(&self) -> anyhow::Result<Box<dyn DBWriter>> {
        return Err(anyhow::anyhow!(
            "This type of databases doesn't support mutiple writers"
        ));
    }

    fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()>;

    fn write_batch_with_retry(
        &mut self,
        batch: &[Row],
        table: &str,
        left_reties: usize,
    ) -> anyhow::Result<()> {
        match self.write_batch(batch, table) {
            Ok(_) => return Ok(()),
            Err(err) => {
                if left_reties - 1 == 0 {
                    return Err(err);
                }
                error!("Got error: {err:?}. Retries left: {left_reties}");
                return self.write_batch_with_retry(batch, table, left_reties - 1);
            }
        }
    }

    fn start_writing(
        &mut self,
        reciever: Reciever,
        table: &str,
        batch_size: usize,
        batch_retries: usize,
        progress: ProgressBar,
        stopped: &AtomicBool,
    ) -> Result<(), Error> {
        let mut batch: Vec<Row> = Vec::with_capacity(batch_size);
        while let Ok(row) = reciever.recv() {
            if stopped.load(Ordering::Relaxed) {
                return Err(Error::Stopped);
            }
            batch.push(row);
            if batch.len() == batch_size {
                self.write_batch_with_retry(&batch, table, batch_retries)?;
                progress.inc(batch.len().try_into().unwrap());
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.write_batch_with_retry(&batch, table, batch_retries)?;
            progress.inc(batch.len().try_into().unwrap());
        }
        progress.finish();
        return Ok(());
    }
}
