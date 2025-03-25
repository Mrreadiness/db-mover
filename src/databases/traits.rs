use tracing::error;

use super::table::{Row, Table};

pub trait DBInfoProvider: Send {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<Table>;
}

pub type ReaderIterator<'a> = Box<dyn Iterator<Item = anyhow::Result<Row>> + 'a>;

pub trait DBReader: Send + DBInfoProvider {
    fn read_iter<'a>(&'a mut self, table: &str) -> anyhow::Result<ReaderIterator<'a>>;
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
                if left_reties == 0 {
                    return Err(err);
                }
                error!("Got error: {err:?}. Retries left: {left_reties}");
                return self.write_batch_with_retry(batch, table, left_reties - 1);
            }
        }
    }
}
