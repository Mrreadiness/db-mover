use tracing::error;

use super::table::{Row, TableInfo};

pub trait DBInfoProvider: Send {
    fn get_table_info(&mut self, table: &str, no_count: bool) -> anyhow::Result<TableInfo>;
}

pub type ReaderIterator<'a> = Box<dyn Iterator<Item = anyhow::Result<Row>> + 'a>;

pub trait DBReader: Send + DBInfoProvider {
    fn read_iter(&mut self, target_format: TableInfo) -> anyhow::Result<ReaderIterator<'_>>;
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
        if let Err(err) = self.write_batch(batch, table) {
            if left_reties == 0 {
                return Err(err);
            }
            error!("Got error: {err:?}. Retries left: {left_reties}");
            return self.write_batch_with_retry(batch, table, left_reties - 1);
        }
        return Ok(());
    }
}
