use indicatif::ProgressBar;

use crate::{channel::Reciever, progress::log_progress_bar_if_no_term, row::Row};

pub trait DBWriter: Send {
    fn write_batch(&mut self, batch: &[Row], table: &str) -> anyhow::Result<()>;

    fn start_writing(
        &mut self,
        reciever: Reciever,
        table: &str,
        progress: ProgressBar,
    ) -> anyhow::Result<()> {
        let batch_size = 100_000;
        let mut batch: Vec<Row> = Vec::with_capacity(batch_size);
        while let Ok(row) = reciever.recv() {
            batch.push(row);
            if batch.len() == batch_size {
                self.write_batch(&batch, table)?;
                progress.inc(batch.len().try_into()?);
                log_progress_bar_if_no_term(&progress);
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.write_batch(&batch, table)?;
            progress.inc(batch.len().try_into()?);
            log_progress_bar_if_no_term(&progress);
        }
        progress.finish();
        return Ok(());
    }
}
