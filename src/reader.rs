use indicatif::ProgressBar;

use crate::channel::Sender;

pub trait DBReader: Send {
    fn get_count(&mut self, table: &str) -> anyhow::Result<u32>;

    fn start_reading(
        &mut self,
        sender: Sender,
        table: &str,
        progress: ProgressBar,
    ) -> anyhow::Result<()>;
}
