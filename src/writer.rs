use indicatif::ProgressBar;

use crate::channel::Reciever;

pub trait DBWriter: Send {
    fn start_writing(
        &mut self,
        reciever: Reciever,
        table: &str,
        progress: ProgressBar,
    ) -> anyhow::Result<()>;
}
