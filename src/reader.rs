use crate::channel::Sender;

pub trait DBReader: Send {
    fn start_reading(&mut self, sender: Sender, table: &str) -> anyhow::Result<()>;
}
