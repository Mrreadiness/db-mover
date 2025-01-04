use crate::channel::Sender;

pub trait DBReader {
    fn start_reading(&self, sender: Sender, table: &str);
}
