use crate::channel::Sender;

pub trait DBReader {
    fn start_reading(&mut self, sender: Sender, table: &str);
}
