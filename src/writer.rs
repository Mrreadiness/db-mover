use crate::channel::Reciever;

pub trait DBWriter {
    fn start_writing(&self, reciever: Reciever, table: &str);
}
