use crate::row::Row;

pub type Reciever = async_channel::Receiver<Row>;
pub type Sender = async_channel::Sender<Row>;

pub fn create_channel(size: Option<usize>) -> (Sender, Reciever) {
    match size {
        None => return async_channel::unbounded(),
        Some(size) => return async_channel::bounded(size),
    }
}
