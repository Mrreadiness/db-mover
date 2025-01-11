use crate::row::Row;

pub type Reciever = crossbeam::channel::Receiver<Row>;
pub type Sender = crossbeam::channel::Sender<Row>;

pub fn create_channel(size: Option<usize>) -> (Sender, Reciever) {
    match size {
        None => return crossbeam::channel::unbounded(),
        Some(size) => return crossbeam::channel::bounded(size),
    }
}
