use crate::databases::table::Row;

pub type Reciever = crossbeam::channel::Receiver<Row>;
pub type Sender = crossbeam::channel::Sender<Row>;

pub fn create_channel(size: usize) -> (Sender, Reciever) {
    return crossbeam::channel::bounded(size);
}
