use crate::channel::Reciever;

pub trait DBWriter: Send {
    fn start_writing(&self, reciever: Reciever, table: &str) -> anyhow::Result<()>;
}
