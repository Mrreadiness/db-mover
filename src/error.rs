use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Stopeed because of the error in an another thread")]
    Stopped,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
