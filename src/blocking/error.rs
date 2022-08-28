use crate::blocking::queue::QueueError;
use std::io::Error as IoError;
use std::sync::PoisonError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FangError {
    #[error("The shared state in an executor thread became poisoned")]
    PoisonedLock,
    #[error(transparent)]
    QueueError(#[from] QueueError),
    #[error("Failed to create executor thread")]
    ExecutorThreadCreationFailed {
        #[from]
        source: IoError,
    },
}

impl<T> From<PoisonError<T>> for FangError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLock
    }
}
