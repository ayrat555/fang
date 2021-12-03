use std::io::Error as IoError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FangError {
    #[error("The shared state in an executor thread became poisoned")]
    SharedStatePoisoned,

    #[error("Failed to create executor thread")]
    ExecutorThreadCreationFailed {
        #[from]
        source: IoError,
    },
}
