use crate::blocking::queue::QueueError;
use crate::FangError;
use diesel::r2d2::PoolError;
use diesel::result::Error as DieselError;
use std::io::Error as IoError;

impl From<IoError> for FangError {
    fn from(error: IoError) -> Self {
        let description = format!("{error:?}");
        FangError { description }
    }
}

impl From<QueueError> for FangError {
    fn from(error: QueueError) -> Self {
        let description = format!("{error:?}");
        FangError { description }
    }
}

impl From<DieselError> for FangError {
    fn from(error: DieselError) -> Self {
        Self::from(QueueError::DieselError(error))
    }
}

impl From<PoolError> for FangError {
    fn from(error: PoolError) -> Self {
        Self::from(QueueError::PoolError(error))
    }
}
