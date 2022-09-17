use crate::async_queue::AsyncQueueError;
use crate::asynk::async_queue::AsyncQueueable;
use crate::FangError;
use crate::Scheduled;
use async_trait::async_trait;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::Error as TokioPostgresError;
use serde_json::Error as SerdeError;

const COMMON_TYPE: &str = "common";
pub const RETRIES_NUMBER: i32 = 20;

impl From<AsyncQueueError> for FangError {
    fn from(error: AsyncQueueError) -> Self {
        let message = format!("{:?}", error);
        FangError {
            description: message,
        }
    }
}

impl From<TokioPostgresError> for FangError {
    fn from(error: TokioPostgresError) -> Self {
        Self::from(AsyncQueueError::PgError(error))
    }
}

impl From<RunError<TokioPostgresError>> for FangError {
    fn from(error: RunError<TokioPostgresError>) -> Self {
        Self::from(AsyncQueueError::PoolError(error))
    }
}

impl From<SerdeError> for FangError {
    fn from(error: SerdeError) -> Self {
        Self::from(AsyncQueueError::SerdeError(error))
    }
}

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait AsyncRunnable: Send + Sync {
    async fn run(&self, client: &mut dyn AsyncQueueable) -> Result<(), FangError>;

    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }

    fn uniq(&self) -> bool {
        false
    }

    fn cron(&self) -> Option<Scheduled> {
        None
    }

    fn max_retries(&self) -> i32 {
        RETRIES_NUMBER
    }

    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
