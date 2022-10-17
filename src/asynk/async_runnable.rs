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

/// Implement this trait to use your custom tasks.
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait AsyncRunnable: Send + Sync {
    /// Execute the task. It should define its logic
    async fn run(&self, client: &mut dyn AsyncQueueable) -> Result<(), FangError>;

    /// Define the type of the task.
    /// By default `COMMON_TYPE` constant is the task type
    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }

    /// If set to true, library will handle the uniqueness of the task that implements this trait.
    /// By default is set to false.
    fn uniq(&self) -> bool {
        false
    }

    /// This will define if a task is periodic or is scheduled once in the time.
    /// You can see [how cron format works](https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm)
    ///
    /// Be careful works only with UTC timezone.
    ///
    /// [Web to check timezones](https://www.timeanddate.com/worldclock/timezone/utc)
    ///
    /// Example:
    ///
    ///
    /**
    ```rust
     fn cron(&self) -> Option<Scheduled> {
         let expression = "0/20 * * * Aug-Sep * 2022/1";
         Some(Scheduled::CronPattern(expression.to_string()))
     }
    ```
    */

    /// In order to schedule  a task once, use the `Scheduled::ScheduleOnce` enum variant.
    fn cron(&self) -> Option<Scheduled> {
        None
    }

    /// Define the maximum number of retries the task will be retried.
    /// By default the number of retries is 20.
    fn max_retries(&self) -> i32 {
        RETRIES_NUMBER
    }

    /// Define the backoff mode
    /// By default is exponential,  2^(attempt)
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
