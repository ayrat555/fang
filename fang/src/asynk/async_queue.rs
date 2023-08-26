#[cfg(test)]
mod async_queue_tests;

use crate::asynk::async_runnable::AsyncRunnable;
use crate::CronError;
use crate::FangTaskState;
use crate::Scheduled::*;
use crate::Task;
use async_trait::async_trait;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use cron::Schedule;
use sha2::{Digest, Sha256};
use sqlx::any::install_default_drivers;
use sqlx::pool::PoolOptions;
use sqlx::types::Uuid;
use sqlx::Acquire;
use sqlx::Any;
use sqlx::AnyPool;
use sqlx::Transaction;
use std::str::FromStr;
use thiserror::Error;
use typed_builder::TypedBuilder;

#[cfg(test)]
use self::async_queue_tests::test_asynk_queue;

const INSERT_TASK_QUERY: &str = include_str!("queries/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY: &str = include_str!("queries/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY: &str = include_str!("queries/update_task_state.sql");
const FAIL_TASK_QUERY: &str = include_str!("queries/fail_task.sql");
const REMOVE_ALL_TASK_QUERY: &str = include_str!("queries/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY: &str =
    include_str!("queries/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY: &str = include_str!("queries/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY: &str = include_str!("queries/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY: &str = include_str!("queries/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY: &str = include_str!("queries/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY: &str = include_str!("queries/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY: &str = include_str!("queries/find_task_by_id.sql");
const RETRY_TASK_QUERY: &str = include_str!("queries/retry_task.sql");

pub const DEFAULT_TASK_TYPE: &str = "common";

#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    SqlXError(#[from] sqlx::Error),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    CronError(#[from] CronError),
    #[error("returned invalid result (expected {expected:?}, found {found:?})")]
    ResultError { expected: u64, found: u64 },
    #[error(
        "AsyncQueue is not connected :( , call connect() method first and then perform operations"
    )]
    NotConnectedError,
    #[error("Can not convert `std::time::Duration` to `chrono::Duration`")]
    TimeError,
    #[error("Can not perform this operation if task is not uniq, please check its definition in impl AsyncRunnable")]
    TaskNotUniqError,
}

impl From<cron::error::Error> for AsyncQueueError {
    fn from(error: cron::error::Error) -> Self {
        AsyncQueueError::CronError(CronError::LibraryError(error))
    }
}

/// This trait defines operations for an asynchronous queue.
/// The trait can be implemented for different storage backends.
/// For now, the trait is only implemented for PostgreSQL. More backends are planned to be implemented in the future.

#[async_trait]
pub trait AsyncQueueable: Send {
    /// This method should retrieve one task of the `task_type` type. If `task_type` is `None` it will try to
    /// fetch a task of the type `common`. After fetching it should update the state of the task to
    /// `FangTaskState::InProgress`.
    ///
    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError>;

    /// Enqueue a task to the queue, The task will be executed as soon as possible by the worker of the same type
    /// created by an AsyncWorkerPool.
    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError>;

    /// The method will remove all tasks from the queue
    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    /// Remove all tasks that are scheduled in the future.
    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its id.
    async fn remove_task(&mut self, id: &[u8]) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its metadata (struct fields values)
    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError>;

    /// Removes all tasks that have the specified `task_type`.
    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError>;

    /// Retrieve a task from storage by its `id`.
    async fn find_task_by_id(&mut self, id: &[u8]) -> Result<Task, AsyncQueueError>;

    /// Update the state field of the specified task
    /// See the `FangTaskState` enum for possible states.
    async fn update_task_state(
        &mut self,
        task: &Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError>;

    /// Update the state of a task to `FangTaskState::Failed` and set an error_message.
    async fn fail_task(
        &mut self,
        task: &Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError>;

    /// Schedule a task.
    async fn schedule_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError>;

    async fn schedule_retry(
        &mut self,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError>;
}

/// An async queue that can be used to enqueue tasks.
/// It uses a PostgreSQL storage. It must be connected to perform any operation.
/// To connect an `AsyncQueue` to PostgreSQL database call the `connect` method.
/// A Queue can be created with the TypedBuilder.
///
///    ```rust
///         let mut queue = AsyncQueue::builder()
///             .uri("postgres://postgres:postgres@localhost/fang")
///             .max_pool_size(max_pool_size)
///             .build();
///     ```
///

#[derive(TypedBuilder, Debug, Clone)]
pub struct AsyncQueue {
    #[builder(default=None, setter(skip))]
    pool: Option<AnyPool>,
    #[builder(setter(into))]
    uri: String,
    #[builder(setter(into))]
    max_pool_size: u32,
    #[builder(default = false, setter(skip))]
    connected: bool,
}

#[cfg(test)]
use tokio::sync::Mutex;

#[cfg(test)]
static ASYNC_QUEUE_DB_TEST_COUNTER: Mutex<u32> = Mutex::const_new(0);

#[cfg(test)]
use sqlx::Executor;

#[cfg(test)]
impl AsyncQueue {
    /// Provides an AsyncQueue connected to its own DB
    pub async fn test() -> Self {
        const BASE_URI: &str = "postgres://postgres:postgres@localhost";
        let mut res = Self::builder()
            .max_pool_size(1_u32)
            .uri(format!("{}/fang", BASE_URI))
            .build();

        let mut new_number = ASYNC_QUEUE_DB_TEST_COUNTER.lock().await;
        res.connect().await.unwrap();

        let db_name = format!("async_queue_test_{}", *new_number);
        *new_number += 1;

        let create_query: &str = &format!("CREATE DATABASE {} WITH TEMPLATE fang;", db_name);
        let delete_query: &str = &format!("DROP DATABASE IF EXISTS {};", db_name);

        let mut conn = res.pool.as_mut().unwrap().acquire().await.unwrap();

        log::info!("Deleting database {db_name} ...");
        conn.execute(delete_query).await.unwrap();

        log::info!("Creating database {db_name} ...");
        while let Err(e) = conn.execute(create_query).await {
            if e.as_database_error().unwrap().message()
                != "source database \"fang\" is being accessed by other users"
            {
                panic!("{:?}", e);
            }
        }

        log::info!("Database {db_name} created !!");

        drop(conn);

        res.connected = false;
        res.pool = None;
        res.uri = format!("{}/{}", BASE_URI, db_name);
        res.connect().await.unwrap();

        res
    }
}

impl AsyncQueue {
    /// Check if the connection with db is established
    pub fn check_if_connection(&self) -> Result<(), AsyncQueueError> {
        if self.connected {
            Ok(())
        } else {
            Err(AsyncQueueError::NotConnectedError)
        }
    }

    /// Connect to the db if not connected
    pub async fn connect(&mut self) -> Result<(), AsyncQueueError> {
        install_default_drivers();

        let pool: AnyPool = PoolOptions::new()
            .max_connections(self.max_pool_size)
            .connect(&self.uri)
            .await?;

        self.pool = Some(pool);
        self.connected = true;
        Ok(())
    }

    async fn remove_all_tasks_query(
        transaction: &mut Transaction<'_, Any>,
    ) -> Result<u64, AsyncQueueError> {
        Ok(sqlx::query(REMOVE_ALL_TASK_QUERY)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn remove_all_scheduled_tasks_query(
        transaction: &mut Transaction<'_, Any>,
    ) -> Result<u64, AsyncQueueError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        Ok(sqlx::query(REMOVE_ALL_SCHEDULED_TASK_QUERY)
            .bind(now_str)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn remove_task_query(
        transaction: &mut Transaction<'_, Any>,
        id: &[u8],
    ) -> Result<u64, AsyncQueueError> {
        let result = sqlx::query(REMOVE_TASK_QUERY)
            .bind(id)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected();

        if result != 1 {
            Err(AsyncQueueError::ResultError {
                expected: 1,
                found: result,
            })
        } else {
            Ok(result)
        }
    }

    async fn remove_task_by_metadata_query(
        transaction: &mut Transaction<'_, Any>,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task)?;

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        Ok(sqlx::query(REMOVE_TASK_BY_METADATA_QUERY)
            .bind(uniq_hash)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn remove_tasks_type_query(
        transaction: &mut Transaction<'_, Any>,
        task_type: &str,
    ) -> Result<u64, AsyncQueueError> {
        Ok(sqlx::query(REMOVE_TASKS_TYPE_QUERY)
            .bind(task_type)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn find_task_by_id_query(
        transaction: &mut Transaction<'_, Any>,
        id: &[u8],
    ) -> Result<Task, AsyncQueueError> {
        let task: Task = sqlx::query_as(FIND_TASK_BY_ID_QUERY)
            .bind(id)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(task)
    }

    async fn fail_task_query(
        transaction: &mut Transaction<'_, Any>,
        task: &Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now().to_rfc3339();

        let failed_task: Task = sqlx::query_as(FAIL_TASK_QUERY)
            .bind(<&str>::from(FangTaskState::Failed))
            .bind(error_message)
            .bind(updated_at)
            .bind(&task.id)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(failed_task)
    }

    async fn schedule_retry_query(
        transaction: &mut Transaction<'_, Any>,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let scheduled_at = now + Duration::seconds(backoff_seconds as i64);
        let scheduled_at_str = scheduled_at.to_rfc3339();
        let retries = task.retries + 1;

        let failed_task: Task = sqlx::query_as(RETRY_TASK_QUERY)
            .bind(error)
            .bind(retries)
            .bind(scheduled_at_str)
            .bind(now_str)
            .bind(&task.id)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(failed_task)
    }

    async fn fetch_and_touch_task_query(
        transaction: &mut Transaction<'_, Any>,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let task_type = match task_type {
            Some(passed_task_type) => passed_task_type,
            None => DEFAULT_TASK_TYPE.to_string(),
        };

        let task = match Self::get_task_type_query(transaction, &task_type).await {
            Ok(some_task) => Some(some_task),
            Err(_) => None,
        };
        let result_task = if let Some(some_task) = task {
            Some(
                Self::update_task_state_query(transaction, &some_task, FangTaskState::InProgress)
                    .await?,
            )
        } else {
            None
        };
        Ok(result_task)
    }

    async fn get_task_type_query(
        transaction: &mut Transaction<'_, Any>,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        let task: Task = sqlx::query_as(FETCH_TASK_TYPE_QUERY)
            .bind(task_type)
            .bind(now_str)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(task)
    }

    async fn update_task_state_query(
        transaction: &mut Transaction<'_, Any>,
        task: &Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now();
        let updated_at_str = updated_at.to_rfc3339();

        let state_str: &str = state.into();

        let task: Task = sqlx::query_as(UPDATE_TASK_STATE_QUERY)
            .bind(state_str)
            .bind(updated_at_str)
            .bind(&task.id)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(task)
    }

    async fn insert_task_query(
        transaction: &mut Transaction<'_, Any>,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let uuid = Uuid::new_v4();
        let bytes: &[u8] = &uuid.to_bytes_le();

        let metadata_str = metadata.to_string();
        let scheduled_at_str = scheduled_at.to_rfc3339();

        let task: Task = sqlx::query_as(INSERT_TASK_QUERY)
            .bind(bytes)
            .bind(metadata_str)
            .bind(task_type)
            .bind(scheduled_at_str)
            .fetch_one(transaction.acquire().await?)
            .await?;
        Ok(task)
    }

    async fn insert_task_uniq_query(
        transaction: &mut Transaction<'_, Any>,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let uuid = Uuid::new_v4();
        let bytes: &[u8] = &uuid.to_bytes_le();

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        let metadata_str = metadata.to_string();
        let scheduled_at_str = scheduled_at.to_rfc3339();

        let task: Task = sqlx::query_as(INSERT_TASK_UNIQ_QUERY)
            .bind(bytes)
            .bind(metadata_str)
            .bind(task_type)
            .bind(uniq_hash)
            .bind(scheduled_at_str)
            .fetch_one(transaction.acquire().await?)
            .await?;
        Ok(task)
    }

    async fn insert_task_if_not_exist_query(
        transaction: &mut Transaction<'_, Any>,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        match Self::find_task_by_uniq_hash_query(transaction, &metadata).await {
            Some(task) => Ok(task),
            None => {
                Self::insert_task_uniq_query(transaction, metadata, task_type, scheduled_at).await
            }
        }
    }

    fn calculate_hash(json: String) -> String {
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    async fn find_task_by_uniq_hash_query(
        transaction: &mut Transaction<'_, Any>,
        metadata: &serde_json::Value,
    ) -> Option<Task> {
        let uniq_hash = Self::calculate_hash(metadata.to_string());

        sqlx::query_as(FIND_TASK_BY_UNIQ_HASH_QUERY)
            .bind(uniq_hash)
            .fetch_one(transaction.acquire().await.ok()?)
            .await
            .ok()
    }

    async fn schedule_task_query(
        transaction: &mut Transaction<'_, Any>,
        task: &dyn AsyncRunnable,
    ) -> Result<Task, AsyncQueueError> {
        let metadata = serde_json::to_value(task)?;

        let scheduled_at = match task.cron() {
            Some(scheduled) => match scheduled {
                CronPattern(cron_pattern) => {
                    let schedule = Schedule::from_str(&cron_pattern)?;
                    let mut iterator = schedule.upcoming(Utc);
                    iterator
                        .next()
                        .ok_or(AsyncQueueError::CronError(CronError::NoTimestampsError))?
                }
                ScheduleOnce(datetime) => datetime,
            },
            None => {
                return Err(AsyncQueueError::CronError(
                    CronError::TaskNotSchedulableError,
                ));
            }
        };

        let task: Task = if !task.uniq() {
            Self::insert_task_query(transaction, metadata, &task.task_type(), scheduled_at).await?
        } else {
            Self::insert_task_if_not_exist_query(
                transaction,
                metadata,
                &task.task_type(),
                scheduled_at,
            )
            .await?
        };
        Ok(task)
    }
}

#[async_trait]
impl AsyncQueueable for AsyncQueue {
    async fn find_task_by_id(&mut self, id: &[u8]) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task = Self::find_task_by_id_query(&mut transaction, id).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task = Self::fetch_and_touch_task_query(&mut transaction, task_type).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;
        let metadata = serde_json::to_value(task)?;

        let task = if !task.uniq() {
            Self::insert_task_query(&mut transaction, metadata, &task.task_type(), Utc::now())
                .await?
        } else {
            Self::insert_task_if_not_exist_query(
                &mut transaction,
                metadata,
                &task.task_type(),
                Utc::now(),
            )
            .await?
        };

        transaction.commit().await?;

        Ok(task)
    }

    async fn schedule_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task = Self::schedule_task_query(&mut transaction, task).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result = Self::remove_all_tasks_query(&mut transaction).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result = Self::remove_all_scheduled_tasks_query(&mut transaction).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_task(&mut self, id: &[u8]) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result = Self::remove_task_query(&mut transaction, id).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        if task.uniq() {
            self.check_if_connection()?;
            let mut transaction = self.pool.as_ref().unwrap().begin().await?;

            let result = Self::remove_task_by_metadata_query(&mut transaction, task).await?;

            transaction.commit().await?;

            Ok(result)
        } else {
            Err(AsyncQueueError::TaskNotUniqError)
        }
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result = Self::remove_tasks_type_query(&mut transaction, task_type).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn update_task_state(
        &mut self,
        task: &Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task = Self::update_task_state_query(&mut transaction, task, state).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn fail_task(
        &mut self,
        task: &Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task = Self::fail_task_query(&mut transaction, task, error_message).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn schedule_retry(
        &mut self,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;

        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let failed_task =
            Self::schedule_retry_query(&mut transaction, task, backoff_seconds, error).await?;

        transaction.commit().await?;

        Ok(failed_task)
    }
}

#[cfg(test)]
test_asynk_queue! {postgres, crate::AsyncQueue, crate::AsyncQueue::test()}
