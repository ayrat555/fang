#[cfg(test)]
mod async_queue_tests;

use crate::asynk::async_runnable::AsyncRunnable;
use crate::CronError;
use crate::FangTaskState;
use crate::Scheduled::*;
use crate::Task;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::row::Row;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::tokio_postgres::Transaction;
use bb8_postgres::PostgresConnectionManager;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use cron::Schedule;
use postgres_types::ToSql;
use sha2::{Digest, Sha256};
use std::str::FromStr;
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(test)]
use bb8_postgres::tokio_postgres::tls::NoTls;

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
    PoolError(#[from] RunError<bb8_postgres::tokio_postgres::Error>),
    #[error(transparent)]
    PgError(#[from] bb8_postgres::tokio_postgres::Error),
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
    async fn remove_task(&mut self, id: Uuid) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its metadata (struct fields values)
    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError>;

    /// Removes all tasks that have the specified `task_type`.
    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError>;

    /// Retrieve a task from storage by its `id`.
    async fn find_task_by_id(&mut self, id: Uuid) -> Result<Task, AsyncQueueError>;

    /// Update the state field of the specified task
    /// See the `FangTaskState` enum for possible states.
    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError>;

    /// Update the state of a task to `FangTaskState::Failed` and set an error_message.
    async fn fail_task(&mut self, task: Task, error_message: &str)
        -> Result<Task, AsyncQueueError>;

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
pub struct AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[builder(default=None, setter(skip))]
    pool: Option<Pool<PostgresConnectionManager<Tls>>>,
    #[builder(setter(into))]
    uri: String,
    #[builder(setter(into))]
    max_pool_size: u32,
    #[builder(default = false, setter(skip))]
    connected: bool,
}

#[cfg(test)]
#[derive(TypedBuilder)]
pub struct AsyncQueueTest<'a> {
    #[builder(setter(into))]
    pub transaction: Transaction<'a>,
}

#[cfg(test)]
#[async_trait]
impl AsyncQueueable for AsyncQueueTest<'_> {
    async fn find_task_by_id(&mut self, id: Uuid) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::find_task_by_id_query(transaction, id).await
    }

    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::fetch_and_touch_task_query(transaction, task_type).await
    }

    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let metadata = serde_json::to_value(task)?;

        let task: Task = if !task.uniq() {
            AsyncQueue::<NoTls>::insert_task_query(
                transaction,
                metadata,
                &task.task_type(),
                Utc::now(),
            )
            .await?
        } else {
            AsyncQueue::<NoTls>::insert_task_if_not_exist_query(
                transaction,
                metadata,
                &task.task_type(),
                Utc::now(),
            )
            .await?
        };
        Ok(task)
    }

    async fn schedule_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let task: Task = AsyncQueue::<NoTls>::schedule_task_query(transaction, task).await?;

        Ok(task)
    }
    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::remove_all_tasks_query(transaction).await
    }

    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::remove_all_scheduled_tasks_query(transaction).await
    }

    async fn remove_task(&mut self, id: Uuid) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::remove_task_query(transaction, id).await
    }

    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        if task.uniq() {
            let transaction = &mut self.transaction;

            AsyncQueue::<NoTls>::remove_task_by_metadata_query(transaction, task).await
        } else {
            Err(AsyncQueueError::TaskNotUniqError)
        }
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::remove_tasks_type_query(transaction, task_type).await
    }

    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::update_task_state_query(transaction, task, state).await
    }

    async fn fail_task(
        &mut self,
        task: Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::fail_task_query(transaction, task, error_message).await
    }

    async fn schedule_retry(
        &mut self,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        AsyncQueue::<NoTls>::schedule_retry_query(transaction, task, backoff_seconds, error).await
    }
}

#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
static ASYNC_QUEUE_DB_TEST_COUNTER: Mutex<u32> = Mutex::new(0);

#[cfg(test)]
impl AsyncQueue<NoTls> {
    /// Provides an AsyncQueue connected to its own DB
    pub async fn test() -> Self {
        let mut new_number = ASYNC_QUEUE_DB_TEST_COUNTER.lock().unwrap();
        const BASE_URI: &str = "postgres://postgres:postgres@localhost";
        let mut res = Self::builder()
            .max_pool_size(1_u32)
            .uri(format!("{}/fang", BASE_URI))
            .build();

        res.connect(NoTls).await.unwrap();

        let db_name = format!("async_queue_test_{}", *new_number);
        *new_number += 1;

        let check_query = format!("SELECT 0 FROM pg_database WHERE datname='{}';", db_name);
        let create_query = format!("CREATE DATABASE {} WITH TEMPLATE fang;", db_name);
        let delete_query = format!("DROP DATABASE {};", db_name);

        let conn = res.pool.as_mut().unwrap().get().await.unwrap();
        let db_exists = conn.query(&check_query, &[]).await.unwrap().is_empty();
        if db_exists {
            conn.execute(&delete_query, &[]).await.unwrap();
        }
        while let Err(e) = conn.execute(&create_query, &[]).await {
            if e.as_db_error().unwrap().message()
                != "source database \"fang\" is being accessed by other users"
            {
                panic!("{:?}", e);
            }
        }

        drop(conn);

        res.connected = false;
        res.pool = None;
        res.uri = format!("{}/{}", BASE_URI, db_name);
        res.connect(NoTls).await.unwrap();

        res
    }
}

impl<Tls> AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    /// Check if the connection with db is established
    pub fn check_if_connection(&self) -> Result<(), AsyncQueueError> {
        if self.connected {
            Ok(())
        } else {
            Err(AsyncQueueError::NotConnectedError)
        }
    }

    /// Connect to the db if not connected
    pub async fn connect(&mut self, tls: Tls) -> Result<(), AsyncQueueError> {
        let manager = PostgresConnectionManager::new_from_stringlike(self.uri.clone(), tls)?;

        let pool = Pool::builder()
            .max_size(self.max_pool_size)
            .build(manager)
            .await?;

        self.pool = Some(pool);
        self.connected = true;
        Ok(())
    }

    async fn remove_all_tasks_query(
        transaction: &mut Transaction<'_>,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(transaction, REMOVE_ALL_TASK_QUERY, &[], None).await
    }

    async fn remove_all_scheduled_tasks_query(
        transaction: &mut Transaction<'_>,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(
            transaction,
            REMOVE_ALL_SCHEDULED_TASK_QUERY,
            &[&Utc::now()],
            None,
        )
        .await
    }

    async fn remove_task_query(
        transaction: &mut Transaction<'_>,
        id: Uuid,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(transaction, REMOVE_TASK_QUERY, &[&id], Some(1)).await
    }

    async fn remove_task_by_metadata_query(
        transaction: &mut Transaction<'_>,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task)?;

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        Self::execute_query(
            transaction,
            REMOVE_TASK_BY_METADATA_QUERY,
            &[&uniq_hash],
            None,
        )
        .await
    }

    async fn remove_tasks_type_query(
        transaction: &mut Transaction<'_>,
        task_type: &str,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(transaction, REMOVE_TASKS_TYPE_QUERY, &[&task_type], None).await
    }

    async fn find_task_by_id_query(
        transaction: &mut Transaction<'_>,
        id: Uuid,
    ) -> Result<Task, AsyncQueueError> {
        let row: Row = transaction.query_one(FIND_TASK_BY_ID_QUERY, &[&id]).await?;

        let task = Self::row_to_task(row);
        Ok(task)
    }

    async fn fail_task_query(
        transaction: &mut Transaction<'_>,
        task: Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now();

        let row: Row = transaction
            .query_one(
                FAIL_TASK_QUERY,
                &[
                    &FangTaskState::Failed,
                    &error_message,
                    &updated_at,
                    &task.id,
                ],
            )
            .await?;
        let failed_task = Self::row_to_task(row);
        Ok(failed_task)
    }

    async fn schedule_retry_query(
        transaction: &mut Transaction<'_>,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let now = Utc::now();
        let scheduled_at = now + Duration::seconds(backoff_seconds as i64);
        let retries = task.retries + 1;

        let row: Row = transaction
            .query_one(
                RETRY_TASK_QUERY,
                &[&error, &retries, &scheduled_at, &now, &task.id],
            )
            .await?;
        let failed_task = Self::row_to_task(row);
        Ok(failed_task)
    }

    async fn fetch_and_touch_task_query(
        transaction: &mut Transaction<'_>,
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
                Self::update_task_state_query(transaction, some_task, FangTaskState::InProgress)
                    .await?,
            )
        } else {
            None
        };
        Ok(result_task)
    }

    async fn get_task_type_query(
        transaction: &mut Transaction<'_>,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let row: Row = transaction
            .query_one(FETCH_TASK_TYPE_QUERY, &[&task_type, &Utc::now()])
            .await?;

        let task = Self::row_to_task(row);

        Ok(task)
    }

    async fn update_task_state_query(
        transaction: &mut Transaction<'_>,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now();

        let row: Row = transaction
            .query_one(UPDATE_TASK_STATE_QUERY, &[&state, &updated_at, &task.id])
            .await?;
        let task = Self::row_to_task(row);
        Ok(task)
    }

    async fn insert_task_query(
        transaction: &mut Transaction<'_>,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let row: Row = transaction
            .query_one(INSERT_TASK_QUERY, &[&metadata, &task_type, &scheduled_at])
            .await?;
        let task = Self::row_to_task(row);
        Ok(task)
    }

    async fn insert_task_uniq_query(
        transaction: &mut Transaction<'_>,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let uniq_hash = Self::calculate_hash(metadata.to_string());

        let row: Row = transaction
            .query_one(
                INSERT_TASK_UNIQ_QUERY,
                &[&metadata, &task_type, &uniq_hash, &scheduled_at],
            )
            .await?;

        let task = Self::row_to_task(row);
        Ok(task)
    }

    async fn execute_query(
        transaction: &mut Transaction<'_>,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        expected_result_count: Option<u64>,
    ) -> Result<u64, AsyncQueueError> {
        let result = transaction.execute(query, params).await?;

        if let Some(expected_result) = expected_result_count {
            if result != expected_result {
                return Err(AsyncQueueError::ResultError {
                    expected: expected_result,
                    found: result,
                });
            }
        }
        Ok(result)
    }

    async fn insert_task_if_not_exist_query(
        transaction: &mut Transaction<'_>,
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
        transaction: &mut Transaction<'_>,
        metadata: &serde_json::Value,
    ) -> Option<Task> {
        let uniq_hash = Self::calculate_hash(metadata.to_string());

        let result = transaction
            .query_one(FIND_TASK_BY_UNIQ_HASH_QUERY, &[&uniq_hash])
            .await;

        match result {
            Ok(row) => Some(Self::row_to_task(row)),
            Err(_) => None,
        }
    }

    fn row_to_task(row: Row) -> Task {
        let id: Uuid = row.get("id");
        let metadata: serde_json::Value = row.get("metadata");

        let error_message: Option<String> = row.try_get("error_message").ok();

        let uniq_hash: Option<String> = row.try_get("uniq_hash").ok();
        let state: FangTaskState = row.get("state");
        let task_type: String = row.get("task_type");
        let retries: i32 = row.get("retries");
        let created_at: DateTime<Utc> = row.get("created_at");
        let updated_at: DateTime<Utc> = row.get("updated_at");
        let scheduled_at: DateTime<Utc> = row.get("scheduled_at");

        Task::builder()
            .id(id)
            .metadata(metadata)
            .error_message(error_message)
            .state(state)
            .uniq_hash(uniq_hash)
            .task_type(task_type)
            .retries(retries)
            .created_at(created_at)
            .updated_at(updated_at)
            .scheduled_at(scheduled_at)
            .build()
    }

    async fn schedule_task_query(
        transaction: &mut Transaction<'_>,
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
impl<Tls> AsyncQueueable for AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn find_task_by_id(&mut self, id: Uuid) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::find_task_by_id_query(&mut transaction, id).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::fetch_and_touch_task_query(&mut transaction, task_type).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let metadata = serde_json::to_value(task)?;

        let task: Task = if !task.uniq() {
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
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::schedule_task_query(&mut transaction, task).await?;

        transaction.commit().await?;
        Ok(task)
    }

    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let result = Self::remove_all_tasks_query(&mut transaction).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let result = Self::remove_all_scheduled_tasks_query(&mut transaction).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_task(&mut self, id: Uuid) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

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
            let mut connection = self.pool.as_ref().unwrap().get().await?;
            let mut transaction = connection.transaction().await?;

            let result = Self::remove_task_by_metadata_query(&mut transaction, task).await?;

            transaction.commit().await?;

            Ok(result)
        } else {
            Err(AsyncQueueError::TaskNotUniqError)
        }
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let result = Self::remove_tasks_type_query(&mut transaction, task_type).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::update_task_state_query(&mut transaction, task, state).await?;
        transaction.commit().await?;

        Ok(task)
    }

    async fn fail_task(
        &mut self,
        task: Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

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
        let mut connection = self.pool.as_ref().unwrap().get().await?;
        let mut transaction = connection.transaction().await?;

        let task =
            Self::schedule_retry_query(&mut transaction, task, backoff_seconds, error).await?;
        transaction.commit().await?;

        Ok(task)
    }
}

#[cfg(test)]
test_asynk_queue! {postgres, crate::AsyncQueue<bb8_postgres::tokio_postgres::tls::NoTls>, crate::AsyncQueue::<bb8_postgres::tokio_postgres::tls::NoTls>::test()}
