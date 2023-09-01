#[cfg(test)]
mod async_queue_tests;

use crate::asynk::async_runnable::AsyncRunnable;
use crate::backend_sqlx::SqlXQuery;
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
use sqlx::Acquire;
use sqlx::Any;
use sqlx::AnyPool;
use sqlx::Transaction;
use std::str::FromStr;
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(test)]
use self::async_queue_tests::test_asynk_queue;

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
    #[error("AsyncQueue generic does not correspond to uri BackendSqlX")]
    ConnectionError,
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
/// The trait can be implemented for different storage BackendSqlXs.
/// For now, the trait is only implemented for PostgreSQL. More BackendSqlXs are planned to be implemented in the future.

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
    async fn remove_task(&mut self, id: &Uuid) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its metadata (struct fields values)
    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError>;

    /// Removes all tasks that have the specified `task_type`.
    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError>;

    /// Retrieve a task from storage by its `id`.
    async fn find_task_by_id(&mut self, id: &Uuid) -> Result<Task, AsyncQueueError>;

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
    #[builder(default = BackendSqlX::NoBackend, setter(skip))]
    backend: BackendSqlX,
}

#[cfg(test)]
use tokio::sync::Mutex;

#[cfg(test)]
static ASYNC_QUEUE_POSTGRES_TEST_COUNTER: Mutex<u32> = Mutex::const_new(0);

#[cfg(test)]
static ASYNC_QUEUE_SQLITE_TEST_COUNTER: Mutex<u32> = Mutex::const_new(0);

#[cfg(test)]
use sqlx::Executor;

#[cfg(test)]
use std::path::Path;

#[cfg(test)]
use std::env;

use super::backend_sqlx::BackendSqlX;

#[cfg(test)]
impl AsyncQueue {
    /// Provides an AsyncQueue connected to its own DB
    pub async fn test_postgres() -> Self {
        dotenvy::dotenv().expect(".env file not found");
        let base_url = env::var("POSTGRES_BASE_URL").expect("Base URL for Postgres not found");
        let base_db = env::var("POSTGRES_DB").expect("Name for base Postgres DB not found");

        let mut res = Self::builder()
            .max_pool_size(1_u32)
            .uri(format!("{}/{}", base_url, base_db))
            .build();

        let mut new_number = ASYNC_QUEUE_POSTGRES_TEST_COUNTER.lock().await;
        res.connect().await.unwrap();

        let db_name = format!("async_queue_test_{}", *new_number);
        *new_number += 1;

        let create_query: &str = &format!("CREATE DATABASE {} WITH TEMPLATE fang;", db_name);
        let delete_query: &str = &format!("DROP DATABASE IF EXISTS {};", db_name);

        let mut conn = res.pool.as_mut().unwrap().acquire().await.unwrap();

        log::info!("Deleting database {db_name} ...");
        conn.execute(delete_query).await.unwrap();

        log::info!("Creating database {db_name} ...");
        let expected_error: &str = &format!(
            "source database \"{}\" is being accessed by other users",
            base_db
        );
        while let Err(e) = conn.execute(create_query).await {
            if e.as_database_error().unwrap().message() != expected_error {
                panic!("{:?}", e);
            }
        }

        log::info!("Database {db_name} created !!");

        res.connected = false;
        res.pool = None;
        res.uri = format!("{}/{}", base_url, db_name);
        res.connect().await.unwrap();

        res
    }

    /// Provides an AsyncQueue connected to its own DB
    pub async fn test_sqlite() -> Self {
        dotenvy::dotenv().expect(".env file not found");
        let tests_dir = env::var("SQLITE_TESTS_DIR").expect("Name for tests directory not found");
        let base_file = env::var("SQLITE_FILE").expect("Name for SQLite DB file not found");
        let sqlite_file = format!("../{}", base_file);

        let mut new_number = ASYNC_QUEUE_SQLITE_TEST_COUNTER.lock().await;

        let db_name = format!("../{}/async_queue_test_{}.db", tests_dir, *new_number);
        *new_number += 1;

        let path = Path::new(&db_name);

        if path.exists() {
            log::info!("Deleting database {db_name} ...");
            std::fs::remove_file(path).unwrap();
        }

        log::info!("Creating database {db_name} ...");
        std::fs::copy(sqlite_file, &db_name).unwrap();
        log::info!("Database {db_name} created !!");

        let mut res = Self::builder()
            .max_pool_size(1_u32)
            .uri(format!("sqlite://{}", db_name))
            .build();

        res.connect().await.expect("fail to connect");
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

        let conn = pool.acquire().await?;

        let backend = conn.backend_name().to_string();

        drop(conn);

        self.backend = BackendSqlX::new_with_name(&backend);

        self.pool = Some(pool);
        self.connected = true;
        Ok(())
    }

    async fn remove_all_tasks_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
    ) -> Result<u64, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::RemoveAllTask);

        Ok(sqlx::query(query)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn remove_all_scheduled_tasks_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
    ) -> Result<u64, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::RemoveAllScheduledTask);

        let now_str = format!("{}", Utc::now().format("%F %T%.f+00"));

        Ok(sqlx::query(query)
            .bind(now_str)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn remove_task_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        id: &Uuid,
    ) -> Result<u64, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::RemoveTask);

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = id.as_hyphenated().encode_lower(&mut buffer);

        let result = sqlx::query(query)
            .bind(&*uuid_as_text)
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
        backend: &BackendSqlX,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task)?;

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        let query = backend.select_query(SqlXQuery::RemoveTaskByMetadata);

        Ok(sqlx::query(query)
            .bind(uniq_hash)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn remove_tasks_type_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        task_type: &str,
    ) -> Result<u64, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::RemoveTaskType);

        Ok(sqlx::query(query)
            .bind(task_type)
            .execute(transaction.acquire().await?)
            .await?
            .rows_affected())
    }

    async fn find_task_by_id_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        id: &Uuid,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::FindTaskById);

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = id.as_hyphenated().encode_lower(&mut buffer);

        let task: Task = sqlx::query_as(query)
            .bind(&*uuid_as_text)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(task)
    }

    async fn fail_task_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        task: &Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::FailTask);

        let updated_at = format!("{}", Utc::now().format("%F %T%.f+00"));

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = task.id.as_hyphenated().encode_lower(&mut buffer);

        let failed_task: Task = sqlx::query_as(query)
            .bind(<&str>::from(FangTaskState::Failed))
            .bind(error_message)
            .bind(updated_at)
            .bind(&*uuid_as_text)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(failed_task)
    }

    async fn schedule_retry_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::RetryTask);

        let now = Utc::now();
        let now_str = format!("{}", now.format("%F %T%.f+00"));

        let scheduled_at = now + Duration::seconds(backoff_seconds as i64);
        let scheduled_at_str = format!("{}", scheduled_at.format("%F %T%.f+00"));
        let retries = task.retries + 1;

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = task.id.as_hyphenated().encode_lower(&mut buffer);

        let failed_task: Task = sqlx::query_as(query)
            .bind(error)
            .bind(retries)
            .bind(scheduled_at_str)
            .bind(now_str)
            .bind(&*uuid_as_text)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(failed_task)
    }

    async fn fetch_and_touch_task_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let task_type = match task_type {
            Some(passed_task_type) => passed_task_type,
            None => DEFAULT_TASK_TYPE.to_string(),
        };

        let task = Self::get_task_type_query(transaction, backend, &task_type)
            .await
            .ok();

        println!("{task:?}");

        let result_task = if let Some(some_task) = task {
            Some(
                Self::update_task_state_query(
                    transaction,
                    backend,
                    &some_task,
                    FangTaskState::InProgress,
                )
                .await?,
            )
        } else {
            None
        };
        Ok(result_task)
    }

    async fn get_task_type_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::FetchTaskType);

        let now_str = format!("{}", Utc::now().format("%F %T%.f+00"));

        let task: Task = sqlx::query_as(query)
            .bind(task_type)
            .bind(now_str)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(task)
    }

    async fn update_task_state_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        task: &Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::UpdateTaskState);

        let updated_at_str = format!("{}", Utc::now().format("%F %T%.f+00"));

        let state_str: &str = state.into();

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = task.id.as_hyphenated().encode_lower(&mut buffer);

        let task: Task = sqlx::query_as(query)
            .bind(state_str)
            .bind(updated_at_str)
            .bind(&*uuid_as_text)
            .fetch_one(transaction.acquire().await?)
            .await?;

        Ok(task)
    }

    async fn insert_task_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::InsertTask);

        let uuid = Uuid::new_v4();
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_str: &str = uuid.as_hyphenated().encode_lower(&mut buffer);

        let metadata_str = metadata.to_string();
        let scheduled_at_str = format!("{}", scheduled_at.format("%F %T%.f+00"));

        let task: Task = sqlx::query_as(query)
            .bind(uuid_as_str)
            .bind(metadata_str)
            .bind(task_type)
            .bind(scheduled_at_str)
            .fetch_one(transaction.acquire().await?)
            .await?;
        Ok(task)
    }

    async fn insert_task_uniq_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let query = backend.select_query(SqlXQuery::InsertTaskUniq);

        let uuid = Uuid::new_v4();
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_str: &str = uuid.as_hyphenated().encode_lower(&mut buffer);

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        let metadata_str = metadata.to_string();
        let scheduled_at_str = format!("{}", scheduled_at.format("%F %T%.f+00"));

        let task: Task = sqlx::query_as(query)
            .bind(uuid_as_str)
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
        backend: &BackendSqlX,
        metadata: serde_json::Value,
        task_type: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        match Self::find_task_by_uniq_hash_query(transaction, backend, &metadata).await {
            Some(task) => Ok(task),
            None => {
                Self::insert_task_uniq_query(
                    transaction,
                    backend,
                    metadata,
                    task_type,
                    scheduled_at,
                )
                .await
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
        backend: &BackendSqlX,
        metadata: &serde_json::Value,
    ) -> Option<Task> {
        let query = backend.select_query(SqlXQuery::FindTaskByUniqHash);

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        sqlx::query_as(query)
            .bind(uniq_hash)
            .fetch_one(transaction.acquire().await.ok()?)
            .await
            .ok()
    }

    async fn schedule_task_query(
        transaction: &mut Transaction<'_, Any>,
        backend: &BackendSqlX,
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
            Self::insert_task_query(
                transaction,
                backend,
                metadata,
                &task.task_type(),
                scheduled_at,
            )
            .await?
        } else {
            Self::insert_task_if_not_exist_query(
                transaction,
                backend,
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
    async fn find_task_by_id(&mut self, id: &Uuid) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task = Self::find_task_by_id_query(&mut transaction, &self.backend, id).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let task =
            Self::fetch_and_touch_task_query(&mut transaction, &self.backend, task_type).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;
        let metadata = serde_json::to_value(task)?;

        let task = if !task.uniq() {
            Self::insert_task_query(
                &mut transaction,
                &self.backend,
                metadata,
                &task.task_type(),
                Utc::now(),
            )
            .await?
        } else {
            Self::insert_task_if_not_exist_query(
                &mut transaction,
                &self.backend,
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

        let task = Self::schedule_task_query(&mut transaction, &self.backend, task).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result = Self::remove_all_tasks_query(&mut transaction, &self.backend).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result =
            Self::remove_all_scheduled_tasks_query(&mut transaction, &self.backend).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_task(&mut self, id: &Uuid) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result = Self::remove_task_query(&mut transaction, &self.backend, id).await?;

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

            let result =
                Self::remove_task_by_metadata_query(&mut transaction, &self.backend, task).await?;

            transaction.commit().await?;

            Ok(result)
        } else {
            Err(AsyncQueueError::TaskNotUniqError)
        }
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let mut transaction = self.pool.as_ref().unwrap().begin().await?;

        let result =
            Self::remove_tasks_type_query(&mut transaction, &self.backend, task_type).await?;

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

        let task =
            Self::update_task_state_query(&mut transaction, &self.backend, task, state).await?;

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

        let task =
            Self::fail_task_query(&mut transaction, &self.backend, task, error_message).await?;

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

        let failed_task = Self::schedule_retry_query(
            &mut transaction,
            &self.backend,
            task,
            backoff_seconds,
            error,
        )
        .await?;

        transaction.commit().await?;

        Ok(failed_task)
    }
}

#[cfg(test)]
test_asynk_queue! {postgres, crate::AsyncQueue, crate::AsyncQueue::test_postgres()}
#[cfg(test)]
test_asynk_queue! {sqlite, crate::AsyncQueue, crate::AsyncQueue::test_sqlite()}
