#[cfg(test)]
mod async_queue_tests;

use crate::asynk::async_runnable::AsyncRunnable;
use crate::backend_sqlx::QueryParams;
use crate::backend_sqlx::SqlXQuery;
use crate::CronError;
use crate::FangTaskState;
use crate::Scheduled::*;
use crate::Task;
use async_trait::async_trait;

use chrono::DateTime;
use chrono::Utc;
use cron::Schedule;
//use sqlx::any::install_default_drivers; // this is supported in sqlx 0.7
use sqlx::any::AnyKind;
use sqlx::pool::PoolOptions;
use sqlx::Any;
use sqlx::AnyPool;
use sqlx::Pool;
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
/// This is implemented by the `AsyncQueue` struct which uses internally a `AnyPool` of `sqlx` to connect to the database.

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
static ASYNC_QUEUE_MYSQL_TEST_COUNTER: Mutex<u32> = Mutex::const_new(0);

#[cfg(test)]
use sqlx::Executor;

#[cfg(test)]
use std::path::Path;

#[cfg(test)]
use std::env;

use super::backend_sqlx::BackendSqlX;

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
        //install_default_drivers();

        let pool: AnyPool = PoolOptions::new()
            .max_connections(self.max_pool_size)
            .connect(&self.uri)
            .await?;

        let anykind = pool.any_kind();

        let backend = match anykind {
            AnyKind::Postgres => BackendSqlX::Pg,
            AnyKind::Sqlite => BackendSqlX::Sqlite,
            AnyKind::MySql => BackendSqlX::Mysql,
        };

        self.backend = backend;

        self.pool = Some(pool);
        self.connected = true;
        Ok(())
    }

    async fn fetch_and_touch_task_query(
        pool: &Pool<Any>,
        backend: &BackendSqlX,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let task_type = match task_type {
            Some(passed_task_type) => passed_task_type,
            None => DEFAULT_TASK_TYPE.to_string(),
        };

        let query_params = QueryParams::builder().task_type(&task_type).build();

        let task = backend
            .execute_query(SqlXQuery::FetchTaskType, pool, query_params)
            .await
            .map(|val| val.unwrap_task())
            .ok();

        let result_task = if let Some(some_task) = task {
            let query_params = QueryParams::builder()
                .uuid(&some_task.id)
                .state(FangTaskState::InProgress)
                .build();

            let task = backend
                .execute_query(SqlXQuery::UpdateTaskState, pool, query_params)
                .await?
                .unwrap_task();

            Some(task)
        } else {
            None
        };
        Ok(result_task)
    }

    async fn insert_task_query(
        pool: &Pool<Any>,
        backend: &BackendSqlX,
        metadata: &serde_json::Value,
        task_type: &str,
        scheduled_at: &DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let query_params = QueryParams::builder()
            .metadata(metadata)
            .task_type(task_type)
            .scheduled_at(scheduled_at)
            .build();

        let task = backend
            .execute_query(SqlXQuery::InsertTask, pool, query_params)
            .await?
            .unwrap_task();

        Ok(task)
    }

    async fn insert_task_if_not_exist_query(
        pool: &Pool<Any>,
        backend: &BackendSqlX,
        metadata: &serde_json::Value,
        task_type: &str,
        scheduled_at: &DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        let query_params = QueryParams::builder()
            .metadata(metadata)
            .task_type(task_type)
            .scheduled_at(scheduled_at)
            .build();

        let task = backend
            .execute_query(SqlXQuery::InsertTaskIfNotExists, pool, query_params)
            .await?
            .unwrap_task();

        Ok(task)
    }

    async fn schedule_task_query(
        pool: &Pool<Any>,
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
            Self::insert_task_query(pool, backend, &metadata, &task.task_type(), &scheduled_at)
                .await?
        } else {
            Self::insert_task_if_not_exist_query(
                pool,
                backend,
                &metadata,
                &task.task_type(),
                &scheduled_at,
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
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder().uuid(id).build();

        let task = self
            .backend
            .execute_query(SqlXQuery::FindTaskById, pool, query_params)
            .await?
            .unwrap_task();

        Ok(task)
    }

    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        self.check_if_connection()?;
        // this unwrap is safe because we check if connection is established
        let pool = self.pool.as_ref().unwrap();

        let task = Self::fetch_and_touch_task_query(pool, &self.backend, task_type).await?;

        Ok(task)
    }

    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        // this unwrap is safe because we check if connection is established
        let pool = self.pool.as_ref().unwrap();
        let metadata = serde_json::to_value(task)?;

        let task = if !task.uniq() {
            Self::insert_task_query(
                pool,
                &self.backend,
                &metadata,
                &task.task_type(),
                &Utc::now(),
            )
            .await?
        } else {
            Self::insert_task_if_not_exist_query(
                pool,
                &self.backend,
                &metadata,
                &task.task_type(),
                &Utc::now(),
            )
            .await?
        };

        Ok(task)
    }

    async fn schedule_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        // this unwrap is safe because we check if connection is established
        let pool = self.pool.as_ref().unwrap();

        let task = Self::schedule_task_query(pool, &self.backend, task).await?;

        Ok(task)
    }

    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        // this unwrap is safe because we check if connection is established
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder().build();

        let result = self
            .backend
            .execute_query(SqlXQuery::RemoveAllTask, pool, query_params)
            .await?
            .unwrap_u64();

        Ok(result)
    }

    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        // this unwrap is safe because we check if connection is established
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder().build();

        let result = self
            .backend
            .execute_query(SqlXQuery::RemoveAllScheduledTask, pool, query_params)
            .await?
            .unwrap_u64();

        Ok(result)
    }

    async fn remove_task(&mut self, id: &Uuid) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder().uuid(id).build();

        let result = self
            .backend
            .execute_query(SqlXQuery::RemoveTask, pool, query_params)
            .await?
            .unwrap_u64();

        Ok(result)
    }

    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        if task.uniq() {
            self.check_if_connection()?;
            let pool = self.pool.as_ref().unwrap();

            let query_params = QueryParams::builder().runnable(task).build();

            let result = self
                .backend
                .execute_query(SqlXQuery::RemoveTaskByMetadata, pool, query_params)
                .await?
                .unwrap_u64();

            Ok(result)
        } else {
            Err(AsyncQueueError::TaskNotUniqError)
        }
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        self.check_if_connection()?;
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder().task_type(task_type).build();

        let result = self
            .backend
            .execute_query(SqlXQuery::RemoveTaskType, pool, query_params)
            .await?
            .unwrap_u64();

        Ok(result)
    }

    async fn update_task_state(
        &mut self,
        task: &Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder().uuid(&task.id).state(state).build();

        let task = self
            .backend
            .execute_query(SqlXQuery::UpdateTaskState, pool, query_params)
            .await?
            .unwrap_task();

        Ok(task)
    }

    async fn fail_task(
        &mut self,
        task: &Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;
        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder()
            .error_message(error_message)
            .task(task)
            .build();

        let failed_task = self
            .backend
            .execute_query(SqlXQuery::FailTask, pool, query_params)
            .await?
            .unwrap_task();

        Ok(failed_task)
    }

    async fn schedule_retry(
        &mut self,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        self.check_if_connection()?;

        let pool = self.pool.as_ref().unwrap();

        let query_params = QueryParams::builder()
            .backoff_seconds(backoff_seconds)
            .error_message(error)
            .task(task)
            .build();

        let failed_task = self
            .backend
            .execute_query(SqlXQuery::RetryTask, pool, query_params)
            .await?
            .unwrap_task();

        Ok(failed_task)
    }
}

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

    /// Provides an AsyncQueue connected to its own DB
    pub async fn test_mysql() -> Self {
        dotenvy::dotenv().expect(".env file not found");
        let base_url = env::var("MYSQL_BASE_URL").expect("Base URL for MySQL not found");
        let base_db = env::var("MYSQL_DB").expect("Name for base MySQL DB not found");

        let mut res = Self::builder()
            .max_pool_size(1_u32)
            .uri(format!("{}/{}", base_url, base_db))
            .build();

        let mut new_number = ASYNC_QUEUE_MYSQL_TEST_COUNTER.lock().await;
        res.connect().await.unwrap();

        let db_name = format!("async_queue_test_{}", *new_number);
        *new_number += 1;

        let create_query: &str = &format!(
            "CREATE DATABASE {}; CREATE TABLE {}.fang_tasks LIKE fang.fang_tasks;",
            db_name, db_name
        );

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
}

#[cfg(test)]
test_asynk_queue! {postgres, crate::AsyncQueue, crate::AsyncQueue::test_postgres()}
#[cfg(test)]
test_asynk_queue! {sqlite, crate::AsyncQueue, crate::AsyncQueue::test_sqlite()}

#[cfg(test)]
test_asynk_queue! {mysql, crate::AsyncQueue, crate::AsyncQueue::test_mysql()}
