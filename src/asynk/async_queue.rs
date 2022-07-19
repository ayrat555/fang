use crate::asynk::AsyncRunnable;
use bb8_postgres::bb8::Pool;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::row::Row;
use bb8_postgres::tokio_postgres::tls::MakeTlsConnect;
use bb8_postgres::tokio_postgres::tls::TlsConnect;
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::tokio_postgres::Transaction;
use bb8_postgres::PostgresConnectionManager;
use chrono::DateTime;
use chrono::Utc;
use postgres_types::{FromSql, ToSql};
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

const INSERT_TASK_QUERY: &str = include_str!("queries/insert_task.sql");
const UPDATE_TASK_STATE_QUERY: &str = include_str!("queries/update_task_state.sql");
const FAIL_TASK_QUERY: &str = include_str!("queries/fail_task.sql");
const REMOVE_ALL_TASK_QUERY: &str = include_str!("queries/remove_all_tasks.sql");
const REMOVE_TASK_QUERY: &str = include_str!("queries/remove_task.sql");
const REMOVE_TASKS_TYPE_QUERY: &str = include_str!("queries/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY: &str = include_str!("queries/fetch_task_type.sql");

const DEFAULT_TASK_TYPE: &str = "common";

#[derive(Debug, Eq, PartialEq, Clone, ToSql, FromSql)]
#[postgres(name = "fang_task_state")]
pub enum FangTaskState {
    #[postgres(name = "new")]
    New,
    #[postgres(name = "in_progress")]
    InProgress,
    #[postgres(name = "failed")]
    Failed,
    #[postgres(name = "finished")]
    Finished,
}
impl Default for FangTaskState {
    fn default() -> Self {
        FangTaskState::New
    }
}
#[derive(TypedBuilder, Debug, Eq, PartialEq, Clone)]
pub struct Task {
    #[builder(setter(into))]
    pub id: Uuid,
    #[builder(setter(into))]
    pub metadata: serde_json::Value,
    #[builder(setter(into))]
    pub error_message: Option<String>,
    #[builder(default, setter(into))]
    pub state: FangTaskState,
    #[builder(setter(into))]
    pub task_type: String,
    #[builder(setter(into))]
    pub created_at: DateTime<Utc>,
    #[builder(setter(into))]
    pub updated_at: DateTime<Utc>,
}

#[derive(TypedBuilder, Debug, Eq, PartialEq, Clone)]
pub struct PeriodicTask {
    #[builder(setter(into))]
    pub id: Uuid,
    #[builder(setter(into))]
    pub metadata: serde_json::Value,
    #[builder(setter(into))]
    pub period_in_seconds: i32,
    #[builder(setter(into))]
    pub scheduled_at: Option<DateTime<Utc>>,
    #[builder(setter(into))]
    pub created_at: DateTime<Utc>,
    #[builder(setter(into))]
    pub updated_at: DateTime<Utc>,
}

#[derive(TypedBuilder, Debug, Eq, PartialEq, Clone)]
pub struct NewTask {
    #[builder(setter(into))]
    pub metadata: serde_json::Value,
    #[builder(setter(into))]
    pub task_type: String,
}

#[derive(TypedBuilder, Debug, Eq, PartialEq, Clone)]
pub struct NewPeriodicTask {
    #[builder(setter(into))]
    pub metadata: serde_json::Value,
    #[builder(setter(into))]
    pub period_in_seconds: i32,
}
#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PoolError(#[from] RunError<bb8_postgres::tokio_postgres::Error>),
    #[error(transparent)]
    PgError(#[from] bb8_postgres::tokio_postgres::Error),
    #[error("returned invalid result (expected {expected:?}, found {found:?})")]
    ResultError { expected: u64, found: u64 },
}

pub struct AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        AsyncQueue { pool }
    }

    pub async fn fetch_and_touch_task(
        &mut self,
        task_type: &Option<String>,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::fetch_and_touch(&mut transaction, task_type).await?;

        transaction.commit().await?;

        Ok(task)
    }

    pub async fn fetch_and_touch(
        transaction: &mut Transaction<'_>,
        task_type: &Option<String>,
    ) -> Result<Task, AsyncQueueError> {
        let mut task = match task_type {
            None => Self::get_task_type(transaction, DEFAULT_TASK_TYPE).await?,
            Some(task_type_str) => Self::get_task_type(transaction, task_type_str).await?,
        };

        Self::update_task_state(transaction, &task, FangTaskState::InProgress).await?;

        task.state = FangTaskState::InProgress;

        Ok(task)
    }

    async fn get_task_type(
        transaction: &mut Transaction<'_>,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let row: Row = transaction
            .query_one(FETCH_TASK_TYPE_QUERY, &[&task_type])
            .await?;

        let task = Self::row_to_task(row);

        Ok(task)
    }

    pub async fn get_row(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, AsyncQueueError> {
        let connection = self.pool.get().await?;

        let row = connection.query_one(query, params).await?;

        Ok(row)
    }

    pub async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task).unwrap();
        let task_type = task.task_type();

        self.execute(INSERT_TASK_QUERY, &[&metadata, &task_type], Some(1))
            .await
    }

    pub async fn update_task_state(
        transaction: &mut Transaction<'_>,
        task: &Task,
        state: FangTaskState,
    ) -> Result<u64, AsyncQueueError> {
        let updated_at = Utc::now();

        let count = transaction
            .execute(UPDATE_TASK_STATE_QUERY, &[&state, &updated_at, &task.id])
            .await?;

        Ok(count)
    }

    pub async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.execute(REMOVE_ALL_TASK_QUERY, &[], None).await
    }

    pub async fn remove_task(&mut self, task: &Task) -> Result<u64, AsyncQueueError> {
        self.execute(REMOVE_TASK_QUERY, &[&task.id], Some(1)).await
    }

    pub async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        self.execute(REMOVE_TASKS_TYPE_QUERY, &[&task_type], None)
            .await
    }

    pub async fn fail_task(&mut self, task: &Task) -> Result<u64, AsyncQueueError> {
        let updated_at = Utc::now();
        self.execute(
            FAIL_TASK_QUERY,
            &[
                &FangTaskState::Failed,
                &task.error_message,
                &updated_at,
                &task.id,
            ],
            Some(1),
        )
        .await
    }

    async fn execute(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        expected_result_count: Option<u64>,
    ) -> Result<u64, AsyncQueueError> {
        let connection = self.pool.get().await?;

        let result = connection.execute(query, params).await?;
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

    fn row_to_task(row: Row) -> Task {
        let id: Uuid = row.get("id");
        let metadata: serde_json::Value = row.get("metadata");
        let error_message: Option<String> = match row.try_get("error_message") {
            Ok(error_message) => Some(error_message),
            Err(_) => None,
        };
        let state: FangTaskState = FangTaskState::New;
        let task_type: String = row.get("task_type");
        let created_at: DateTime<Utc> = row.get("created_at");
        let updated_at: DateTime<Utc> = row.get("updated_at");

        Task::builder()
            .id(id)
            .metadata(metadata)
            .error_message(error_message)
            .state(state)
            .task_type(task_type)
            .created_at(created_at)
            .updated_at(updated_at)
            .build()
    }
}

#[cfg(test)]
mod async_queue_tests {
    use super::AsyncQueue;
    use crate::asynk::AsyncRunnable;
    use crate::asynk::Error;
    use async_trait::async_trait;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::tokio_postgres::Client;
    use bb8_postgres::tokio_postgres::NoTls;
    use bb8_postgres::PostgresConnectionManager;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncTask {
        async fn run(&self, _connection: &Client) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn insert_task_creates_new_task() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();
        let mut queue = AsyncQueue::<NoTls>::new_with_transaction(transaction);

        let result = queue.insert_task(&AsyncTask { number: 1 }).await.unwrap();

        assert_eq!(1, result);
        queue.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn remove_all_tasks_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();
        let mut queue = AsyncQueue::<NoTls>::new_with_transaction(transaction);

        let result = queue.insert_task(&AsyncTask { number: 1 }).await.unwrap();
        assert_eq!(1, result);

        let result = queue.insert_task(&AsyncTask { number: 2 }).await.unwrap();
        assert_eq!(1, result);

        let result = queue.remove_all_tasks().await.unwrap();
        assert_eq!(2, result);

        queue.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn fetch_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();
        let mut queue = AsyncQueue::<NoTls>::new_with_transaction(transaction);

        let result = queue.insert_task(&AsyncTask { number: 1 }).await.unwrap();
        assert_eq!(1, result);

        let result = queue.insert_task(&AsyncTask { number: 2 }).await.unwrap();
        assert_eq!(1, result);

        let task = queue.fetch_and_touch_task(&None).await.unwrap();
        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = queue.fetch_and_touch_task(&None).await.unwrap();
        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        queue.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn remove_tasks_type_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();
        let mut queue = AsyncQueue::<NoTls>::new_with_transaction(transaction);

        let result = queue.insert_task(&AsyncTask { number: 1 }).await.unwrap();
        assert_eq!(1, result);

        let result = queue.insert_task(&AsyncTask { number: 2 }).await.unwrap();
        assert_eq!(1, result);

        let result = queue.remove_tasks_type("common").await.unwrap();
        assert_eq!(2, result);

        queue.rollback().await.unwrap();
    }

    async fn pool() -> Pool<PostgresConnectionManager<NoTls>> {
        let pg_mgr = PostgresConnectionManager::new_from_stringlike(
            "postgres://postgres:mypassword1@localhost/fang",
            NoTls,
        )
        .unwrap();

        Pool::builder().build(pg_mgr).await.unwrap()
    }
}
