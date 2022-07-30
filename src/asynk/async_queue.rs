use crate::asynk::async_runnable::Error as FangError;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::row::Row;
#[cfg(test)]
use bb8_postgres::tokio_postgres::tls::NoTls;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::tokio_postgres::Transaction;
use bb8_postgres::PostgresConnectionManager;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use postgres_types::{FromSql, ToSql};
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

const INSERT_TASK_QUERY: &str = include_str!("queries/insert_task.sql");
const INSERT_PERIODIC_TASK_QUERY: &str = include_str!("queries/insert_periodic_task.sql");
const SCHEDULE_NEXT_TASK_QUERY: &str = include_str!("queries/schedule_next_task.sql");
const UPDATE_TASK_STATE_QUERY: &str = include_str!("queries/update_task_state.sql");
const FAIL_TASK_QUERY: &str = include_str!("queries/fail_task.sql");
const REMOVE_ALL_TASK_QUERY: &str = include_str!("queries/remove_all_tasks.sql");
const REMOVE_TASK_QUERY: &str = include_str!("queries/remove_task.sql");
const REMOVE_TASKS_TYPE_QUERY: &str = include_str!("queries/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY: &str = include_str!("queries/fetch_task_type.sql");
const FETCH_PERIODIC_TASKS_QUERY: &str = include_str!("queries/fetch_periodic_tasks.sql");
const FIND_TASK_BY_METADATA_QUERY: &str = include_str!("queries/find_task_by_metadata.sql");

#[cfg(test)]
const FIND_TASK_BY_ID_QUERY: &str = include_str!("queries/find_task_by_id.sql");
#[cfg(test)]
const FIND_PERIODIC_TASK_BY_ID_QUERY: &str = include_str!("queries/find_periodic_task_by_id.sql");

pub const DEFAULT_TASK_TYPE: &str = "common";

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

#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PoolError(#[from] RunError<bb8_postgres::tokio_postgres::Error>),
    #[error(transparent)]
    PgError(#[from] bb8_postgres::tokio_postgres::Error),
    #[error("returned invalid result (expected {expected:?}, found {found:?})")]
    ResultError { expected: u64, found: u64 },
}

impl From<AsyncQueueError> for FangError {
    fn from(error: AsyncQueueError) -> Self {
        let message = format!("{:?}", error);
        FangError {
            description: message,
        }
    }
}

#[async_trait]
pub trait AsyncQueueable {
    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError>;

    async fn insert_task(
        &mut self,
        task: serde_json::Value,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError>;
    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    async fn remove_task(&mut self, task: Task) -> Result<u64, AsyncQueueError>;

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError>;

    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError>;

    async fn fail_task(&mut self, task: Task, error_message: &str)
        -> Result<Task, AsyncQueueError>;

    async fn fetch_periodic_tasks(
        &mut self,
        error_margin_seconds: i64,
    ) -> Result<Option<Vec<PeriodicTask>>, AsyncQueueError>;

    async fn insert_periodic_task(
        &mut self,
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        period: i32,
    ) -> Result<PeriodicTask, AsyncQueueError>;
    async fn schedule_next_task(
        &mut self,
        periodic_task: PeriodicTask,
    ) -> Result<PeriodicTask, AsyncQueueError>;
}

#[derive(TypedBuilder, Debug, Clone)]
pub struct AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[builder(setter(into))]
    pool: Pool<PostgresConnectionManager<Tls>>,
    #[builder(setter(into))]
    duplicated_tasks: bool,
}

#[cfg(test)]
pub struct AsyncQueueTest<'a> {
    pub transaction: Transaction<'a>,
    pub duplicated_tasks: bool,
}

#[cfg(test)]
impl<'a> AsyncQueueTest<'a> {
    pub async fn find_task_by_id(&mut self, id: Uuid) -> Result<Task, AsyncQueueError> {
        let row: Row = self
            .transaction
            .query_one(FIND_TASK_BY_ID_QUERY, &[&id])
            .await?;

        let task = AsyncQueue::<NoTls>::row_to_task(row);
        Ok(task)
    }
    pub async fn find_periodic_task_by_id(
        &mut self,
        id: Uuid,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let row: Row = self
            .transaction
            .query_one(FIND_PERIODIC_TASK_BY_ID_QUERY, &[&id])
            .await?;

        let task = AsyncQueue::<NoTls>::row_to_periodic_task(row);
        Ok(task)
    }
}

#[cfg(test)]
#[async_trait]
impl AsyncQueueable for AsyncQueueTest<'_> {
    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let task = AsyncQueue::<NoTls>::fetch_and_touch_task_query(transaction, task_type).await?;

        Ok(task)
    }

    async fn insert_task(
        &mut self,
        metadata: serde_json::Value,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;
        let task: Task;

        if self.duplicated_tasks {
            task = AsyncQueue::<NoTls>::insert_task_query(transaction, metadata, task_type).await?;
        } else {
            task = AsyncQueue::<NoTls>::insert_task_if_not_exist_query(
                transaction,
                metadata,
                task_type,
            )
            .await?;
        }
        Ok(task)
    }

    async fn schedule_next_task(
        &mut self,
        periodic_task: PeriodicTask,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let periodic_task =
            AsyncQueue::<NoTls>::schedule_next_task_query(transaction, periodic_task).await?;

        Ok(periodic_task)
    }
    async fn insert_periodic_task(
        &mut self,
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        period: i32,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let periodic_task = AsyncQueue::<NoTls>::insert_periodic_task_query(
            transaction,
            metadata,
            timestamp,
            period,
        )
        .await?;

        Ok(periodic_task)
    }

    async fn fetch_periodic_tasks(
        &mut self,
        error_margin_seconds: i64,
    ) -> Result<Option<Vec<PeriodicTask>>, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let periodic_task =
            AsyncQueue::<NoTls>::fetch_periodic_tasks_query(transaction, error_margin_seconds)
                .await?;

        Ok(periodic_task)
    }
    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let result = AsyncQueue::<NoTls>::remove_all_tasks_query(transaction).await?;

        Ok(result)
    }

    async fn remove_task(&mut self, task: Task) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let result = AsyncQueue::<NoTls>::remove_task_query(transaction, task).await?;

        Ok(result)
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let result = AsyncQueue::<NoTls>::remove_tasks_type_query(transaction, task_type).await?;

        Ok(result)
    }

    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let task = AsyncQueue::<NoTls>::update_task_state_query(transaction, task, state).await?;

        Ok(task)
    }

    async fn fail_task(
        &mut self,
        task: Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let transaction = &mut self.transaction;

        let task = AsyncQueue::<NoTls>::fail_task_query(transaction, task, error_message).await?;

        Ok(task)
    }
}
impl<Tls> AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn connect(
        uri: impl ToString,
        tls: Tls,
        duplicated_tasks: bool,
    ) -> Result<Self, AsyncQueueError> {
        let manager = PostgresConnectionManager::new_from_stringlike(uri, tls)?;
        let pool = Pool::builder().build(manager).await?;

        Ok(Self {
            pool,
            duplicated_tasks,
        })
    }

    pub async fn remove_all_tasks_query(
        transaction: &mut Transaction<'_>,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(transaction, REMOVE_ALL_TASK_QUERY, &[], None).await
    }

    pub async fn remove_task_query(
        transaction: &mut Transaction<'_>,
        task: Task,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(transaction, REMOVE_TASK_QUERY, &[&task.id], Some(1)).await
    }

    pub async fn remove_tasks_type_query(
        transaction: &mut Transaction<'_>,
        task_type: &str,
    ) -> Result<u64, AsyncQueueError> {
        Self::execute_query(transaction, REMOVE_TASKS_TYPE_QUERY, &[&task_type], None).await
    }

    pub async fn fail_task_query(
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

    pub async fn fetch_and_touch_task_query(
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

    pub async fn get_task_type_query(
        transaction: &mut Transaction<'_>,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let row: Row = transaction
            .query_one(FETCH_TASK_TYPE_QUERY, &[&task_type])
            .await?;

        let task = Self::row_to_task(row);

        Ok(task)
    }

    pub async fn update_task_state_query(
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

    pub async fn insert_task_query(
        transaction: &mut Transaction<'_>,
        metadata: serde_json::Value,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let row: Row = transaction
            .query_one(INSERT_TASK_QUERY, &[&metadata, &task_type])
            .await?;
        let task = Self::row_to_task(row);
        Ok(task)
    }
    pub async fn schedule_next_task_query(
        transaction: &mut Transaction<'_>,
        periodic_task: PeriodicTask,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let updated_at = Utc::now();
        let scheduled_at = updated_at + Duration::seconds(periodic_task.period_in_seconds.into());

        let row: Row = transaction
            .query_one(SCHEDULE_NEXT_TASK_QUERY, &[&scheduled_at, &updated_at])
            .await?;

        let periodic_task = Self::row_to_periodic_task(row);
        Ok(periodic_task)
    }
    pub async fn insert_periodic_task_query(
        transaction: &mut Transaction<'_>,
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        period: i32,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let row: Row = transaction
            .query_one(
                INSERT_PERIODIC_TASK_QUERY,
                &[&metadata, &timestamp, &period],
            )
            .await?;
        let periodic_task = Self::row_to_periodic_task(row);
        Ok(periodic_task)
    }

    pub async fn fetch_periodic_tasks_query(
        transaction: &mut Transaction<'_>,
        error_margin_seconds: i64,
    ) -> Result<Option<Vec<PeriodicTask>>, AsyncQueueError> {
        let current_time = Utc::now();

        let low_limit = current_time - Duration::seconds(error_margin_seconds);
        let high_limit = current_time + Duration::seconds(error_margin_seconds);
        let rows: Vec<Row> = transaction
            .query(FETCH_PERIODIC_TASKS_QUERY, &[&low_limit, &high_limit])
            .await?;

        let periodic_tasks: Vec<PeriodicTask> = rows
            .into_iter()
            .map(|row| Self::row_to_periodic_task(row))
            .collect();

        if periodic_tasks.is_empty() {
            Ok(None)
        } else {
            Ok(Some(periodic_tasks))
        }
    }
    pub async fn execute_query(
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

    pub async fn insert_task_if_not_exist_query(
        transaction: &mut Transaction<'_>,
        metadata: serde_json::Value,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        match Self::find_task_by_metadata_query(transaction, &metadata).await {
            Some(task) => Ok(task),
            None => Self::insert_task_query(transaction, metadata, task_type).await,
        }
    }
    pub async fn find_task_by_metadata_query(
        transaction: &mut Transaction<'_>,
        metadata: &serde_json::Value,
    ) -> Option<Task> {
        let result = transaction
            .query_one(FIND_TASK_BY_METADATA_QUERY, &[metadata])
            .await;

        match result {
            Ok(row) => Some(Self::row_to_task(row)),
            Err(_) => None,
        }
    }
    fn row_to_periodic_task(row: Row) -> PeriodicTask {
        let id: Uuid = row.get("id");
        let metadata: serde_json::Value = row.get("metadata");
        let period_in_seconds: i32 = row.get("period_in_seconds");
        let scheduled_at: Option<DateTime<Utc>> = match row.try_get("scheduled_at") {
            Ok(datetime) => Some(datetime),
            Err(_) => None,
        };
        let created_at: DateTime<Utc> = row.get("created_at");
        let updated_at: DateTime<Utc> = row.get("updated_at");

        PeriodicTask::builder()
            .id(id)
            .metadata(metadata)
            .period_in_seconds(period_in_seconds)
            .scheduled_at(scheduled_at)
            .created_at(created_at)
            .updated_at(updated_at)
            .build()
    }
    fn row_to_task(row: Row) -> Task {
        let id: Uuid = row.get("id");
        let metadata: serde_json::Value = row.get("metadata");
        let error_message: Option<String> = match row.try_get("error_message") {
            Ok(error_message) => Some(error_message),
            Err(_) => None,
        };
        let state: FangTaskState = row.get("state");
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

#[async_trait]
impl<Tls> AsyncQueueable for AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::fetch_and_touch_task_query(&mut transaction, task_type).await?;

        transaction.commit().await?;

        Ok(task)
    }

    async fn insert_task(
        &mut self,
        metadata: serde_json::Value,
        task_type: &str,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let task: Task;

        if self.duplicated_tasks {
            task = Self::insert_task_query(&mut transaction, metadata, task_type).await?;
        } else {
            task =
                Self::insert_task_if_not_exist_query(&mut transaction, metadata, task_type).await?;
        }

        transaction.commit().await?;

        Ok(task)
    }

    async fn insert_periodic_task(
        &mut self,
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        period: i32,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let periodic_task =
            Self::insert_periodic_task_query(&mut transaction, metadata, timestamp, period).await?;

        transaction.commit().await?;

        Ok(periodic_task)
    }

    async fn schedule_next_task(
        &mut self,
        periodic_task: PeriodicTask,
    ) -> Result<PeriodicTask, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let periodic_task = Self::schedule_next_task_query(&mut transaction, periodic_task).await?;

        transaction.commit().await?;

        Ok(periodic_task)
    }

    async fn fetch_periodic_tasks(
        &mut self,
        error_margin_seconds: i64,
    ) -> Result<Option<Vec<PeriodicTask>>, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let periodic_task =
            Self::fetch_periodic_tasks_query(&mut transaction, error_margin_seconds).await?;

        transaction.commit().await?;

        Ok(periodic_task)
    }

    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let result = Self::remove_all_tasks_query(&mut transaction).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_task(&mut self, task: Task) -> Result<u64, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let result = Self::remove_task_query(&mut transaction, task).await?;

        transaction.commit().await?;

        Ok(result)
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        let mut connection = self.pool.get().await?;
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
        let mut connection = self.pool.get().await?;
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
        let mut connection = self.pool.get().await?;
        let mut transaction = connection.transaction().await?;

        let task = Self::fail_task_query(&mut transaction, task, error_message).await?;
        transaction.commit().await?;

        Ok(task)
    }
}

#[cfg(test)]
mod async_queue_tests {
    use super::AsyncQueueTest;
    use super::AsyncQueueable;
    use super::FangTaskState;
    use super::Task;
    use crate::asynk::AsyncRunnable;
    use crate::asynk::Error;
    use async_trait::async_trait;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::tokio_postgres::NoTls;
    use bb8_postgres::PostgresConnectionManager;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait(?Send)]
    impl AsyncRunnable for AsyncTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn insert_task_creates_new_task() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);
        test.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn update_task_state_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        let id = task.id;

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let finished_task = test
            .update_task_state(task, FangTaskState::Finished)
            .await
            .unwrap();

        assert_eq!(id, finished_task.id);
        assert_eq!(FangTaskState::Finished, finished_task.state);

        test.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn failed_task_query_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        let id = task.id;

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let failed_task = test.fail_task(task, "Some error").await.unwrap();

        assert_eq!(id, failed_task.id);
        assert_eq!(Some("Some error"), failed_task.error_message.as_deref());
        assert_eq!(FangTaskState::Failed, failed_task.state);

        test.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn remove_all_tasks_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = insert_task(&mut test, &AsyncTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let result = test.remove_all_tasks().await.unwrap();
        assert_eq!(2, result);

        test.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn fetch_and_touch_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = insert_task(&mut test, &AsyncTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.fetch_and_touch_task(None).await.unwrap().unwrap();

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.fetch_and_touch_task(None).await.unwrap().unwrap();
        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        test.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn remove_tasks_type_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = insert_task(&mut test, &AsyncTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let result = test.remove_tasks_type("mytype").await.unwrap();
        assert_eq!(0, result);

        let result = test.remove_tasks_type("common").await.unwrap();
        assert_eq!(2, result);

        test.transaction.rollback().await.unwrap();
    }

    async fn insert_task(test: &mut AsyncQueueTest<'_>, task: &dyn AsyncRunnable) -> Task {
        let metadata = serde_json::to_value(task).unwrap();
        test.insert_task(metadata, &task.task_type()).await.unwrap()
    }

    async fn pool() -> Pool<PostgresConnectionManager<NoTls>> {
        let pg_mgr = PostgresConnectionManager::new_from_stringlike(
            "postgres://postgres:postgres@localhost/fang",
            NoTls,
        )
        .unwrap();

        Pool::builder().build(pg_mgr).await.unwrap()
    }
}
