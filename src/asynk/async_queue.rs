use crate::asynk::AsyncRunnable;
use crate::Task;
use bb8_postgres::bb8::Pool;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::tls::MakeTlsConnect;
use bb8_postgres::tokio_postgres::tls::TlsConnect;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::tokio_postgres::Transaction;
use bb8_postgres::PostgresConnectionManager;
use chrono::Utc;
use thiserror::Error;
use typed_builder::TypedBuilder;

#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PoolError(#[from] RunError<bb8_postgres::tokio_postgres::Error>),
    #[error(transparent)]
    PgError(#[from] bb8_postgres::tokio_postgres::Error),
    #[error("returned invalid result (expected {expected:?}, found {found:?})")]
    ResultError { expected: u64, found: u64 },
    #[error("Queue doesn't have a connection")]
    PoolAndTransactionEmpty,
    #[error("Need to create a transaction to perform this operation")]
    TransactionEmpty,
}

#[derive(TypedBuilder)]
pub struct AsyncQueue<'a, Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[builder(default, setter(into))]
    pool: Option<Pool<PostgresConnectionManager<Tls>>>,
    #[builder(default, setter(into))]
    transaction: Option<Transaction<'a>>,
}

const INSERT_TASK_QUERY: &str = include_str!("queries/insert_task.sql");
const UPDATE_TASK_STATE_QUERY: &str = include_str!("queries/update_task_state.sql");
const FAIL_TASK_QUERY: &str = include_str!("queries/fail_task.sql");
const REMOVE_ALL_TASK_QUERY: &str = include_str!("queries/remove_all_tasks.sql");
const REMOVE_TASK_QUERY: &str = include_str!("queries/remove_task.sql");
const REMOVE_TASKS_TYPE_QUERY: &str = include_str!("queries/remove_tasks_type.sql");

impl<'a, Tls> AsyncQueue<'a, Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        AsyncQueue::builder().pool(pool).build()
    }

    pub fn new_with_transaction(transaction: Transaction<'a>) -> Self {
        AsyncQueue::builder().transaction(transaction).build()
    }
    pub async fn rollback(mut self) -> Result<AsyncQueue<'a, Tls>, AsyncQueueError> {
        let transaction = self.transaction;
        self.transaction = None;
        match transaction {
            Some(tr) => {
                tr.rollback().await?;
                Ok(self)
            }
            None => Err(AsyncQueueError::TransactionEmpty),
        }
    }
    pub async fn commit(mut self) -> Result<AsyncQueue<'a, Tls>, AsyncQueueError> {
        let transaction = self.transaction;
        self.transaction = None;
        match transaction {
            Some(tr) => {
                tr.commit().await?;
                Ok(self)
            }
            None => Err(AsyncQueueError::TransactionEmpty),
        }
    }
    pub async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task).unwrap();
        let task_type = task.task_type();

        self.execute(INSERT_TASK_QUERY, &[&metadata, &task_type], Some(1))
            .await
    }
    pub async fn update_task_state(
        &mut self,
        task: &Task,
        state: &str,
    ) -> Result<u64, AsyncQueueError> {
        let updated_at = Utc::now();
        self.execute(
            UPDATE_TASK_STATE_QUERY,
            &[&state, &updated_at, &task.id],
            Some(1),
        )
        .await
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
            &[&"failed", &task.error_message, &updated_at, &task.id],
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
        let result = if let Some(pool) = &self.pool {
            let connection = pool.get().await?;

            connection.execute(query, params).await?
        } else if let Some(transaction) = &self.transaction {
            transaction.execute(query, params).await?
        } else {
            return Err(AsyncQueueError::PoolAndTransactionEmpty);
        };
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
    struct Job {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for Job {
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

        let result = queue.insert_task(&Job { number: 1 }).await.unwrap();

        assert_eq!(1, result);
        queue.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn remove_all_tasks_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();
        let mut queue = AsyncQueue::<NoTls>::new_with_transaction(transaction);

        let result = queue.insert_task(&Job { number: 1 }).await.unwrap();
        assert_eq!(1, result);
        let result = queue.insert_task(&Job { number: 2 }).await.unwrap();
        assert_eq!(1, result);
        let result = queue.remove_all_tasks().await.unwrap();
        assert_eq!(2, result);
        queue.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn remove_tasks_type_test() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();
        let mut queue = AsyncQueue::<NoTls>::new_with_transaction(transaction);

        let result = queue.insert_task(&Job { number: 1 }).await.unwrap();
        assert_eq!(1, result);
        let result = queue.insert_task(&Job { number: 2 }).await.unwrap();
        assert_eq!(1, result);
        let result = queue.remove_tasks_type("common").await.unwrap();
        assert_eq!(2, result);
        queue.rollback().await.unwrap();
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
