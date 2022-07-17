use crate::asynk::AsyncRunnable;
use bb8_postgres::bb8::Pool;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::tls::MakeTlsConnect;
use bb8_postgres::tokio_postgres::tls::TlsConnect;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::PostgresConnectionManager;
use chrono::NaiveDateTime;
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PoolError(#[from] RunError<bb8_postgres::tokio_postgres::Error>),
    #[error(transparent)]
    PgError(#[from] bb8_postgres::tokio_postgres::Error),
    #[error("returned invalid result (expected {expected:?}, found {found:?})")]
    ResultError { expected: u64, found: u64 },
}

#[derive(Debug, TypedBuilder)]
pub struct AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pool: Pool<PostgresConnectionManager<Tls>>,
    #[builder(default = false)]
    test: bool,
}

const INSERT_TASK_QUERY: &str = include_str!("queries/insert_task.sql");
const UPDATE_TASK_STATE_QUERY: &str = include_str!("queries/update_task_state.sql");
const FAIL_TASK_QUERY: &str = include_str!("queries/fail_task.sql");
const REMOVE_ALL_TASK_QUERY: &str = include_str!("queries/remove_all_tasks.sql");
const REMOVE_TASK_QUERY: &str = include_str!("queries/remove_task.sql");
const REMOVE_TASKS_TYPE_QUERY: &str = include_str!("queries/remove_tasks_type.sql");
// const SCHEDULE_NEXT_TASK_QUERY: &str = include_str!("queries/schedule_next_task.sql");

impl<Tls> AsyncQueue<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        AsyncQueue::builder().pool(pool).build()
    }

    pub async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task).unwrap();
        let task_type = task.task_type();

        self.execute_one(INSERT_TASK_QUERY, &[&metadata, &task_type])
            .await
    }
    pub async fn update_task_state(
        &mut self,
        uuid: Uuid,
        state: &str,
        updated_at: NaiveDateTime,
    ) -> Result<u64, AsyncQueueError> {
        self.execute_one(UPDATE_TASK_STATE_QUERY, &[&state, &updated_at, &uuid])
            .await
    }
    pub async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        self.execute_one(REMOVE_ALL_TASK_QUERY, &[]).await
    }
    pub async fn remove_task(&mut self, uuid: Uuid) -> Result<u64, AsyncQueueError> {
        self.execute_one(REMOVE_TASK_QUERY, &[&uuid]).await
    }
    pub async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        self.execute_one(REMOVE_TASKS_TYPE_QUERY, &[&task_type])
            .await
    }
    pub async fn fail_task(
        &mut self,
        uuid: Uuid,
        state: &str,
        error_message: &str,
        updated_at: NaiveDateTime,
    ) -> Result<u64, AsyncQueueError> {
        self.execute_one(
            FAIL_TASK_QUERY,
            &[&state, &error_message, &updated_at, &uuid],
        )
        .await
    }
    async fn execute_one(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, AsyncQueueError> {
        let mut connection = self.pool.get().await?;

        let result = if self.test {
            let transaction = connection.transaction().await?;

            let result = transaction.execute(query, params).await?;

            transaction.rollback().await?;

            result
        } else {
            connection.execute(query, params).await?
        };

        if result != 1 {
            return Err(AsyncQueueError::ResultError {
                expected: 1,
                found: result,
            });
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
        let mut queue = queue().await;

        let result = queue.insert_task(&Job { number: 1 }).await.unwrap();

        assert_eq!(1, result);
    }

    async fn queue() -> AsyncQueue<NoTls> {
        let pg_mgr = PostgresConnectionManager::new_from_stringlike(
            "postgres://postgres:postgres@localhost/fang",
            NoTls,
        )
        .unwrap();

        let pool = Pool::builder().build(pg_mgr).await.unwrap();

        AsyncQueue::builder().pool(pool).test(true).build()
    }
}
