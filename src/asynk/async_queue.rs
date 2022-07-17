use crate::asynk::AsyncRunnable;
use bb8_postgres::bb8::Pool;
use bb8_postgres::bb8::RunError;
use bb8_postgres::tokio_postgres::tls::MakeTlsConnect;
use bb8_postgres::tokio_postgres::tls::TlsConnect;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::Row;
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::PostgresConnectionManager;
use thiserror::Error;
use typed_builder::TypedBuilder;

#[derive(Error, Debug)]
pub enum AsyncQueueError {
    #[error("pool error")]
    PoolError(#[from] RunError<bb8_postgres::tokio_postgres::Error>),
    #[error("pg query error")]
    PgError(#[from] bb8_postgres::tokio_postgres::Error),
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

    pub async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Row, AsyncQueueError> {
        let json_task = serde_json::to_value(task).unwrap();
        let task_type = task.task_type();

        self.execute_query_one(INSERT_TASK_QUERY, &[&json_task, &task_type])
            .await
    }

    async fn execute_query_one(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, AsyncQueueError> {
        let mut connection = self.pool.get().await?;

        if self.test {
            let transaction = connection.transaction().await?;

            let result = transaction.query_one(query, params).await?;

            transaction.rollback().await?;

            Ok(result)
        } else {
            let result = connection.query_one(query, params).await?;

            Ok(result)
        }
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

        let result = queue.insert_task(&Job { number: 1 }).await;
        eprintln!("{:?}", result);

        log::error!("{:?}", result);
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
