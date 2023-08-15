use super::AsyncQueueTest;
use super::AsyncQueueable;
use super::FangTaskState;
use super::Task;
use crate::asynk::AsyncRunnable;
use crate::FangError;
use crate::Scheduled;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use chrono::DateTime;
use chrono::Duration;
use chrono::SubsecRound;
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AsyncTask {
    pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncTask {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct AsyncUniqTask {
    pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncUniqTask {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        Ok(())
    }

    fn uniq(&self) -> bool {
        true
    }
}

#[derive(Serialize, Deserialize)]
struct AsyncTaskSchedule {
    pub number: u16,
    pub datetime: String,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncTaskSchedule {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        Ok(())
    }

    fn cron(&self) -> Option<Scheduled> {
        let datetime = self.datetime.parse::<DateTime<Utc>>().ok()?;
        Some(Scheduled::ScheduleOnce(datetime))
    }
}

#[tokio::test]
async fn insert_task_creates_new_task() {
    let pool = pool().await;
    let mut connection = pool.get().await.unwrap();
    let transaction = connection.transaction().await.unwrap();

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

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

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

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

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

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

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

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
async fn schedule_task_test() {
    let pool = pool().await;
    let mut connection = pool.get().await.unwrap();
    let transaction = connection.transaction().await.unwrap();

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

    let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

    let task = &AsyncTaskSchedule {
        number: 1,
        datetime: datetime.to_string(),
    };

    let task = test.schedule_task(task).await.unwrap();

    let metadata = task.metadata.as_object().unwrap();
    let number = metadata["number"].as_u64();
    let type_task = metadata["type"].as_str();

    assert_eq!(Some(1), number);
    assert_eq!(Some("AsyncTaskSchedule"), type_task);
    assert_eq!(task.scheduled_at, datetime);
}

#[tokio::test]
async fn remove_all_scheduled_tasks_test() {
    let pool = pool().await;
    let mut connection = pool.get().await.unwrap();
    let transaction = connection.transaction().await.unwrap();

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

    let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

    let task1 = &AsyncTaskSchedule {
        number: 1,
        datetime: datetime.to_string(),
    };

    let task2 = &AsyncTaskSchedule {
        number: 2,
        datetime: datetime.to_string(),
    };

    test.schedule_task(task1).await.unwrap();
    test.schedule_task(task2).await.unwrap();

    let number = test.remove_all_scheduled_tasks().await.unwrap();

    assert_eq!(2, number);
}

#[tokio::test]
async fn fetch_and_touch_test() {
    let pool = pool().await;
    let mut connection = pool.get().await.unwrap();
    let transaction = connection.transaction().await.unwrap();

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

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

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

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

#[tokio::test]
async fn remove_tasks_by_metadata() {
    let pool = pool().await;
    let mut connection = pool.get().await.unwrap();
    let transaction = connection.transaction().await.unwrap();

    let mut test = AsyncQueueTest::builder().transaction(transaction).build();

    let task = insert_task(&mut test, &AsyncUniqTask { number: 1 }).await;

    let metadata = task.metadata.as_object().unwrap();
    let number = metadata["number"].as_u64();
    let type_task = metadata["type"].as_str();

    assert_eq!(Some(1), number);
    assert_eq!(Some("AsyncUniqTask"), type_task);

    let task = insert_task(&mut test, &AsyncUniqTask { number: 2 }).await;

    let metadata = task.metadata.as_object().unwrap();
    let number = metadata["number"].as_u64();
    let type_task = metadata["type"].as_str();

    assert_eq!(Some(2), number);
    assert_eq!(Some("AsyncUniqTask"), type_task);

    let result = test
        .remove_task_by_metadata(&AsyncUniqTask { number: 0 })
        .await
        .unwrap();
    assert_eq!(0, result);

    let result = test
        .remove_task_by_metadata(&AsyncUniqTask { number: 1 })
        .await
        .unwrap();
    assert_eq!(1, result);

    test.transaction.rollback().await.unwrap();
}

async fn insert_task(test: &mut AsyncQueueTest<'_>, task: &dyn AsyncRunnable) -> Task {
    test.insert_task(task).await.unwrap()
}

async fn pool() -> Pool<PostgresConnectionManager<NoTls>> {
    let pg_mgr = PostgresConnectionManager::new_from_stringlike(
        "postgres://postgres:postgres@localhost/fang",
        NoTls,
    )
    .unwrap();

    Pool::builder().build(pg_mgr).await.unwrap()
}