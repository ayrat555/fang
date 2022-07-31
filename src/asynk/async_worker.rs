use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::FangTaskState;
use crate::asynk::async_queue::Task;
use crate::asynk::async_queue::DEFAULT_TASK_TYPE;
use crate::asynk::async_runnable::AsyncRunnable;
use crate::asynk::Error;
use crate::{RetentionMode, SleepParams};
use log::error;
use std::time::Duration;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct AsyncWorker<'a> {
    #[builder(setter(into))]
    pub queue: &'a mut dyn AsyncQueueable,
    #[builder(default=DEFAULT_TASK_TYPE.to_string(), setter(into))]
    pub task_type: String,
    #[builder(default, setter(into))]
    pub sleep_params: SleepParams,
    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,
}

impl<'a> AsyncWorker<'a> {
    pub async fn run(&mut self, task: Task) -> Result<(), Error> {
        let result = self.execute_task(task).await;
        self.finalize_task(result).await
    }

    async fn execute_task(&mut self, task: Task) -> Result<Task, (Task, String)> {
        let actual_task: Box<dyn AsyncRunnable> =
            serde_json::from_value(task.metadata.clone()).unwrap();

        let task_result = actual_task.run(self.queue).await;
        match task_result {
            Ok(()) => Ok(task),
            Err(error) => Err((task, error.description)),
        }
    }

    async fn finalize_task(&mut self, result: Result<Task, (Task, String)>) -> Result<(), Error> {
        match self.retention_mode {
            RetentionMode::KeepAll => match result {
                Ok(task) => {
                    self.queue
                        .update_task_state(task, FangTaskState::Finished)
                        .await?;
                    Ok(())
                }
                Err((task, error)) => {
                    self.queue.fail_task(task, &error).await?;
                    Ok(())
                }
            },
            RetentionMode::RemoveAll => match result {
                Ok(task) => {
                    self.queue.remove_task(task).await?;
                    Ok(())
                }
                Err((task, _error)) => {
                    self.queue.remove_task(task).await?;
                    Ok(())
                }
            },
            RetentionMode::RemoveFinished => match result {
                Ok(task) => {
                    self.queue.remove_task(task).await?;
                    Ok(())
                }
                Err((task, error)) => {
                    self.queue.fail_task(task, &error).await?;
                    Ok(())
                }
            },
        }
    }

    pub async fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        tokio::time::sleep(Duration::from_secs(self.sleep_params.sleep_period)).await;
    }

    pub async fn run_tasks(&mut self) -> Result<(), Error> {
        log::info!("Loop..");
        log::info!("kill me...{:?}", self.task_type.clone());
        loop {
            let task_result = self
                .queue
                .fetch_and_touch_task(Some(self.task_type.clone()))
                .await;

            log::info!("result {:?}", task_result);

            match task_result {
                Ok(Some(task)) => {
                    self.sleep_params.maybe_reset_sleep_period();
                    self.run(task).await?
                }
                Ok(None) => {
                    self.sleep().await;
                }

                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);

                    self.sleep().await;
                }
            };
        }
    }

    #[cfg(test)]
    pub async fn run_tasks_until_none(&mut self) -> Result<(), Error> {
        loop {
            match self
                .queue
                .fetch_and_touch_task(Some(self.task_type.clone()))
                .await
            {
                Ok(Some(task)) => {
                    self.sleep_params.maybe_reset_sleep_period();
                    self.run(task).await?
                }
                Ok(None) => {
                    return Ok(());
                }
                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);

                    self.sleep().await;
                }
            };
        }
    }
}

#[cfg(test)]
mod async_worker_tests {
    use super::AsyncWorker;
    use crate::asynk::async_queue::AsyncQueueTest;
    use crate::asynk::async_queue::AsyncQueueable;
    use crate::asynk::async_queue::FangTaskState;
    use crate::asynk::async_worker::Task;
    use crate::asynk::AsyncRunnable;
    use crate::asynk::Error;
    use crate::RetentionMode;
    use async_trait::async_trait;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::tokio_postgres::NoTls;
    use bb8_postgres::PostgresConnectionManager;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for WorkerAsyncTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }
    }
    #[derive(Serialize, Deserialize)]
    struct AsyncFailedTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncFailedTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            let message = format!("number {} is wrong :(", self.number);

            Err(Error {
                description: message,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType1 {}

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncTaskType1 {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type1".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType2 {}

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncTaskType2 {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type2".to_string()
        }
    }
    #[tokio::test]
    async fn execute_and_finishes_task() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest::builder().transaction(transaction).build();

        let task = insert_task(&mut test, &WorkerAsyncTask { number: 1 }).await;
        let id = task.id;

        let mut worker = AsyncWorker::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run(task).await.unwrap();
        let task_finished = test.find_task_by_id(id).await.unwrap();
        assert_eq!(id, task_finished.id);
        assert_eq!(FangTaskState::Finished, task_finished.state);
        test.transaction.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn saves_error_for_failed_task() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest::builder().transaction(transaction).build();

        let task = insert_task(&mut test, &AsyncFailedTask { number: 1 }).await;
        let id = task.id;

        let mut worker = AsyncWorker::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run(task).await.unwrap();
        let task_finished = test.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task_finished.id);
        assert_eq!(FangTaskState::Failed, task_finished.state);
        assert_eq!(
            "number 1 is wrong :(".to_string(),
            task_finished.error_message.unwrap()
        );
        test.transaction.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn executes_task_only_of_specific_type() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest::builder().transaction(transaction).build();

        let task1 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task12 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task2 = insert_task(&mut test, &AsyncTaskType2 {}).await;

        let id1 = task1.id;
        let id12 = task12.id;
        let id2 = task2.id;

        let mut worker = AsyncWorker::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .task_type("type1".to_string())
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run_tasks_until_none().await.unwrap();
        let task1 = test.find_task_by_id(id1).await.unwrap();
        let task12 = test.find_task_by_id(id12).await.unwrap();
        let task2 = test.find_task_by_id(id2).await.unwrap();

        assert_eq!(id1, task1.id);
        assert_eq!(id12, task12.id);
        assert_eq!(id2, task2.id);
        assert_eq!(FangTaskState::Finished, task1.state);
        assert_eq!(FangTaskState::Finished, task12.state);
        assert_eq!(FangTaskState::New, task2.state);
        test.transaction.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn remove_when_finished() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest::builder().transaction(transaction).build();

        let task1 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task12 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task2 = insert_task(&mut test, &AsyncTaskType2 {}).await;

        let _id1 = task1.id;
        let _id12 = task12.id;
        let id2 = task2.id;

        let mut worker = AsyncWorker::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .task_type("type1".to_string())
            .build();

        worker.run_tasks_until_none().await.unwrap();
        let task = test
            .fetch_and_touch_task(Some("type1".to_string()))
            .await
            .unwrap();
        assert_eq!(None, task);

        let task2 = test
            .fetch_and_touch_task(Some("type2".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id2, task2.id);

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
