use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::FangTaskState;
use crate::asynk::async_queue::Task;
use crate::asynk::async_runnable::AsyncRunnable;
use crate::asynk::Error;
use crate::{RetentionMode, SleepParams};
use log::error;
use std::time::Duration;
use typed_builder::TypedBuilder;

pub enum SomeValue {
    SomeTask(Task),
    SomeNumber(u64),
}
impl SomeValue {
    fn _get_task(&self) -> Task {
        match self {
            Self::SomeTask(task) => task.clone(),
            Self::SomeNumber(_) => panic!("try to get a task and it was a number"),
        }
    }
    fn _get_number(&self) -> u64 {
        match self {
            Self::SomeNumber(number) => *number,
            Self::SomeTask(_) => panic!("try to get a number and it was a task"),
        }
    }
}
#[derive(TypedBuilder)]
pub struct AsyncWorker<'a> {
    #[builder(setter(into))]
    pub queue: &'a mut dyn AsyncQueueable,
    #[builder(setter(into))]
    pub task_type: Option<String>,
    #[builder(default, setter(into))]
    pub sleep_params: SleepParams,
    #[builder(setter(into))]
    pub retention_mode: RetentionMode,
}
impl<'a> AsyncWorker<'a> {
    pub async fn run(&mut self, task: Task) -> SomeValue {
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
    async fn finalize_task(&mut self, result: Result<Task, (Task, String)>) -> SomeValue {
        match self.retention_mode {
            RetentionMode::KeepAll => match result {
                Ok(task) => SomeValue::SomeTask(
                    self.queue
                        .update_task_state(task, FangTaskState::Finished)
                        .await
                        .unwrap(),
                ),
                Err((task, error)) => {
                    SomeValue::SomeTask(self.queue.fail_task(task, &error).await.unwrap())
                }
            },
            RetentionMode::RemoveAll => match result {
                Ok(task) => SomeValue::SomeNumber(self.queue.remove_task(task).await.unwrap()),
                Err((task, _error)) => {
                    SomeValue::SomeNumber(self.queue.remove_task(task).await.unwrap())
                }
            },
            RetentionMode::RemoveFinished => match result {
                Ok(task) => SomeValue::SomeNumber(self.queue.remove_task(task).await.unwrap()),
                Err((task, error)) => {
                    SomeValue::SomeTask(self.queue.fail_task(task, &error).await.unwrap())
                }
            },
        }
    }
    pub async fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        tokio::time::sleep(Duration::from_secs(self.sleep_params.sleep_period)).await;
    }
    pub async fn run_tasks(&mut self) -> Result<(), Error> {
        loop {
            match self
                .queue
                .fetch_and_touch_task(&self.task_type.clone())
                .await
            {
                Ok(Some(task)) => {
                    self.sleep_params.maybe_reset_sleep_period();
                    self.run(task).await;
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
}

#[cfg(test)]
mod async_worker_tests {
    use super::AsyncWorker;
    use crate::asynk::async_queue::AsyncQueueTest;
    use crate::asynk::async_queue::AsyncQueueable;
    use crate::asynk::async_queue::FangTaskState;
    use crate::asynk::AsyncRunnable;
    use crate::asynk::Error;
    use crate::RetentionMode;
    //use crate::SleepParams;
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
    #[async_trait(?Send)]
    impl AsyncRunnable for WorkerAsyncTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn execute_and_finishes_task() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction: Some(transaction),
        };

        let task = WorkerAsyncTask { number: 1 };
        let metadata = serde_json::to_value(&task as &dyn AsyncRunnable).unwrap();

        let task = test.insert_task(metadata, &task.task_type()).await.unwrap();
        let id = task.id;

        let mut worker = AsyncWorker::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .task_type(Some("common".to_string()))
            .retention_mode(RetentionMode::KeepAll)
            .build();
        let task_finished = worker.run(task).await._get_task();
        assert_eq!(id, task_finished.id);
        assert_eq!(FangTaskState::Finished, task_finished.state);
        test.transaction.unwrap().rollback().await.unwrap();
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
