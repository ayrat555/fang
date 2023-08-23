use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::DEFAULT_TASK_TYPE;
use crate::asynk::async_runnable::AsyncRunnable;
use crate::FangError;
use crate::FangTaskState;
use crate::Scheduled::*;
use crate::Task;
use crate::{RetentionMode, SleepParams};
use log::error;
use typed_builder::TypedBuilder;

/// it executes tasks only of task_type type, it sleeps when there are no tasks in the queue
#[derive(TypedBuilder)]
pub struct AsyncWorker<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    #[builder(setter(into))]
    pub queue: AQueue,
    #[builder(default=DEFAULT_TASK_TYPE.to_string(), setter(into))]
    pub task_type: String,
    #[builder(default, setter(into))]
    pub sleep_params: SleepParams,
    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,
}

impl<AQueue> AsyncWorker<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    async fn run(&mut self, task: &Task, runnable: &dyn AsyncRunnable) -> Result<(), FangError> {
        let result = runnable.run(&mut self.queue).await;

        match result {
            Ok(_) => self.finalize_task(task, &result).await?,

            Err(ref error) => {
                if task.retries < runnable.max_retries() {
                    let backoff_seconds = runnable.backoff(task.retries as u32);

                    self.queue
                        .schedule_retry(task, backoff_seconds, &error.description)
                        .await?;
                } else {
                    self.finalize_task(task, &result).await?;
                }
            }
        }

        Ok(())
    }

    async fn finalize_task(
        &mut self,
        task: &Task,
        result: &Result<(), FangError>,
    ) -> Result<(), FangError> {
        match self.retention_mode {
            RetentionMode::KeepAll => match result {
                Ok(_) => {
                    self.queue
                        .update_task_state(task, FangTaskState::Finished)
                        .await?;
                }
                Err(error) => {
                    self.queue.fail_task(task, &error.description).await?;
                }
            },
            RetentionMode::RemoveAll => {
                self.queue.remove_task(task.id).await?;
            }
            RetentionMode::RemoveFinished => match result {
                Ok(_) => {
                    self.queue.remove_task(task.id).await?;
                }
                Err(error) => {
                    self.queue.fail_task(task, &error.description).await?;
                }
            },
        };

        Ok(())
    }

    async fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        tokio::time::sleep(self.sleep_params.sleep_period).await;
    }

    pub(crate) async fn run_tasks(&mut self) -> Result<(), FangError> {
        loop {
            //fetch task
            match self
                .queue
                .fetch_and_touch_task(Some(self.task_type.clone()))
                .await
            {
                Ok(Some(task)) => {
                    let actual_task: Box<dyn AsyncRunnable> =
                        serde_json::from_value(task.metadata.clone()).unwrap();

                    let cron = actual_task.cron();

                    self.sleep_params.maybe_reset_sleep_period();

                    // run scheduled task
                    self.run(&task, &*actual_task).await?;

                    // check if task is scheduled or not
                    if let Some(CronPattern(_)) = cron {
                        // program task
                        self.queue.schedule_task(&*actual_task).await?;
                    }
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
#[derive(TypedBuilder)]
pub struct AsyncWorkerTest<'a> {
    #[builder(setter(into))]
    pub queue: &'a mut dyn AsyncQueueable,
    #[builder(default=DEFAULT_TASK_TYPE.to_string(), setter(into))]
    pub task_type: String,
    #[builder(default, setter(into))]
    pub sleep_params: SleepParams,
    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,
}

#[cfg(test)]
impl<'a> AsyncWorkerTest<'a> {
    pub async fn run(
        &mut self,
        task: &Task,
        runnable: &dyn AsyncRunnable,
    ) -> Result<(), FangError> {
        let result = runnable.run(self.queue).await;

        match result {
            Ok(_) => self.finalize_task(task, &result).await?,

            Err(ref error) => {
                if task.retries < runnable.max_retries() {
                    let backoff_seconds = runnable.backoff(task.retries as u32);

                    self.queue
                        .schedule_retry(task, backoff_seconds, &error.description)
                        .await?;
                } else {
                    self.finalize_task(task, &result).await?;
                }
            }
        }

        Ok(())
    }

    async fn finalize_task(
        &mut self,
        task: &Task,
        result: &Result<(), FangError>,
    ) -> Result<(), FangError> {
        match self.retention_mode {
            RetentionMode::KeepAll => match result {
                Ok(_) => {
                    self.queue
                        .update_task_state(task, FangTaskState::Finished)
                        .await?;
                }
                Err(error) => {
                    self.queue.fail_task(task, &error.description).await?;
                }
            },
            RetentionMode::RemoveAll => match result {
                Ok(_) => {
                    self.queue.remove_task(task.id).await?;
                }
                Err(_error) => {
                    self.queue.remove_task(task.id).await?;
                }
            },
            RetentionMode::RemoveFinished => match result {
                Ok(_) => {
                    self.queue.remove_task(task.id).await?;
                }
                Err(error) => {
                    self.queue.fail_task(task, &error.description).await?;
                }
            },
        };

        Ok(())
    }

    pub async fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        tokio::time::sleep(self.sleep_params.sleep_period).await;
    }

    pub async fn run_tasks_until_none(&mut self) -> Result<u32, FangError> {
        let mut number_runned = 0u32;
        loop {
            match self
                .queue
                .fetch_and_touch_task(Some(self.task_type.clone()))
                .await
            {
                Ok(Some(task)) => {
                    let actual_task: Box<dyn AsyncRunnable> =
                        serde_json::from_value(task.metadata.clone()).unwrap();

                    self.sleep_params.maybe_reset_sleep_period();
                    // run scheduled task
                    self.run(&task, &*actual_task).await?;
                    number_runned += 1;

                    // check if task is scheduled or not
                    if let Some(CronPattern(_)) = actual_task.cron() {
                        // program task
                        self.queue.schedule_task(&*actual_task).await?;
                    }
                }
                Ok(None) => {
                    return Ok(number_runned);
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
    use super::AsyncWorkerTest;
    use crate::asynk::async_queue::AsyncQueue;
    use crate::asynk::async_queue::AsyncQueueable;
    use crate::asynk::async_worker::Task;
    use crate::asynk::AsyncRunnable;
    use crate::FangError;
    use crate::FangTaskState;
    use crate::RetentionMode;
    use crate::Scheduled;
    use async_trait::async_trait;
    use bb8_postgres::tokio_postgres::NoTls;
    use chrono::Duration;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for WorkerAsyncTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTaskSchedule {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for WorkerAsyncTaskSchedule {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
            Ok(())
        }
        fn cron(&self) -> Option<Scheduled> {
            Some(Scheduled::ScheduleOnce(Utc::now() + Duration::seconds(1)))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTaskScheduled {}

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for WorkerAsyncTaskScheduled {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
            log::info!("WorkerAsyncTaskScheduled has been run");
            tokio::time::sleep(std::time::Duration::from_millis(2050)).await;
            Ok(())
        }
        fn cron(&self) -> Option<Scheduled> {
            let cron = "0/1 * * * * * *".to_string();
            Some(Scheduled::CronPattern(cron))
        }

        fn task_type(&self) -> String {
            "type1".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncFailedTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncFailedTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
            let message = format!("number {} is wrong :(", self.number);

            Err(FangError {
                description: message,
            })
        }

        fn max_retries(&self) -> i32 {
            0
        }
    }

    #[derive(Serialize, Deserialize, Clone)]
    struct AsyncRetryTask {}

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncRetryTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
            let message = "Failed".to_string();

            Err(FangError {
                description: message,
            })
        }

        fn max_retries(&self) -> i32 {
            2
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType1 {}

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncTaskType1 {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
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
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type2".to_string()
        }
    }

    #[tokio::test]
    async fn execute_and_finishes_task() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let actual_task = WorkerAsyncTask { number: 1 };

        let task = insert_task(&mut test, &actual_task).await;
        let id = task.id;

        let mut worker = AsyncWorkerTest::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run(&task, &actual_task).await.unwrap();
        let task_finished = test.find_task_by_id(id).await.unwrap();
        assert_eq!(id, task_finished.id);
        assert_eq!(FangTaskState::Finished, task_finished.state);
    }

    #[tokio::test]
    async fn schedule_task_test() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let actual_task = WorkerAsyncTaskSchedule { number: 1 };

        let task = test.schedule_task(&actual_task).await.unwrap();

        let id = task.id;

        let mut worker = AsyncWorkerTest::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run_tasks_until_none().await.unwrap();

        let task = worker.queue.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task.id);
        assert_eq!(FangTaskState::New, task.state);

        tokio::time::sleep(core::time::Duration::from_secs(3)).await;

        worker.run_tasks_until_none().await.unwrap();

        let task = test.find_task_by_id(id).await.unwrap();
        assert_eq!(id, task.id);
        assert_eq!(FangTaskState::Finished, task.state);
    }

    #[tokio::test]
    async fn retries_task_test() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let actual_task = AsyncRetryTask {};

        let task = test.insert_task(&actual_task).await.unwrap();

        let id = task.id;

        let mut worker = AsyncWorkerTest::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run_tasks_until_none().await.unwrap();

        let task = worker.queue.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task.id);
        assert_eq!(FangTaskState::Retried, task.state);
        assert_eq!(1, task.retries);

        tokio::time::sleep(core::time::Duration::from_secs(5)).await;
        worker.run_tasks_until_none().await.unwrap();

        let task = worker.queue.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task.id);
        assert_eq!(FangTaskState::Retried, task.state);
        assert_eq!(2, task.retries);

        tokio::time::sleep(core::time::Duration::from_secs(10)).await;
        worker.run_tasks_until_none().await.unwrap();

        let task = test.find_task_by_id(id).await.unwrap();
        assert_eq!(id, task.id);
        assert_eq!(FangTaskState::Failed, task.state);
        assert_eq!("Failed".to_string(), task.error_message.unwrap());
    }

    #[tokio::test]
    async fn saves_error_for_failed_task() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let failed_task = AsyncFailedTask { number: 1 };
        let task = insert_task(&mut test, &failed_task).await;
        let id = task.id;

        let mut worker = AsyncWorkerTest::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run(&task, &failed_task).await.unwrap();
        let task_finished = test.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task_finished.id);
        assert_eq!(FangTaskState::Failed, task_finished.state);
        assert_eq!(
            "number 1 is wrong :(".to_string(),
            task_finished.error_message.unwrap()
        );
    }

    #[tokio::test]
    async fn executes_task_only_of_specific_type() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let task1 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task12 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task2 = insert_task(&mut test, &AsyncTaskType2 {}).await;

        let id1 = task1.id;
        let id12 = task12.id;
        let id2 = task2.id;

        let mut worker = AsyncWorkerTest::builder()
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
    }

    #[tokio::test]
    async fn remove_when_finished() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let task1 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task12 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task2 = insert_task(&mut test, &AsyncTaskType2 {}).await;

        let _id1 = task1.id;
        let _id12 = task12.id;
        let id2 = task2.id;

        let mut worker = AsyncWorkerTest::builder()
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
    }

    async fn insert_task(test: &mut AsyncQueue<NoTls>, task: &dyn AsyncRunnable) -> Task {
        test.insert_task(task).await.unwrap()
    }

    #[tokio::test()]
    async fn no_schedule_until_run() {
        let mut test = AsyncQueue::<NoTls>::test().await;

        let _task_1 = test
            .schedule_task(&WorkerAsyncTaskScheduled {})
            .await
            .unwrap();

        let _id1 = _task_1.id;

        tokio::time::sleep(std::time::Duration::from_millis(1020)).await;

        let mut worker = AsyncWorkerTest::builder()
            .queue(&mut test as &mut dyn AsyncQueueable)
            .task_type("type1".to_string())
            .build();

        let _n = worker.run_tasks_until_none().await.unwrap();

        //assert_eq!(n, 1u32);

        let task = test
            .fetch_and_touch_task(Some("type1".to_string()))
            .await
            .unwrap();
        assert_eq!(None, task);
    }
}
