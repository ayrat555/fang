#![allow(clippy::borrowed_box)]
#![allow(clippy::unnecessary_unwrap)]

use crate::fang_task_state::FangTaskState;
use crate::queue::Queueable;
use crate::queue::Task;
use crate::runnable::Runnable;
use crate::runnable::COMMON_TYPE;
use crate::FangError;
use crate::Scheduled::*;
use crate::{RetentionMode, SleepParams};
use log::error;
use std::thread;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct Worker<BQueue>
where
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    #[builder(setter(into))]
    pub queue: BQueue,
    #[builder(default=COMMON_TYPE.to_string(), setter(into))]
    pub task_type: String,
    #[builder(default, setter(into))]
    pub sleep_params: SleepParams,
    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,
}

impl<BQueue> Worker<BQueue>
where
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    pub fn run(&self, task: Task) {
        let runnable: Box<dyn Runnable> = serde_json::from_value(task.metadata.clone()).unwrap();
        let result = self.execute_task(&runnable);

        if result.is_err() && task.retries < runnable.max_retries() {
            let backoff_seconds = runnable.backoff(task.retries as u32);

            self.queue
                .schedule_retry(&task, backoff_seconds, result.unwrap_err())
                .expect("Failed to retry");
        } else {
            self.finalize_task(task, result);
        }
    }

    pub fn run_tasks(&mut self) -> Result<(), FangError> {
        loop {
            match self.queue.fetch_and_touch_task(self.task_type.clone()) {
                Ok(Some(task)) => {
                    let actual_task: Box<dyn Runnable> =
                        serde_json::from_value(task.metadata.clone()).unwrap();

                    // check if task is scheduled or not
                    if let Some(CronPattern(_)) = actual_task.cron() {
                        // program task
                        self.queue.schedule_task(&*actual_task)?;
                    }

                    self.maybe_reset_sleep_period();
                    self.run(task);
                }
                Ok(None) => {
                    self.sleep();
                }

                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);

                    self.sleep();
                }
            };
        }
    }

    #[cfg(test)]
    pub fn run_tasks_until_none(&mut self) -> Result<(), FangError> {
        loop {
            match self.queue.fetch_and_touch_task(self.task_type.clone()) {
                Ok(Some(task)) => {
                    let actual_task: Box<dyn Runnable> =
                        serde_json::from_value(task.metadata.clone()).unwrap();

                    // check if task is scheduled or not
                    if let Some(CronPattern(_)) = actual_task.cron() {
                        // program task
                        self.queue.schedule_task(&*actual_task)?;
                    }

                    self.maybe_reset_sleep_period();
                    self.run(task);
                }
                Ok(None) => {
                    return Ok(());
                }
                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);

                    self.sleep();
                }
            };
        }
    }

    pub fn maybe_reset_sleep_period(&mut self) {
        self.sleep_params.maybe_reset_sleep_period();
    }

    pub fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        thread::sleep(self.sleep_params.sleep_period);
    }

    fn execute_task(&self, runnable: &Box<dyn Runnable>) -> Result<(), String> {
        let task_result = runnable.run(&self.queue);

        match task_result {
            Ok(()) => Ok(()),
            Err(error) => Err(error.description),
        }
    }

    fn finalize_task(&self, task: Task, result: Result<(), String>) {
        match self.retention_mode {
            RetentionMode::KeepAll => {
                match result {
                    Ok(_) => self
                        .queue
                        .update_task_state(&task, FangTaskState::Finished)
                        .unwrap(),
                    Err(error) => self.queue.fail_task(&task, error).unwrap(),
                };
            }
            RetentionMode::RemoveAll => {
                match result {
                    Ok(_) => self.queue.remove_task(task.id).unwrap(),
                    Err(_error) => self.queue.remove_task(task.id).unwrap(),
                };
            }
            RetentionMode::RemoveFinished => match result {
                Ok(_) => {
                    self.queue.remove_task(task.id).unwrap();
                }
                Err(error) => {
                    self.queue.fail_task(&task, error).unwrap();
                }
            },
        }
    }
}

#[cfg(test)]
mod worker_tests {
    use super::RetentionMode;
    use super::Runnable;
    use super::Worker;
    use crate::fang_task_state::FangTaskState;
    use crate::queue::Queue;
    use crate::queue::Queueable;
    use crate::typetag;
    use crate::FangError;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct WorkerTaskTest {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for WorkerTaskTest {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            println!("the number is {}", self.number);

            Ok(())
        }

        fn task_type(&self) -> String {
            "worker_task".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct FailedTask {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for FailedTask {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            let message = format!("the number is {}", self.number);

            Err(FangError {
                description: message,
            })
        }

        fn max_retries(&self) -> i32 {
            0
        }

        fn task_type(&self) -> String {
            "F_task".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct TaskType1 {}

    #[typetag::serde]
    impl Runnable for TaskType1 {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type1".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct TaskType2 {}

    #[typetag::serde]
    impl Runnable for TaskType2 {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type2".to_string()
        }
    }

    // Worker tests has to commit because the worker operations commits
    #[test]
    #[ignore]
    fn executes_and_finishes_task() {
        let task = WorkerTaskTest { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let worker = Worker::<Queue>::builder()
            .queue(queue)
            .retention_mode(RetentionMode::KeepAll)
            .task_type(task.task_type())
            .build();
        let mut pooled_connection = worker.queue.connection_pool.get().unwrap();

        let task = Queue::insert_query(&mut pooled_connection, &task, Utc::now()).unwrap();

        assert_eq!(FangTaskState::New, task.state);

        // this operation commits and thats why need to commit this test
        worker.run(task.clone());

        let found_task = Queue::find_task_by_id_query(&mut pooled_connection, task.id).unwrap();

        assert_eq!(FangTaskState::Finished, found_task.state);

        Queue::remove_tasks_of_type_query(&mut pooled_connection, "worker_task").unwrap();
    }

    #[test]
    #[ignore]
    fn executes_task_only_of_specific_type() {
        let task1 = TaskType1 {};
        let task2 = TaskType2 {};

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut worker = Worker::<Queue>::builder()
            .queue(queue)
            .task_type(task1.task_type())
            .retention_mode(RetentionMode::KeepAll)
            .build();

        let mut pooled_connection = worker.queue.connection_pool.get().unwrap();

        let task1 = Queue::insert_query(&mut pooled_connection, &task1, Utc::now()).unwrap();
        let task2 = Queue::insert_query(&mut pooled_connection, &task2, Utc::now()).unwrap();

        assert_eq!(FangTaskState::New, task1.state);
        assert_eq!(FangTaskState::New, task2.state);

        worker.run_tasks_until_none().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task1 = Queue::find_task_by_id_query(&mut pooled_connection, task1.id).unwrap();
        assert_eq!(FangTaskState::Finished, found_task1.state);

        let found_task2 = Queue::find_task_by_id_query(&mut pooled_connection, task2.id).unwrap();
        assert_eq!(FangTaskState::New, found_task2.state);

        Queue::remove_tasks_of_type_query(&mut pooled_connection, "type1").unwrap();
        Queue::remove_tasks_of_type_query(&mut pooled_connection, "type2").unwrap();
    }

    #[test]
    #[ignore]
    fn saves_error_for_failed_task() {
        let task = FailedTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let worker = Worker::<Queue>::builder()
            .queue(queue)
            .retention_mode(RetentionMode::KeepAll)
            .task_type(task.task_type())
            .build();

        let mut pooled_connection = worker.queue.connection_pool.get().unwrap();

        let task = Queue::insert_query(&mut pooled_connection, &task, Utc::now()).unwrap();

        assert_eq!(FangTaskState::New, task.state);

        worker.run(task.clone());

        let found_task = Queue::find_task_by_id_query(&mut pooled_connection, task.id).unwrap();

        assert_eq!(FangTaskState::Failed, found_task.state);
        assert_eq!(
            "the number is 10".to_string(),
            found_task.error_message.unwrap()
        );

        Queue::remove_tasks_of_type_query(&mut pooled_connection, "F_task").unwrap();
    }
}
