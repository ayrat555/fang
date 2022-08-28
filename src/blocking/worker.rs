use crate::error::FangError;
use crate::queue::Queueable;
use crate::queue::Task;
use crate::runnable::Runnable;
use crate::runnable::COMMON_TYPE;
use crate::schema::FangTaskState;
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

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

impl<BQueue> Worker<BQueue>
where
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    pub fn run(&self, task: Task) {
        let result = self.execute_task(task);
        self.finalize_task(result)
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

    fn execute_task(&self, task: Task) -> Result<Task, (Task, String)> {
        let actual_task: Box<dyn Runnable> = serde_json::from_value(task.metadata.clone()).unwrap();
        let task_result = actual_task.run(&self.queue);

        match task_result {
            Ok(()) => Ok(task),
            Err(error) => Err((task, error.description)),
        }
    }

    fn finalize_task(&self, result: Result<Task, (Task, String)>) {
        match self.retention_mode {
            RetentionMode::KeepAll => {
                match result {
                    Ok(task) => self
                        .queue
                        .update_task_state(&task, FangTaskState::Finished)
                        .unwrap(),
                    Err((task, error)) => self.queue.fail_task(&task, error).unwrap(),
                };
            }
            RetentionMode::RemoveAll => {
                match result {
                    Ok(task) => self.queue.remove_task(task.id).unwrap(),
                    Err((task, _error)) => self.queue.remove_task(task.id).unwrap(),
                };
            }
            RetentionMode::RemoveFinished => match result {
                Ok(task) => {
                    self.queue.remove_task(task.id).unwrap();
                }
                Err((task, error)) => {
                    self.queue.fail_task(&task, error).unwrap();
                }
            },
        }
    }
}

#[cfg(test)]
mod executor_tests {
    use super::Error;
    use super::RetentionMode;
    use super::Runnable;
    use super::Worker;
    use crate::queue::Queue;
    use crate::queue::Queueable;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use crate::NewTask;
    use chrono::Utc;
    use diesel::connection::Connection;
    use diesel::pg::PgConnection;
    use diesel::r2d2::{ConnectionManager, PooledConnection};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct WorkerTaskTest {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for WorkerTaskTest {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), Error> {
            println!("the number is {}", self.number);

            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct FailedTask {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for FailedTask {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), Error> {
            let message = format!("the number is {}", self.number);

            Err(Error {
                description: message,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct TaskType1 {}

    #[typetag::serde]
    impl Runnable for TaskType1 {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), Error> {
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
        fn run(&self, _queue: &dyn Queueable) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type2".to_string()
        }
    }

    pub fn serialize(job: &dyn Runnable) -> serde_json::Value {
        serde_json::to_value(job).unwrap()
    }

    #[test]
    fn executes_and_finishes_task() {
        let task = WorkerTaskTest { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let worker = Worker::<Queue>::builder()
            .queue(queue)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        let worker_pooled_connection = pooled_connection();
        let pooled_connection = pooled_connection();

        worker_pooled_connection.test_transaction::<(), Error, _>(|| {
            let task = Queue::insert_query(&pooled_connection, &task, Utc::now()).unwrap();

            assert_eq!(FangTaskState::New, task.state);

            worker.run(task.clone());

            let found_task = Queue::find_task_by_id_query(&pooled_connection, task.id).unwrap();

            assert_eq!(FangTaskState::Finished, found_task.state);

            Ok(())
        });
    }

    #[test]
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

        let worker_pooled_connection = pooled_connection();
        let pooled_connection = pooled_connection();

        worker_pooled_connection.test_transaction::<(), Error, _>(|| {
            let task1 = Queue::insert_query(&pooled_connection, &task1, Utc::now()).unwrap();
            let task2 = Queue::insert_query(&pooled_connection, &task2, Utc::now()).unwrap();

            assert_eq!(FangTaskState::New, task1.state);
            assert_eq!(FangTaskState::New, task2.state);

            worker.run_tasks_until_none().unwrap();

            std::thread::sleep(std::time::Duration::from_millis(1000));

            let found_task1 = Queue::find_task_by_id_query(&pooled_connection, task1.id).unwrap();
            assert_eq!(FangTaskState::Finished, found_task1.state);

            let found_task2 = Queue::find_task_by_id_query(&pooled_connection, task2.id).unwrap();
            assert_eq!(FangTaskState::New, found_task2.state);

            Ok(())
        });
    }

    #[test]
    fn saves_error_for_failed_task() {
        let task = FailedTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let worker = Worker::<Queue>::builder()
            .queue(queue)
            .retention_mode(RetentionMode::KeepAll)
            .build();

        let worker_pooled_connection = pooled_connection();
        let pooled_connection = pooled_connection();

        worker_pooled_connection.test_transaction::<(), Error, _>(|| {
            let task = Queue::insert_query(&pooled_connection, &task, Utc::now()).unwrap();

            assert_eq!(FangTaskState::New, task.state);

            worker.run(task.clone());

            let found_task = Queue::find_task_by_id_query(&pooled_connection, task.id).unwrap();

            assert_eq!(FangTaskState::Failed, found_task.state);
            assert_eq!(
                "the number is 10".to_string(),
                found_task.error_message.unwrap()
            );

            Ok(())
        });
    }

    fn pooled_connection() -> PooledConnection<ConnectionManager<PgConnection>> {
        Queue::connection_pool(1).get().unwrap()
    }
}
