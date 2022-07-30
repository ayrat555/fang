use crate::error::FangError;
use crate::queue::Queue;
use crate::queue::Task;
use crate::worker_pool::{SharedState, WorkerState};
use crate::{RetentionMode, SleepParams};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use log::error;
use std::thread;
use std::time::Duration;

pub struct Executor {
    pub pooled_connection: PooledConnection<ConnectionManager<PgConnection>>,
    pub task_type: Option<String>,
    pub sleep_params: SleepParams,
    pub retention_mode: RetentionMode,
    shared_state: Option<SharedState>,
}

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

#[typetag::serde(tag = "type")]
pub trait Runnable {
    fn run(&self, connection: &PgConnection) -> Result<(), Error>;

    fn task_type(&self) -> String {
        "common".to_string()
    }
}

impl Executor {
    pub fn new(pooled_connection: PooledConnection<ConnectionManager<PgConnection>>) -> Self {
        Self {
            pooled_connection,
            sleep_params: SleepParams::default(),
            retention_mode: RetentionMode::RemoveFinished,
            task_type: None,
            shared_state: None,
        }
    }

    pub fn set_shared_state(&mut self, shared_state: SharedState) {
        self.shared_state = Some(shared_state);
    }

    pub fn set_task_type(&mut self, task_type: String) {
        self.task_type = Some(task_type);
    }

    pub fn set_sleep_params(&mut self, sleep_params: SleepParams) {
        self.sleep_params = sleep_params;
    }

    pub fn set_retention_mode(&mut self, retention_mode: RetentionMode) {
        self.retention_mode = retention_mode;
    }

    pub fn run(&self, task: Task) {
        let result = self.execute_task(task);
        self.finalize_task(result)
    }

    pub fn run_tasks(&mut self) -> Result<(), FangError> {
        loop {
            if let Some(ref shared_state) = self.shared_state {
                let shared_state = shared_state.read()?;
                if let WorkerState::Shutdown = *shared_state {
                    return Ok(());
                }
            }

            match Queue::fetch_and_touch_query(&self.pooled_connection, &self.task_type.clone()) {
                Ok(Some(task)) => {
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

    pub fn maybe_reset_sleep_period(&mut self) {
        self.sleep_params.maybe_reset_sleep_period();
    }

    pub fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        thread::sleep(Duration::from_secs(self.sleep_params.sleep_period));
    }

    fn execute_task(&self, task: Task) -> Result<Task, (Task, String)> {
        let actual_task: Box<dyn Runnable> = serde_json::from_value(task.metadata.clone()).unwrap();
        let task_result = actual_task.run(&self.pooled_connection);

        match task_result {
            Ok(()) => Ok(task),
            Err(error) => Err((task, error.description)),
        }
    }

    fn finalize_task(&self, result: Result<Task, (Task, String)>) {
        match self.retention_mode {
            RetentionMode::KeepAll => {
                match result {
                    Ok(task) => Queue::finish_task_query(&self.pooled_connection, &task).unwrap(),
                    Err((task, error)) => {
                        Queue::fail_task_query(&self.pooled_connection, &task, error).unwrap()
                    }
                };
            }
            RetentionMode::RemoveAll => {
                match result {
                    Ok(task) => Queue::remove_task_query(&self.pooled_connection, task.id).unwrap(),
                    Err((task, _error)) => {
                        Queue::remove_task_query(&self.pooled_connection, task.id).unwrap()
                    }
                };
            }
            RetentionMode::RemoveFinished => match result {
                Ok(task) => {
                    Queue::remove_task_query(&self.pooled_connection, task.id).unwrap();
                }
                Err((task, error)) => {
                    Queue::fail_task_query(&self.pooled_connection, &task, error).unwrap();
                }
            },
        }
    }
}

#[cfg(test)]
mod executor_tests {
    use super::Error;
    use super::Executor;
    use super::RetentionMode;
    use super::Runnable;
    use crate::queue::Queue;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use crate::NewTask;
    use diesel::connection::Connection;
    use diesel::pg::PgConnection;
    use diesel::r2d2::{ConnectionManager, PooledConnection};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct ExecutorTaskTest {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for ExecutorTaskTest {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
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
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
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
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
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
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
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
        let job = ExecutorTaskTest { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
            task_type: "common".to_string(),
        };

        let mut executor = Executor::new(pooled_connection());
        executor.set_retention_mode(RetentionMode::KeepAll);

        executor
            .pooled_connection
            .test_transaction::<(), Error, _>(|| {
                let task = Queue::insert_query(&executor.pooled_connection, &new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(task.clone());

                let found_task =
                    Queue::find_task_by_id_query(&executor.pooled_connection, task.id).unwrap();

                assert_eq!(FangTaskState::Finished, found_task.state);

                Ok(())
            });
    }

    #[test]
    #[ignore]
    fn executes_task_only_of_specific_type() {
        let task1 = TaskType1 {};
        let task2 = TaskType2 {};

        let new_task1 = NewTask {
            metadata: serialize(&task1),
            task_type: "type1".to_string(),
        };

        let new_task2 = NewTask {
            metadata: serialize(&task2),
            task_type: "type2".to_string(),
        };

        let executor = Executor::new(pooled_connection());

        let task1 = Queue::insert_query(&executor.pooled_connection, &new_task1).unwrap();
        let task2 = Queue::insert_query(&executor.pooled_connection, &new_task2).unwrap();

        assert_eq!(FangTaskState::New, task1.state);
        assert_eq!(FangTaskState::New, task2.state);

        std::thread::spawn(move || {
            let mut executor = Executor::new(pooled_connection());
            executor.set_retention_mode(RetentionMode::KeepAll);
            executor.set_task_type("type1".to_string());

            executor.run_tasks().unwrap();
        });

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task1 =
            Queue::find_task_by_id_query(&executor.pooled_connection, task1.id).unwrap();
        assert_eq!(FangTaskState::Finished, found_task1.state);

        let found_task2 =
            Queue::find_task_by_id_query(&executor.pooled_connection, task2.id).unwrap();
        assert_eq!(FangTaskState::New, found_task2.state);
    }

    #[test]
    fn saves_error_for_failed_task() {
        let task = FailedTask { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&task),
            task_type: "common".to_string(),
        };

        let executor = Executor::new(pooled_connection());

        executor
            .pooled_connection
            .test_transaction::<(), Error, _>(|| {
                let task = Queue::insert_query(&executor.pooled_connection, &new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(task.clone());

                let found_task =
                    Queue::find_task_by_id_query(&executor.pooled_connection, task.id).unwrap();

                assert_eq!(FangTaskState::Failed, found_task.state);
                assert_eq!(
                    "the number is 10".to_string(),
                    found_task.error_message.unwrap()
                );

                Ok(())
            });
    }

    fn pooled_connection() -> PooledConnection<ConnectionManager<PgConnection>> {
        Queue::connection_pool(5).get().unwrap()
    }
}
