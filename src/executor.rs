use crate::postgres::Postgres;
use crate::postgres::Task;
use std::panic;
use std::panic::RefUnwindSafe;
use std::thread;
use std::time::Duration;

pub struct Executor {
    pub storage: Postgres,
    pub task_type: Option<String>,
    pub sleep_params: SleepParams,
    pub retention_mode: RetentionMode,
}

#[derive(Clone)]
pub enum RetentionMode {
    KeepAll,
    RemoveAll,
    RemoveFinished,
}

#[derive(Clone)]
pub struct SleepParams {
    pub sleep_period: u64,
    pub max_sleep_period: u64,
    pub min_sleep_period: u64,
    pub sleep_step: u64,
}

impl SleepParams {
    pub fn maybe_reset_sleep_period(&mut self) {
        if self.sleep_period != self.min_sleep_period {
            self.sleep_period = self.min_sleep_period;
        }
    }

    pub fn maybe_increase_sleep_period(&mut self) {
        if self.sleep_period < self.max_sleep_period {
            self.sleep_period += self.sleep_step;
        }
    }
}

impl Default for SleepParams {
    fn default() -> Self {
        SleepParams {
            sleep_period: 5,
            max_sleep_period: 15,
            min_sleep_period: 5,
            sleep_step: 5,
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

#[typetag::serde(tag = "type")]
pub trait Runnable
where
    Self: RefUnwindSafe,
{
    fn run(&self) -> Result<(), Error>;

    fn task_type(&self) -> String {
        "common".to_string()
    }
}

impl Executor {
    pub fn new(storage: Postgres) -> Self {
        Self {
            storage,
            sleep_params: SleepParams::default(),
            retention_mode: RetentionMode::RemoveFinished,
            task_type: None,
        }
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

    pub fn run_tasks(&mut self) {
        loop {
            match self.storage.fetch_and_touch(&self.task_type.clone()) {
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
        let task_result = panic::catch_unwind(|| actual_task.run());

        match task_result {
            Ok(result) => match result {
                Ok(()) => Ok(task),
                Err(error) => Err((task, error.description)),
            },

            Err(error) => {
                let message = format!("panicked during task execution {:?}", error);

                Err((task, message))
            }
        }
    }

    fn finalize_task(&self, result: Result<Task, (Task, String)>) {
        match self.retention_mode {
            RetentionMode::KeepAll => {
                match result {
                    Ok(task) => self.storage.finish_task(&task).unwrap(),
                    Err((task, error)) => self.storage.fail_task(&task, error).unwrap(),
                };

                ()
            }
            RetentionMode::RemoveAll => {
                match result {
                    Ok(task) => self.storage.remove_task(task.id).unwrap(),
                    Err((task, _error)) => self.storage.remove_task(task.id).unwrap(),
                };

                ()
            }
            RetentionMode::RemoveFinished => match result {
                Ok(task) => {
                    self.storage.remove_task(task.id).unwrap();
                    ()
                }
                Err((task, error)) => {
                    self.storage.fail_task(&task, error).unwrap();
                    ()
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
    use crate::postgres::NewTask;
    use crate::postgres::Postgres;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use crate::{Deserialize, Serialize};
    use diesel::connection::Connection;

    #[derive(Serialize, Deserialize)]
    struct ExecutorJobTest {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for ExecutorJobTest {
        fn run(&self) -> Result<(), Error> {
            println!("the number is {}", self.number);

            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct FailedJob {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for FailedJob {
        fn run(&self) -> Result<(), Error> {
            let message = format!("the number is {}", self.number);

            Err(Error {
                description: message,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct PanicJob {}

    #[typetag::serde]
    impl Runnable for PanicJob {
        fn run(&self) -> Result<(), Error> {
            if true {
                panic!("panic!");
            }

            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct JobType1 {}

    #[typetag::serde]
    impl Runnable for JobType1 {
        fn run(&self) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type1".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct JobType2 {}

    #[typetag::serde]
    impl Runnable for JobType2 {
        fn run(&self) -> Result<(), Error> {
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
        let job = ExecutorJobTest { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
            task_type: "common".to_string(),
        };

        let mut executor = Executor::new(Postgres::new());
        executor.set_retention_mode(RetentionMode::KeepAll);

        executor
            .storage
            .connection
            .test_transaction::<(), Error, _>(|| {
                let task = executor.storage.insert(&new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(task.clone());

                let found_task = executor.storage.find_task_by_id(task.id).unwrap();

                assert_eq!(FangTaskState::Finished, found_task.state);

                Ok(())
            });
    }

    #[test]
    #[ignore]
    fn executes_task_only_of_specific_type() {
        let job1 = JobType1 {};
        let job2 = JobType2 {};

        let new_task1 = NewTask {
            metadata: serialize(&job1),
            task_type: "type1".to_string(),
        };

        let new_task2 = NewTask {
            metadata: serialize(&job2),
            task_type: "type2".to_string(),
        };

        let executor = Executor::new(Postgres::new());

        let task1 = executor.storage.insert(&new_task1).unwrap();
        let task2 = executor.storage.insert(&new_task2).unwrap();

        assert_eq!(FangTaskState::New, task1.state);
        assert_eq!(FangTaskState::New, task2.state);

        std::thread::spawn(move || {
            let postgres = Postgres::new();
            let mut executor = Executor::new(postgres);
            executor.set_retention_mode(RetentionMode::KeepAll);
            executor.set_task_type("type1".to_string());

            executor.run_tasks();
        });

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task1 = executor.storage.find_task_by_id(task1.id).unwrap();
        assert_eq!(FangTaskState::Finished, found_task1.state);

        let found_task2 = executor.storage.find_task_by_id(task2.id).unwrap();
        assert_eq!(FangTaskState::New, found_task2.state);
    }

    #[test]
    fn saves_error_for_failed_task() {
        let job = FailedJob { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
            task_type: "common".to_string(),
        };

        let executor = Executor::new(Postgres::new());

        executor
            .storage
            .connection
            .test_transaction::<(), Error, _>(|| {
                let task = executor.storage.insert(&new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(task.clone());

                let found_task = executor.storage.find_task_by_id(task.id).unwrap();

                assert_eq!(FangTaskState::Failed, found_task.state);
                assert_eq!(
                    "the number is 10".to_string(),
                    found_task.error_message.unwrap()
                );

                Ok(())
            });
    }

    #[test]
    fn recovers_from_panics() {
        let job = PanicJob {};

        let new_task = NewTask {
            metadata: serialize(&job),
            task_type: "common".to_string(),
        };

        let executor = Executor::new(Postgres::new());

        executor
            .storage
            .connection
            .test_transaction::<(), Error, _>(|| {
                let task = executor.storage.insert(&new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(task.clone());

                let found_task = executor.storage.find_task_by_id(task.id).unwrap();

                assert_eq!(FangTaskState::Failed, found_task.state);
                assert_eq!(
                    "panicked during task execution Any".to_string(),
                    found_task.error_message.unwrap()
                );

                Ok(())
            });
    }
}
