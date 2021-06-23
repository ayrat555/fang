use crate::postgres::Postgres;
use crate::postgres::Task;
use std::thread;
use std::time::Duration;

pub struct Executor {
    pub storage: Postgres,
    pub sleep_period: u64,
    pub max_sleep_period: u64,
    pub min_sleep_period: u64,
    pub sleep_step: u64,
}

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

#[typetag::serde(tag = "type")]
pub trait Runnable {
    fn run(&self) -> Result<(), Error>;
}

impl Executor {
    pub fn new(storage: Postgres) -> Self {
        Self {
            storage,
            sleep_period: 5,
            max_sleep_period: 15,
            min_sleep_period: 5,
            sleep_step: 5,
        }
    }

    pub fn run(&self, task: &Task) {
        let actual_task: Box<dyn Runnable> = serde_json::from_value(task.metadata.clone()).unwrap();

        match actual_task.run() {
            Ok(()) => self.storage.finish_task(task).unwrap(),
            Err(error) => self.storage.fail_task(task, error.description).unwrap(),
        };
    }

    pub fn run_tasks(&mut self) {
        match self.storage.fetch_and_touch() {
            Ok(Some(task)) => {
                self.maybe_reset_sleep_period();
                self.run(&task);
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

    pub fn maybe_reset_sleep_period(&mut self) {
        if self.sleep_period != self.min_sleep_period {
            self.sleep_period = self.min_sleep_period;
        }
    }

    pub fn sleep(&mut self) {
        if self.sleep_period < self.max_sleep_period {
            self.sleep_period = self.sleep_period + self.sleep_step;
        }

        thread::sleep(Duration::from_secs(self.sleep_period));
    }
}

#[cfg(test)]
mod executor_tests {
    use super::Error;
    use super::Executor;
    use super::Runnable;
    use crate::postgres::NewTask;
    use crate::postgres::Postgres;
    use crate::schema::FangTaskState;
    use diesel::connection::Connection;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Job {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for Job {
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

    pub fn serialize(job: &dyn Runnable) -> serde_json::Value {
        serde_json::to_value(job).unwrap()
    }

    #[test]
    fn executes_and_finishes_task() {
        let job = Job { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
        };

        let executor = Executor::new(Postgres::new(None));

        executor
            .storage
            .connection
            .test_transaction::<(), Error, _>(|| {
                let task = executor.storage.insert(&new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(&task);

                let found_task = executor.storage.find_task_by_id(task.id).unwrap();

                assert_eq!(FangTaskState::Finished, found_task.state);

                Ok(())
            });
    }

    #[test]
    fn saves_error_for_failed_task() {
        let job = FailedJob { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
        };

        let executor = Executor::new(Postgres::new(None));

        executor
            .storage
            .connection
            .test_transaction::<(), Error, _>(|| {
                let task = executor.storage.insert(&new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(&task);

                let found_task = executor.storage.find_task_by_id(task.id).unwrap();

                assert_eq!(FangTaskState::Failed, found_task.state);
                assert_eq!(
                    "the number is 10".to_string(),
                    found_task.error_message.unwrap()
                );

                Ok(())
            });
    }
}
