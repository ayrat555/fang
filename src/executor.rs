use crate::postgres::Postgres;
use crate::postgres::Task;

pub struct Executor {
    pub storage: Postgres,
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
        Self { storage }
    }

    pub fn run(&self, task: &Task) {
        let actual_task: Box<dyn Runnable> = serde_json::from_value(task.metadata.clone()).unwrap();

        match actual_task.run() {
            Ok(()) => self.storage.finish_task(task).unwrap(),
            Err(error) => self.storage.fail_task(task, error.description).unwrap(),
        };
    }

    pub fn run_tasks(&self) {
        while let Ok(Some(task)) = self.storage.fetch_and_touch() {
            self.run(&task)
        }
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
