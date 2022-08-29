use fang::runnable::Runnable;
use fang::serde::{Deserialize, Serialize};
use fang::typetag;
use fang::Error;
use fang::Queueable;
use std::thread;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyTask {
    pub number: u16,
    pub current_thread_name: String,
}

impl MyTask {
    pub fn new(number: u16) -> Self {
        let handle = thread::current();
        let current_thread_name = handle.name().unwrap().to_string();

        Self {
            number,
            current_thread_name,
        }
    }
}

#[typetag::serde]
impl Runnable for MyTask {
    fn run(&self, queue: &dyn Queueable) -> Result<(), Error> {
        thread::sleep(Duration::from_secs(3));

        let new_task = MyTask::new(self.number + 1);

        log::info!(
            "The number is {}, thread name {}",
            self.number,
            self.current_thread_name
        );

        queue.insert_task(&new_task).unwrap();

        Ok(())
    }

    fn task_type(&self) -> String {
        "worker_pool_test".to_string()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyFailingTask {
    pub number: u16,
    pub current_thread_name: String,
}

impl MyFailingTask {
    pub fn new(number: u16) -> Self {
        let handle = thread::current();
        let current_thread_name = handle.name().unwrap().to_string();
        Self {
            number,
            current_thread_name,
        }
    }
}

#[typetag::serde]
impl Runnable for MyFailingTask {
    fn run(&self, queue: &dyn Queueable) -> Result<(), Error> {
        let new_task = MyFailingTask::new(self.number + 1);

        queue.insert_task(&new_task).unwrap();

        log::info!(
            "Failing task number {}, Thread name:{}",
            self.number,
            self.current_thread_name
        );

        thread::sleep(Duration::from_secs(3));

        let b = true;

        if b {
            panic!("Hello!");
        } else {
            Ok(())
        }
    }

    fn task_type(&self) -> String {
        "worker_pool_test".to_string()
    }
}
