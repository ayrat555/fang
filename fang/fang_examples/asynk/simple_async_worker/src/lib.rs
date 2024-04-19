use fang::async_trait;
use fang::asynk::async_queue::AsyncQueueable;
use fang::serde::{Deserialize, Serialize};
use fang::typetag;
use fang::AsyncRunnable;
use fang::FangError;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyTask {
    pub number: u16,
}

impl MyTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyFailingTask {
    pub number: u16,
}

impl MyFailingTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyTask {
    async fn run(&self, queue: &dyn AsyncQueueable) -> Result<(), FangError> {
        let new_task = MyTask::new(self.number + 1);
        queue
            .insert_task(&new_task as &dyn AsyncRunnable)
            .await
            .unwrap();

        log::info!("the current number is {}", self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(())
    }
}

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyFailingTask {
    async fn run(&self, queue: &dyn AsyncQueueable) -> Result<(), FangError> {
        let new_task = MyFailingTask::new(self.number + 1);
        queue
            .insert_task(&new_task as &dyn AsyncRunnable)
            .await
            .unwrap();

        log::info!("the current number is {}", self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        let b = true;

        if b {
            panic!("Hello!");
        } else {
            Ok(())
        }
    }
}
