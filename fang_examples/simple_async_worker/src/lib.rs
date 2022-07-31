use async_trait::async_trait;
use fang::asynk::async_queue::AsyncQueueable;
use fang::asynk::async_runnable::Error;
use fang::typetag;
use fang::AsyncRunnable;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub struct MyTask {
    pub number: u16,
}

impl MyTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyTask {
    async fn run(&self, queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
        log::info!("the curreny number is {}", self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        let new_task = MyTask::new(self.number + 1);
        let metadata = serde_json::to_value(&new_task as &dyn AsyncRunnable).unwrap();
        queue.insert_task(metadata, "common").await.unwrap();

        Ok(())
    }
}
