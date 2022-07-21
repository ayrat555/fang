use crate::asynk::async_queue::AsyncQueueable;
use async_trait::async_trait;
use futures::future::BoxFuture;

const COMMON_TYPE: &str = "common";

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait AsyncRunnable {
    async fn run(&self, client: BoxFuture<dyn AsyncQueueable>) -> Result<(), Error>;

    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }
}
