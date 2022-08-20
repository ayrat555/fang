use crate::asynk::async_queue::AsyncQueueable;
use async_trait::async_trait;

const COMMON_TYPE: &str = "common";

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

pub enum Uniq {
    Metadata,
    String(String),
    Hash(String),
}

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait AsyncRunnable: Send + Sync {
    async fn run(&self, client: &mut dyn AsyncQueueable) -> Result<(), Error>;

    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }

    fn uniq(&self) -> Option<Uniq> {
        None
    }
}
