use async_trait::async_trait;
use bb8_postgres::tokio_postgres::Client;

const COMMON_TYPE: &str = "common";

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait AsyncRunnable {
    async fn run(&self, client: &Client) -> Result<(), Error>;

    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }
}
