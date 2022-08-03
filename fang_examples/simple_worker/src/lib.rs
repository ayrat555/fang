use fang::serde::{Deserialize, Serialize};
use fang::typetag;
use fang::Error;
use fang::PgConnection;
use fang::Queue;
use fang::Runnable;
use std::thread;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyJob {
    pub number: u16,
    pub current_thread_name: String,
}

impl MyJob {
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
impl Runnable for MyJob {
    fn run(&self, connection: &PgConnection) -> Result<(), Error> {
        thread::sleep(Duration::from_secs(3));

        let new_job = MyJob::new(self.number + 1);

        Queue::push_task_query(connection, &new_job).unwrap();

        Ok(())
    }

    fn task_type(&self) -> String {
        "worker_pool_test".to_string()
    }
}
