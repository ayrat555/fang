use fang::async_trait;
use fang::asynk::async_queue::AsyncQueueable;
use fang::asynk::async_runnable::Error;
use fang::asynk::async_runnable::Scheduled;
use fang::serde::{Deserialize, Serialize};
use fang::typetag;
use fang::AsyncRunnable;
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
    async fn run(&self, queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
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
    async fn run(&self, queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
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

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyCronTask {
    pub number: u16,
}

impl MyCronTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyTriggeredCronTasks {
    pub number: u16,
}

impl MyTriggeredCronTasks {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyTriggeredCronTasks {
    async fn run(&self, queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
        let new_task = MyTriggeredCronTasks::new(self.number + 1);

        queue
            .insert_task(&new_task as &dyn AsyncRunnable)
            .await
            .unwrap();

        log::info!(
            "CRON!!!!!!!!!!!!!!!!!!!! the current number is {}",
            self.number
        );
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }

    fn uniq(&self) -> bool {
        true
    }
}

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyCronTask {
    async fn run(&self, queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
        let new_task = MyTriggeredCronTasks::new(self.number + 1);
        queue
            .insert_task(&new_task as &dyn AsyncRunnable)
            .await
            .unwrap();

        log::info!(
            "CRON!!!!!!!!!!!!!!!!!!!! the current number is {}",
            self.number
        );
        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(())
    }

    fn cron(&self) -> Option<Scheduled> {
        //               sec  min   hour   day of month   month   day of week   year
        //               be careful works only with UTC hour.
        //               https://www.timeanddate.com/worldclock/timezone/utc
        let expression = "0 3 22 26,27 Aug-Sep * 2022/1";
        Some(Scheduled::CronPattern(expression.to_string()))
    }

    fn uniq(&self) -> bool {
        true
    }
}
