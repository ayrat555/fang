use crate::asynk::async_queue::AsyncQueueError;
use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::PeriodicTask;
use futures::executor;
use std::time::Duration;
use tokio::time::sleep;

pub struct Scheduler {
    pub check_period: u64,
    pub error_margin_seconds: u64,
    pub queue: &'static mut dyn AsyncQueueable,
}

impl Drop for Scheduler {
    fn drop(&'static mut self) {
        executor::block_on(Scheduler::start(
            self.check_period,
            self.error_margin_seconds,
            self.queue,
        ))
    }
}

impl Scheduler {
    pub async fn start(
        check_period: u64,
        error_margin_seconds: u64,
        queue: &'static mut dyn AsyncQueueable,
    ) {
        tokio::spawn(async move {
            let mut scheduler = Self::new(check_period, error_margin_seconds, queue);
            scheduler.schedule_loop().await.unwrap();
        })
        .await;
    }
    pub fn new(
        check_period: u64,
        error_margin_seconds: u64,
        queue: &'static mut dyn AsyncQueueable,
    ) -> Self {
        Self {
            check_period,
            queue,
            error_margin_seconds,
        }
    }

    pub async fn schedule_loop(&mut self) -> Result<(), AsyncQueueError> {
        let sleep_duration = Duration::from_secs(self.check_period);

        loop {
            self.schedule().await?;

            sleep(sleep_duration);
        }
    }

    pub async fn schedule(&mut self) -> Result<(), AsyncQueueError> {
        if let Some(tasks) = self
            .queue
            .fetch_periodic_tasks(self.error_margin_seconds as i64)
            .await?
        {
            for task in tasks {
                self.process_task(task).await;
            }
        };
        Ok(())
    }

    async fn process_task(&mut self, task: PeriodicTask) -> Result<(), AsyncQueueError> {
        match task.scheduled_at {
            None => {
                self.queue.schedule_next_task(task).await?;
            }
            Some(_) => {
                let metadata = task.metadata.clone();
                let period = task.period_in_seconds;
                self.queue.insert_periodic_task(metadata, period).await?;

                self.queue.schedule_next_task(task).await?;
            }
        }
        Ok(())
    }
}
