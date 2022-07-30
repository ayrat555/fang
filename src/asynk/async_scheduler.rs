use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::PeriodicTask;
use crate::asynk::Error;
use async_recursion::async_recursion;
use log::error;
use std::time::Duration;
use tokio::time::sleep;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct Scheduler<'a> {
    #[builder(setter(into))]
    pub check_period: u64,
    #[builder(setter(into))]
    pub error_margin_seconds: u64,
    #[builder(setter(into))]
    pub queue: &'a mut dyn AsyncQueueable,
    #[builder(default = 0, setter(into))]
    number_of_restarts: u32,
}

impl<'a> Scheduler<'a> {
    #[async_recursion(?Send)]
    pub async fn start(&mut self) -> Result<(), Error> {
        let task_res = self.schedule_loop().await;

        match task_res {
            Err(err) => {
                error!(
                    "Scheduler failed, restarting {:?}. Number of restarts {}",
                    err, self.number_of_restarts
                );
                self.number_of_restarts += 1;
                self.start().await
            }
            Ok(_) => {
                error!(
                    "Scheduler stopped. restarting. Number of restarts {}",
                    self.number_of_restarts
                );
                self.number_of_restarts += 1;
                self.start().await
            }
        }
    }

    pub async fn schedule_loop(&mut self) -> Result<(), Error> {
        let sleep_duration = Duration::from_secs(self.check_period);

        loop {
            self.schedule().await?;

            sleep(sleep_duration).await;
        }
    }

    pub async fn schedule(&mut self) -> Result<(), Error> {
        if let Some(tasks) = self
            .queue
            .fetch_periodic_tasks(self.error_margin_seconds as i64)
            .await?
        {
            for task in tasks {
                self.process_task(task).await?;
            }
        };
        Ok(())
    }

    async fn process_task(&mut self, task: PeriodicTask) -> Result<(), Error> {
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
