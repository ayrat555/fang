use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::PeriodicTask;
use crate::asynk::AsyncRunnable;
use crate::asynk::Error;
use async_recursion::async_recursion;
use log::error;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder, Clone)]
pub struct Scheduler<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    #[builder(setter(into))]
    pub check_period: u64,
    #[builder(setter(into))]
    pub error_margin_seconds: u64,
    #[builder(setter(into))]
    pub queue: AQueue,
    #[builder(default = 0, setter(into))]
    pub number_of_restarts: u32,
}

impl<AQueue> Scheduler<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    #[async_recursion(?Send)]
    pub async fn start(&mut self) -> Result<(), Error> {
        let join_handle: JoinHandle<Result<(), Error>> = self.schedule_loop().await;

        match join_handle.await {
            Err(err) => {
                error!(
                    "Scheduler panicked, restarting {:?}. Number of restarts {}",
                    err, self.number_of_restarts
                );
                self.number_of_restarts += 1;
                sleep(Duration::from_secs(1)).await;
                self.start().await
            }
            Ok(task_res) => match task_res {
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
            },
        }
    }

    pub async fn schedule_loop(&mut self) -> JoinHandle<Result<(), Error>> {
        let mut scheduler = self.clone();
        tokio::spawn(async move {
            let sleep_duration = Duration::from_secs(scheduler.check_period);

            loop {
                scheduler.schedule().await?;

                sleep(sleep_duration).await;
            }
        })
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
                let actual_task: Box<dyn AsyncRunnable> =
                    serde_json::from_value(task.metadata.clone()).unwrap();

                self.queue.insert_task(&*actual_task).await?;

                self.queue.schedule_next_task(task).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[derive(TypedBuilder)]
pub struct SchedulerTest<'a> {
    #[builder(setter(into))]
    pub check_period: u64,
    #[builder(setter(into))]
    pub error_margin_seconds: u64,
    #[builder(setter(into))]
    pub queue: &'a mut dyn AsyncQueueable,
    #[builder(default = 0, setter(into))]
    pub number_of_restarts: u32,
}

#[cfg(test)]
impl<'a> SchedulerTest<'a> {
    async fn schedule_test(&mut self) -> Result<(), Error> {
        let sleep_duration = Duration::from_secs(self.check_period);

        loop {
            match self
                .queue
                .fetch_periodic_tasks(self.error_margin_seconds as i64)
                .await?
            {
                Some(tasks) => {
                    for task in tasks {
                        self.process_task(task).await?;
                    }

                    return Ok(());
                }
                None => {
                    sleep(sleep_duration).await;
                }
            };
        }
    }

    async fn process_task(&mut self, task: PeriodicTask) -> Result<(), Error> {
        match task.scheduled_at {
            None => {
                self.queue.schedule_next_task(task).await?;
            }
            Some(_) => {
                let actual_task: Box<dyn AsyncRunnable> =
                    serde_json::from_value(task.metadata.clone()).unwrap();

                self.queue.insert_task(&*actual_task).await?;

                self.queue.schedule_next_task(task).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod async_scheduler_tests {
    use super::SchedulerTest;
    use crate::asynk::async_queue::AsyncQueueTest;
    use crate::asynk::async_queue::AsyncQueueable;
    use crate::asynk::async_queue::PeriodicTask;
    use crate::asynk::AsyncRunnable;
    use crate::asynk::Error;
    use async_trait::async_trait;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::tokio_postgres::NoTls;
    use bb8_postgres::PostgresConnectionManager;
    use chrono::DateTime;
    use chrono::Duration as OtherDuration;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AsyncScheduledTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncScheduledTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }
        fn task_type(&self) -> String {
            "schedule".to_string()
        }
    }

    #[tokio::test]
    async fn schedules_tasks() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest::builder().transaction(transaction).build();

        let schedule_in_future = Utc::now() + OtherDuration::seconds(5);

        let _periodic_task = insert_periodic_task(
            &mut test,
            &AsyncScheduledTask { number: 1 },
            schedule_in_future,
            10,
        )
        .await;

        let check_period: u64 = 1;
        let error_margin_seconds: u64 = 2;

        let mut scheduler = SchedulerTest::builder()
            .check_period(check_period)
            .error_margin_seconds(error_margin_seconds)
            .queue(&mut test as &mut dyn AsyncQueueable)
            .build();
        // Scheduler start tricky not loop :)
        scheduler.schedule_test().await.unwrap();

        let task = scheduler
            .queue
            .fetch_and_touch_task(Some("schedule".to_string()))
            .await
            .unwrap()
            .unwrap();

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        let runnable_task: Box<dyn AsyncRunnable> =
            serde_json::from_value(task.metadata.clone()).unwrap();

        assert_eq!("schedule", runnable_task.task_type());
        assert_eq!(Some("AsyncScheduledTask"), type_task);
        assert_eq!(Some(1), number);
    }

    async fn insert_periodic_task(
        test: &mut AsyncQueueTest<'_>,
        task: &dyn AsyncRunnable,
        timestamp: DateTime<Utc>,
        period_in_seconds: i32,
    ) -> PeriodicTask {
        test.insert_periodic_task(task, timestamp, period_in_seconds)
            .await
            .unwrap()
    }

    async fn pool() -> Pool<PostgresConnectionManager<NoTls>> {
        let pg_mgr = PostgresConnectionManager::new_from_stringlike(
            "postgres://postgres:postgres@localhost/fang",
            NoTls,
        )
        .unwrap();

        Pool::builder().build(pg_mgr).await.unwrap()
    }
}
