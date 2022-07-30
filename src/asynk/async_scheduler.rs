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
    pub number_of_restarts: u32,
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
                eprintln!("Insert task");
                self.queue.schedule_next_task(task).await?;
            }
            Some(_) => {
                let metadata = task.metadata.clone();
                let type_task = match metadata["type"].as_str() {
                    Some(task_type) => task_type.clone(),
                    None => "common",
                };
                println!("Insert task");
                self.queue.insert_task(metadata.clone(), type_task).await?;

                self.queue.schedule_next_task(task).await?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
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
                        eprintln!("Process task");
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
}

#[cfg(test)]
mod async_scheduler_tests {
    use super::Scheduler;
    use crate::asynk::async_queue::AsyncQueueTest;
    use crate::asynk::async_queue::AsyncQueueable;
    use crate::asynk::async_queue::PeriodicTask;
    use crate::asynk::AsyncRunnable;
    use crate::asynk::Error;
    use async_trait::async_trait;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::tokio_postgres::NoTls;
    use bb8_postgres::PostgresConnectionManager;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct AsyncScheduledTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait(?Send)]
    impl AsyncRunnable for AsyncScheduledTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
            Ok(())
        }
        /*        fn task_type(&self) -> String {
                  "schedule".to_string()
              }
        */
    }

    #[tokio::test]
    async fn schedules_tasks() {
        let pool = pool().await;
        let mut connection = pool.get().await.unwrap();
        let transaction = connection.transaction().await.unwrap();

        let mut test = AsyncQueueTest {
            transaction,
            duplicated_tasks: true,
        };

        let _periodic_task =
            insert_periodic_task(&mut test, &AsyncScheduledTask { number: 1 }, 10).await;

        let mut scheduler = Scheduler::builder()
            .check_period(1 as u64)
            .error_margin_seconds(2 as u64)
            .queue(&mut test as &mut dyn AsyncQueueable)
            .build();
        // Scheduler start tricky not loop :)
        scheduler.schedule_test().await.unwrap();

        let sleep_duration = Duration::from_secs(15);
        tokio::time::sleep(sleep_duration).await;

        let task = scheduler
            .queue
            .fetch_and_touch_task(Some("common".to_string()))
            .await
            .unwrap()
            .unwrap();

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        assert_eq!(Some("common"), type_task);
        assert_eq!(Some(1), number);
    }

    async fn insert_periodic_task(
        test: &mut AsyncQueueTest<'_>,
        task: &dyn AsyncRunnable,
        period_in_seconds: i32,
    ) -> PeriodicTask {
        let metadata = serde_json::to_value(task).unwrap();

        test.insert_periodic_task(metadata, period_in_seconds)
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
