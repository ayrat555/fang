use crate::executor::Runnable;
use crate::postgres::PeriodicTask;
use crate::postgres::Postgres;
use std::thread;
use std::time::Duration;

pub struct Scheduler {
    pub check_period: u64,
    pub error_margin_seconds: u64,
    pub postgres: Postgres,
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        Scheduler::start(self.check_period, self.error_margin_seconds)
    }
}

impl Scheduler {
    pub fn start(check_period: u64, error_margin_seconds: u64) {
        let postgres = Postgres::new();
        let builder = thread::Builder::new().name("scheduler".to_string());

        builder
            .spawn(move || {
                let scheduler = Self::new(check_period, error_margin_seconds, postgres);

                scheduler.schedule_loop();
            })
            .unwrap();
    }

    pub fn new(check_period: u64, error_margin_seconds: u64, postgres: Postgres) -> Self {
        Self {
            check_period,
            postgres,
            error_margin_seconds,
        }
    }

    pub fn schedule_loop(&self) {
        let sleep_duration = Duration::from_secs(self.check_period);

        loop {
            self.schedule();

            thread::sleep(sleep_duration);
        }
    }

    pub fn schedule(&self) {
        if let Some(tasks) = self
            .postgres
            .fetch_periodic_tasks(self.error_margin_seconds as i64)
        {
            for task in tasks {
                self.process_task(task);
            }
        };
    }

    fn process_task(&self, task: PeriodicTask) {
        match task.scheduled_at {
            None => {
                self.postgres.schedule_next_task_execution(&task).unwrap();
            }
            Some(_) => {
                let actual_task: Box<dyn Runnable> =
                    serde_json::from_value(task.metadata.clone()).unwrap();

                self.postgres.push_task(&(*actual_task)).unwrap();

                self.postgres.schedule_next_task_execution(&task).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod job_scheduler_tests {
    use super::Scheduler;
    use crate::executor::Error;
    use crate::executor::Runnable;
    use crate::postgres::Postgres;
    use crate::postgres::Task;
    use crate::schema::fang_tasks;
    use crate::typetag;
    use crate::{Deserialize, Serialize};
    use diesel::pg::PgConnection;
    use diesel::prelude::*;
    use std::thread;
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct ScheduledJob {}

    #[typetag::serde]
    impl Runnable for ScheduledJob {
        fn run(&self) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "schedule".to_string()
        }
    }

    #[test]
    #[ignore]
    fn schedules_jobs() {
        let postgres = Postgres::new();

        postgres.push_periodic_task(&ScheduledJob {}, 10).unwrap();
        Scheduler::start(1, 2);

        let sleep_duration = Duration::from_secs(15);
        thread::sleep(sleep_duration);

        let tasks = get_all_tasks(&postgres.connection);

        assert_eq!(1, tasks.len());
    }

    fn get_all_tasks(conn: &PgConnection) -> Vec<Task> {
        fang_tasks::table
            .filter(fang_tasks::task_type.eq("schedule"))
            .get_results::<Task>(conn)
            .unwrap()
    }
}
