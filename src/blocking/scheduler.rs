use crate::executor::Runnable;
use crate::queue::Queue;
use crate::PeriodicTask;
use std::thread;
use std::time::Duration;

pub struct Scheduler {
    pub check_period: Duration,
    pub error_margin: Duration,
    pub queue: Queue,
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        Scheduler::start(self.check_period, self.error_margin)
    }
}

impl Scheduler {
    pub fn start(check_period: Duration, error_margin: Duration) {
        let queue = Queue::new();
        let builder = thread::Builder::new().name("scheduler".to_string());

        builder
            .spawn(move || {
                let scheduler = Self::new(check_period, error_margin, queue);

                scheduler.schedule_loop();
            })
            .unwrap();
    }

    pub fn new(check_period: Duration, error_margin: Duration, queue: Queue) -> Self {
        Self {
            check_period,
            queue,
            error_margin,
        }
    }

    pub fn schedule_loop(&self) {
        loop {
            self.schedule();

            thread::sleep(self.check_period);
        }
    }

    pub fn schedule(&self) {
        if let Some(tasks) = self.queue.fetch_periodic_tasks(self.error_margin) {
            for task in tasks {
                self.process_task(task);
            }
        };
    }

    fn process_task(&self, task: PeriodicTask) {
        match task.scheduled_at {
            None => {
                self.queue.schedule_next_task_execution(&task).unwrap();
            }
            Some(_) => {
                let actual_task: Box<dyn Runnable> =
                    serde_json::from_value(task.metadata.clone()).unwrap();

                self.queue.push_task(&(*actual_task)).unwrap();

                self.queue.schedule_next_task_execution(&task).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod task_scheduler_tests {
    use super::Scheduler;
    use crate::executor::Error;
    use crate::executor::Runnable;
    use crate::queue::Queue;
    use crate::schema::fang_tasks;
    use crate::typetag;
    use crate::Task;
    use diesel::pg::PgConnection;
    use diesel::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::thread;
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct ScheduledTask {}

    #[typetag::serde]
    impl Runnable for ScheduledTask {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "schedule".to_string()
        }
    }

    #[test]
    #[ignore]
    fn schedules_tasks() {
        let queue = Queue::new();

        queue.push_periodic_task(&ScheduledTask {}, 10).unwrap();
        Scheduler::start(Duration::from_secs(1), Duration::from_secs(2));

        let sleep_duration = Duration::from_secs(15);
        thread::sleep(sleep_duration);

        let tasks = get_all_tasks(&queue.connection);

        assert_eq!(1, tasks.len());
    }

    fn get_all_tasks(conn: &PgConnection) -> Vec<Task> {
        fang_tasks::table
            .filter(fang_tasks::task_type.eq("schedule"))
            .get_results::<Task>(conn)
            .unwrap()
    }
}
