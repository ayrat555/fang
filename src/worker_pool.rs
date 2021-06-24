use crate::executor::Executor;
use crate::postgres::Postgres;
use std::thread;

pub struct WorkerPool {
    pub number_of_workers: u16,
    pub name: String,
}

pub struct WorkerThread {
    pub name: String,
    pub restarts: u64,
}

impl WorkerPool {
    pub fn new(number_of_workers: u16, name: String) -> Self {
        Self {
            number_of_workers,
            name,
        }
    }

    pub fn start(&self) {
        for idx in 1..self.number_of_workers + 1 {
            let name = format!("{}{}", self.name, idx);

            WorkerThread::spawn_in_pool(name, 0)
        }
    }
}

impl WorkerThread {
    pub fn new(name: String, restarts: u64) -> Self {
        Self { name, restarts }
    }

    pub fn spawn_in_pool(name: String, restarts: u64) {
        let builder = thread::Builder::new().name(name.clone());

        info!(
            "starting a worker thread {}, number of restarts {}",
            name, restarts
        );

        builder
            .spawn(move || {
                // when _job is dropped, it will be restarted (see Drop trait impl)
                let _job = WorkerThread::new(name, restarts);

                let postgres = Postgres::new(None);

                Executor::new(postgres).run_tasks()
            })
            .unwrap();
    }
}

impl Drop for WorkerThread {
    fn drop(&mut self) {
        WorkerThread::spawn_in_pool(self.name.clone(), self.restarts + 1)
    }
}

#[cfg(test)]
mod job_pool_tests {
    use super::WorkerPool;
    use crate::executor::Error;
    use crate::executor::Runnable;
    use crate::postgres::Postgres;
    use crate::postgres::Task;
    use crate::schema::fang_tasks;
    use diesel::pg::PgConnection;
    use diesel::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::thread;
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct MyJob {
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

    fn get_all_tasks(conn: &PgConnection) -> Vec<Task> {
        fang_tasks::table.get_results::<Task>(conn).unwrap()
    }

    #[typetag::serde]
    impl Runnable for MyJob {
        fn run(&self) -> Result<(), Error> {
            let postgres = Postgres::new(None);

            thread::sleep(Duration::from_secs(3));

            let new_job = MyJob::new(self.number + 1);

            postgres.push_task(&new_job).unwrap();

            Ok(())
        }
    }

    // this test is ignored because it commits data to the db
    #[test]
    #[ignore]
    fn tasks_are_split_between_two_threads() {
        env_logger::init();

        let postgres = Postgres::new(None);
        let job_pool = WorkerPool::new(2, "test_worker".to_string());

        postgres.push_task(&MyJob::new(0)).unwrap();
        postgres.push_task(&MyJob::new(0)).unwrap();

        job_pool.start();

        thread::sleep(Duration::from_secs(100));

        let tasks = get_all_tasks(&postgres.connection);

        assert!(tasks.len() > 40);

        let test_worker1_jobs: Vec<Task> = tasks
            .clone()
            .into_iter()
            .filter(|job| {
                serde_json::to_string(&job.metadata)
                    .unwrap()
                    .contains("test_worker1")
            })
            .collect();

        let test_worker2_jobs: Vec<Task> = tasks
            .into_iter()
            .filter(|job| {
                serde_json::to_string(&job.metadata)
                    .unwrap()
                    .contains("test_worker2")
            })
            .collect();

        assert!(test_worker1_jobs.len() > 20);
        assert!(test_worker2_jobs.len() > 20);
    }
}
