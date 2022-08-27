use crate::error::FangError;
use crate::queue::Queueable;
use crate::worker::Worker;
use crate::RetentionMode;
use crate::SleepParams;
use log::error;
use log::info;
use std::thread;
use typed_builder::TypedBuilder;

// TODO Re-implement worker pool with new Worker and Queue
#[derive(Clone, TypedBuilder)]
pub struct WorkerPool<BQueue>
where
    BQueue: Queueable,
{
    pub number_of_workers: u32,
    pub queue: BQueue,
    #[builder(setter(into, strip_option), default)]
    pub task_type: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub sleep_params: Option<SleepParams>,
    #[builder(setter(into, strip_option), default)]
    pub retention_mode: Option<RetentionMode>,
}

#[derive(TypedBuilder)]
pub struct WorkerThread<BQueue>
where
    BQueue: Queueable,
{
    pub name: String,
    pub restarts: u64,
    pub worker_pool: WorkerPool<BQueue>,
}

#[derive(Clone)]
pub struct WorkerParams {
    pub retention_mode: Option<RetentionMode>,
    pub sleep_params: Option<SleepParams>,
    pub task_type: Option<String>,
}

impl<BQueue> WorkerPool<BQueue>
where
    BQueue: Queueable,
{
    pub fn start(&mut self) -> Result<(), FangError> {
        for idx in 1..self.number_of_workers + 1 {
            let worker_type = self.task_type.clone().unwrap_or_else(|| "".to_string());
            let name = format!("worker_{}{}", worker_type, idx);

            let worker_thread = WorkerThread::builder()
                .name(name.clone())
                .restarts(0)
                .worker_pool(self.clone())
                .build();

            worker_thread.spawn()?;
        }
        Ok(())
    }
}

impl<BQueue> WorkerThread<BQueue>
where
    BQueue: Queueable,
{
    fn spawn(&self) -> Result<thread::JoinHandle<()>, FangError> {
        info!(
            "starting a worker thread {}, number of restarts {}",
            self.name, self.restarts
        );

        let builder = thread::Builder::new().name(self.name.clone());

        builder
            .spawn(move || {
                let mut worker_builder = Worker::builder();

                worker_builder.queue(self.worker_pool.queue);

                if let Some(ref task_type_str) = self.worker_pool.task_type {
                    worker_builder.set_task_type(task_type_str.to_owned());
                }

                if let Some(ref retention_mode) = self.worker_pool.retention_mode {
                    worker_builder.retention_mode(retention_mode.to_owned());
                }

                if let Some(ref sleep_params) = self.worker_pool.sleep_params {
                    worker_builder.sleep_params(sleep_params.clone());
                }

                let worker = worker_builder.build();

                // Run worker
                if let Err(error) = worker.run_tasks() {
                    error!(
                        "Error executing tasks in worker '{}': {:?}",
                        self.name, error
                    );
                }
            })
            .map_err(FangError::from)
    }
}

impl<BQueue> Drop for WorkerThread<BQueue>
where
    BQueue: Queueable,
{
    fn drop(&mut self) {
        self.spawn().unwrap();
    }
}

#[cfg(test)]
mod task_pool_tests {
    use super::WorkerParams;
    use super::WorkerPool;
    use crate::executor::Error;
    use crate::executor::Runnable;
    use crate::queue::Queue;
    use crate::schema::{fang_tasks, FangTaskState};
    use crate::typetag;
    use crate::RetentionMode;
    use crate::Task;
    use diesel::pg::PgConnection;
    use diesel::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::thread;
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct MyTask {
        pub number: u16,
        pub current_thread_name: String,
    }

    impl MyTask {
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
    impl Runnable for MyTask {
        fn run(&self, connection: &PgConnection) -> Result<(), Error> {
            thread::sleep(Duration::from_secs(3));

            let new_task = MyTask::new(self.number + 1);

            Queue::push_task_query(connection, &new_task).unwrap();

            Ok(())
        }

        fn task_type(&self) -> String {
            "worker_pool_test".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct ShutdownTask {
        pub number: u16,
        pub current_thread_name: String,
    }

    impl ShutdownTask {
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
    impl Runnable for ShutdownTask {
        fn run(&self, connection: &PgConnection) -> Result<(), Error> {
            thread::sleep(Duration::from_secs(3));

            let new_task = MyTask::new(self.number + 1);

            Queue::push_task_query(connection, &new_task).unwrap();

            Ok(())
        }

        fn task_type(&self) -> String {
            "shutdown_test".to_string()
        }
    }

    fn get_all_tasks(conn: &PgConnection, job_type: &str) -> Vec<Task> {
        fang_tasks::table
            .filter(fang_tasks::task_type.eq(job_type))
            .get_results::<Task>(conn)
            .unwrap()
    }

    // Following tests ignored because they commit data to the db
    #[test]
    #[ignore]
    fn tasks_are_finished_on_shutdown() {
        let queue = Queue::new();

        let mut worker_params = WorkerParams::new();
        worker_params.set_retention_mode(RetentionMode::KeepAll);
        let mut task_pool = WorkerPool::new_with_params(2, worker_params);

        queue.push_task(&ShutdownTask::new(100)).unwrap();
        queue.push_task(&ShutdownTask::new(200)).unwrap();

        task_pool.start().unwrap();
        thread::sleep(Duration::from_secs(1));
        task_pool.shutdown().unwrap();
        thread::sleep(Duration::from_secs(5));

        let tasks = get_all_tasks(&queue.connection, "shutdown_test");
        let in_progress_tasks = tasks
            .iter()
            .filter(|task| task.state == FangTaskState::InProgress);
        let finished_tasks = tasks
            .iter()
            .filter(|task| task.state == FangTaskState::Finished);

        // Asserts first two tasks are allowed to finish, the tasks they spawn are not started
        // though. No tasks should be in progress after a graceful shutdown.
        assert_eq!(in_progress_tasks.count(), 0);
        assert_eq!(finished_tasks.count(), 2);
    }

    #[test]
    #[ignore]
    fn tasks_are_split_between_two_threads() {
        let queue = Queue::new();

        let mut worker_params = WorkerParams::new();
        worker_params.set_retention_mode(RetentionMode::KeepAll);
        let mut task_pool = WorkerPool::new_with_params(2, worker_params);

        queue.push_task(&MyTask::new(100)).unwrap();
        queue.push_task(&MyTask::new(200)).unwrap();

        task_pool.start().unwrap();

        thread::sleep(Duration::from_secs(100));

        let tasks = get_all_tasks(&queue.connection, "worker_pool_test");

        assert!(tasks.len() > 40);

        let test_worker1_tasks = tasks.clone().into_iter().filter(|task| {
            serde_json::to_string(&task.metadata)
                .unwrap()
                .contains("worker_1")
        });

        let test_worker2_tasks = tasks.into_iter().filter(|task| {
            serde_json::to_string(&task.metadata)
                .unwrap()
                .contains("worker_2")
        });

        assert!(test_worker1_tasks.count() > 20);
        assert!(test_worker2_tasks.count() > 20);
    }
}
