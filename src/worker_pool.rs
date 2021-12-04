use crate::diesel::r2d2;
use crate::diesel::PgConnection;
use crate::error::FangError;
use crate::executor::Executor;
use crate::executor::RetentionMode;
use crate::executor::SleepParams;
use crate::queue::Queue;
use log::error;
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

#[derive(Clone)]
pub struct WorkerPool {
    pub number_of_workers: u32,
    pub worker_params: WorkerParams,
    pub connection_pool: r2d2::Pool<r2d2::ConnectionManager<PgConnection>>,
    shared_state: SharedState,
    thread_join_handles: Arc<RwLock<HashMap<String, thread::JoinHandle<()>>>>,
}

pub struct WorkerThread {
    pub name: String,
    pub restarts: u64,
    pub worker_pool: WorkerPool,
    graceful_shutdown: bool,
}

pub type SharedState = Arc<RwLock<Option<WorkerState>>>;

pub enum WorkerState {
    Shutdown,
}

#[derive(Clone)]
pub struct WorkerParams {
    pub retention_mode: Option<RetentionMode>,
    pub sleep_params: Option<SleepParams>,
    pub task_type: Option<String>,
}

impl Default for WorkerParams {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerParams {
    pub fn new() -> Self {
        Self {
            retention_mode: None,
            sleep_params: None,
            task_type: None,
        }
    }

    pub fn set_retention_mode(&mut self, retention_mode: RetentionMode) {
        self.retention_mode = Some(retention_mode);
    }

    pub fn set_sleep_params(&mut self, sleep_params: SleepParams) {
        self.sleep_params = Some(sleep_params);
    }

    pub fn set_task_type(&mut self, task_type: String) {
        self.task_type = Some(task_type);
    }
}

impl WorkerPool {
    pub fn new(number_of_workers: u32) -> Self {
        let worker_params = WorkerParams::new();
        let connection_pool = Queue::connection_pool(number_of_workers);

        Self {
            number_of_workers,
            worker_params,
            connection_pool,
            shared_state: Arc::new(RwLock::new(None)),
            thread_join_handles: Arc::new(RwLock::new(HashMap::with_capacity(
                number_of_workers as usize,
            ))),
        }
    }

    pub fn new_with_params(number_of_workers: u32, worker_params: WorkerParams) -> Self {
        let connection_pool = Queue::connection_pool(number_of_workers);

        Self {
            number_of_workers,
            worker_params,
            connection_pool,
            shared_state: Arc::new(RwLock::new(None)),
            thread_join_handles: Arc::new(RwLock::new(HashMap::with_capacity(
                number_of_workers as usize,
            ))),
        }
    }

    pub fn start(&mut self) -> Result<(), FangError> {
        for idx in 1..self.number_of_workers + 1 {
            let worker_type = self
                .worker_params
                .task_type
                .clone()
                .unwrap_or_else(|| "".to_string());
            let name = format!("worker_{}{}", worker_type, idx);
            WorkerThread::spawn_in_pool(name.clone(), 0, self.clone())?;
        }
        Ok(())
    }

    /// Attempt graceful shutdown of each job thread, blocks until all threads exit. Threads exit
    /// when their current job finishes.
    pub fn shutdown(&mut self) -> Result<(), FangError> {
        let mut shared_state = self.shared_state.write()?;
        *shared_state = Some(WorkerState::Shutdown);

        for (worker_name, thread) in self.thread_join_handles.write()?.drain() {
            if let Err(err) = thread.join() {
                error!(
                    "Failed to exit executor thread '{}' cleanly: {:?}",
                    worker_name, err
                );
            }
        }
        Ok(())
    }
}

impl WorkerThread {
    pub fn new(name: String, restarts: u64, worker_pool: WorkerPool) -> Self {
        Self {
            name,
            restarts,
            worker_pool,
            graceful_shutdown: false,
        }
    }

    pub fn spawn_in_pool(
        name: String,
        restarts: u64,
        worker_pool: WorkerPool,
    ) -> Result<(), FangError> {
        info!(
            "starting a worker thread {}, number of restarts {}",
            name, restarts
        );

        let job = WorkerThread::new(name.clone(), restarts, worker_pool.clone());
        let join_handle = Self::spawn_thread(name.clone(), job)?;
        worker_pool
            .thread_join_handles
            .write()?
            .insert(name, join_handle);
        Ok(())
    }

    fn spawn_thread(
        name: String,
        mut job: WorkerThread,
    ) -> Result<thread::JoinHandle<()>, FangError> {
        let builder = thread::Builder::new().name(name.clone());
        builder
            .spawn(move || {
                match job.worker_pool.connection_pool.get() {
                    Ok(connection) => {
                        let mut executor = Executor::new(connection);
                        executor.set_shared_state(job.worker_pool.shared_state.clone());

                        if let Some(ref task_type_str) = job.worker_pool.worker_params.task_type {
                            executor.set_task_type(task_type_str.to_owned());
                        }

                        if let Some(ref retention_mode) =
                            job.worker_pool.worker_params.retention_mode
                        {
                            executor.set_retention_mode(retention_mode.to_owned());
                        }

                        if let Some(ref sleep_params) = job.worker_pool.worker_params.sleep_params {
                            executor.set_sleep_params(sleep_params.clone());
                        }

                        // Run executor
                        match executor.run_tasks() {
                            Ok(_) => {
                                job.graceful_shutdown = true;
                            }
                            Err(error) => {
                                error!("Error executing tasks in worker '{}': {:?}", name, error);
                            }
                        }
                    }
                    Err(error) => {
                        error!("Failed to get postgres connection: {:?}", error);
                    }
                }
            })
            .map_err(FangError::from)
    }
}

impl Drop for WorkerThread {
    fn drop(&mut self) {
        if self.graceful_shutdown {
            return;
        }

        WorkerThread::spawn_in_pool(
            self.name.clone(),
            self.restarts + 1,
            self.worker_pool.clone(),
        )
        .unwrap();
    }
}

#[cfg(test)]
mod job_pool_tests {
    use super::WorkerParams;
    use super::WorkerPool;
    use crate::executor::Error;
    use crate::executor::RetentionMode;
    use crate::executor::Runnable;
    use crate::queue::Queue;
    use crate::queue::Task;
    use crate::schema::fang_tasks;
    use crate::typetag;
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

    fn get_all_tasks(conn: &PgConnection) -> Vec<Task> {
        fang_tasks::table
            .filter(fang_tasks::task_type.eq("worker_pool_test"))
            .get_results::<Task>(conn)
            .unwrap()
    }

    // this test is ignored because it commits data to the db
    #[test]
    #[ignore]
    fn tasks_are_split_between_two_threads() {
        let queue = Queue::new();

        let mut worker_params = WorkerParams::new();
        worker_params.set_retention_mode(RetentionMode::KeepAll);
        let mut job_pool = WorkerPool::new_with_params(2, worker_params);

        queue.push_task(&MyJob::new(100)).unwrap();
        queue.push_task(&MyJob::new(200)).unwrap();

        job_pool.start().unwrap();

        thread::sleep(Duration::from_secs(100));

        let tasks = get_all_tasks(&queue.connection);

        assert!(tasks.len() > 40);

        let test_worker1_jobs = tasks.clone().into_iter().filter(|job| {
            serde_json::to_string(&job.metadata)
                .unwrap()
                .contains("worker_1")
        });

        let test_worker2_jobs = tasks.into_iter().filter(|job| {
            serde_json::to_string(&job.metadata)
                .unwrap()
                .contains("worker_2")
        });

        assert!(test_worker1_jobs.count() > 20);
        assert!(test_worker2_jobs.count() > 20);
    }
}
