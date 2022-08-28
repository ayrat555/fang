use crate::error::FangError;
use crate::queue::Queueable;
use crate::worker::Worker;
use crate::RetentionMode;
use crate::SleepParams;
use log::error;
use log::info;
use std::thread;
use typed_builder::TypedBuilder;

#[derive(Clone, TypedBuilder)]
pub struct WorkerPool<BQueue>
where
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    #[builder(setter(into))]
    pub number_of_workers: u32,
    #[builder(setter(into))]
    pub queue: BQueue,
    #[builder(setter(into), default)]
    pub task_type: String,
    #[builder(setter(into), default)]
    pub sleep_params: SleepParams,
    #[builder(setter(into), default)]
    pub retention_mode: RetentionMode,
}

#[derive(Clone, TypedBuilder)]
pub struct WorkerThread<BQueue>
where
    BQueue: Queueable + Clone + Sync + Send + 'static,
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
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    pub fn start(&mut self) -> Result<(), FangError> {
        for idx in 1..self.number_of_workers + 1 {
            let name = format!("worker_{}{}", self.task_type, idx);

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
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    fn spawn(self) -> Result<thread::JoinHandle<()>, FangError> {
        info!(
            "starting a worker thread {}, number of restarts {}",
            self.name, self.restarts
        );

        let builder = thread::Builder::new().name(self.name.clone());

        builder
            .spawn(move || {
                let mut worker: Worker<BQueue> = Worker::builder()
                    .queue(self.worker_pool.queue.clone())
                    .task_type(self.worker_pool.task_type.clone())
                    .retention_mode(self.worker_pool.retention_mode.clone())
                    .sleep_params(self.worker_pool.sleep_params.clone())
                    .build();

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
    BQueue: Queueable + Clone + Sync + Send + 'static,
{
    fn drop(&mut self) {
        self.clone().spawn().unwrap();
    }
}
