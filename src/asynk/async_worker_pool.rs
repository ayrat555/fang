use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_worker::AsyncWorker;
use crate::asynk::AsyncError as Error;
use crate::{RetentionMode, SleepParams};
use async_recursion::async_recursion;
use log::error;
use tokio::task::JoinHandle;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder, Clone)]
pub struct AsyncWorkerPool<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    #[builder(setter(into))]
    pub queue: AQueue,
    #[builder(default, setter(into))]
    pub sleep_params: SleepParams,
    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,
    #[builder(setter(into))]
    pub number_of_workers: u32,
}

impl<AQueue> AsyncWorkerPool<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    pub async fn start(&mut self) {
        for idx in 0..self.number_of_workers {
            let pool = self.clone();
            tokio::spawn(Self::supervise_task(pool, 0, idx));
        }
    }

    #[async_recursion]
    pub async fn supervise_task(pool: AsyncWorkerPool<AQueue>, restarts: u64, worker_number: u32) {
        let restarts = restarts + 1;
        let join_handle = Self::spawn_worker(
            pool.queue.clone(),
            pool.sleep_params.clone(),
            pool.retention_mode.clone(),
        )
        .await;

        if (join_handle.await).is_err() {
            error!(
                "Worker {} stopped. Restarting. the number of restarts {}",
                worker_number, restarts,
            );
            Self::supervise_task(pool, restarts, worker_number).await;
        }
    }

    pub async fn spawn_worker(
        queue: AQueue,
        sleep_params: SleepParams,
        retention_mode: RetentionMode,
    ) -> JoinHandle<Result<(), Error>> {
        tokio::spawn(async move { Self::run_worker(queue, sleep_params, retention_mode).await })
    }
    pub async fn run_worker(
        queue: AQueue,
        sleep_params: SleepParams,
        retention_mode: RetentionMode,
    ) -> Result<(), Error> {
        let mut worker: AsyncWorker<AQueue> = AsyncWorker::builder()
            .queue(queue)
            .sleep_params(sleep_params)
            .retention_mode(retention_mode)
            .build();

        worker.run_tasks().await
    }
}
