use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_worker::AsyncWorker;
use crate::asynk::Error;
use crate::{RetentionMode, SleepParams};
use async_recursion::async_recursion;
use log::error;
use std::time::Duration;
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

#[derive(TypedBuilder, Clone)]
pub struct WorkerParams {
    #[builder(setter(into, strip_option), default)]
    pub retention_mode: Option<RetentionMode>,
    #[builder(setter(into, strip_option), default)]
    pub sleep_params: Option<SleepParams>,
    #[builder(setter(into, strip_option), default)]
    pub task_type: Option<String>,
}

impl<AQueue> AsyncWorkerPool<AQueue>
where
    AQueue: AsyncQueueable + Clone + Sync + 'static,
{
    pub async fn start(&mut self) {
        for _idx in 0..self.number_of_workers {
            let queue = self.queue.clone();
            let sleep_params = self.sleep_params.clone();
            let retention_mode = self.retention_mode.clone();

            Self::supervise_task(queue, sleep_params, retention_mode, 0).await
        }
    }

    #[async_recursion]
    pub async fn supervise_task(
        queue: AQueue,
        sleep_params: SleepParams,
        retention_mode: RetentionMode,
        restarts: u64,
    ) {
        let restarts = restarts + 1;
        let join_handle =
            Self::spawn_worker(queue.clone(), sleep_params.clone(), retention_mode.clone()).await;

        tokio::join!(join_handle);

        error!(
            "Worker stopped. Restarting. the number of restarts {}",
            restarts
        );

        // println!("Original task is joined.");

        // if join_handle.is_finished() {
        //     println!("restarting");
        Self::supervise_task(queue, sleep_params, retention_mode, restarts).await;
        // }

        // join!(join_handle);

        // Self::supervise_task(queue, sleep_params, retention_mode).await;
    }

    pub async fn spawn_worker(
        queue: AQueue,
        sleep_params: SleepParams,
        retention_mode: RetentionMode,
    ) -> JoinHandle<Result<(), Error>> {
        tokio::spawn(async move { Self::run_worker(queue, sleep_params, retention_mode).await })
    }

    // #[async_recursion]
    // pub async fn supervise_worker(
    //     queue: AQueue,
    //     sleep_params: SleepParams,
    //     retention_mode: RetentionMode,
    // ) -> Result<(), Error> {
    //     let result =
    //         Self::run_worker(queue.clone(), sleep_params.clone(), retention_mode.clone()).await;

    //     tokio::time::sleep(Duration::from_secs(1)).await;

    //     match result {
    //         Err(err) => {
    //             error!("Worker failed. Restarting. {:?}", err);
    //             Self::supervise_worker(queue, sleep_params, retention_mode).await
    //         }
    //         Ok(_) => {
    //             error!("Worker stopped. Restarting");
    //             Self::supervise_worker(queue, sleep_params, retention_mode).await
    //         }
    //     }
    // }

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
