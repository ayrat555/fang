use fang::asynk::async_queue::AsyncQueue;
use fang::asynk::async_queue::AsyncQueueable;
use fang::asynk::async_worker_pool::AsyncWorkerPool;
use fang::AsyncRunnable;
use fang::NoTls;
use simple_cron_async_worker::MyCronTask;
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();

    log::info!("Starting...");
    let max_pool_size: u32 = 3;
    let mut queue = AsyncQueue::builder()
        .uri("postgres://postgres:postgres@localhost/fang")
        .max_pool_size(max_pool_size)
        .build();

    queue.connect(NoTls).await.unwrap();
    log::info!("Queue connected...");

    let mut pool: AsyncWorkerPool<AsyncQueue<NoTls>> = AsyncWorkerPool::builder()
        .number_of_workers(10_u32)
        .queue(queue.clone())
        .build();

    log::info!("Pool created ...");

    pool.start().await;
    log::info!("Workers started ...");

    let task = MyCronTask {};

    queue
        .schedule_task(&task as &dyn AsyncRunnable)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(100)).await;
}
