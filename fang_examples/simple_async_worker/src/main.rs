use fang::asynk::async_queue::AsyncQueue;
use fang::asynk::async_queue::AsyncQueueable;
use fang::asynk::async_worker_pool::AsyncWorkerPool;
use fang::AsyncRunnable;
use simple_async_worker::MyTask;
use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    env_logger::init();

    log::info!("Starting...");
    let number_conns: u32 = 2;
    let mut queue = AsyncQueue::builder()
        .uri("postgres://postgres:postgres@localhost/fang")
        .number_conns(number_conns)
        .duplicated_tasks(true)
        .build();

    queue.connect(NoTls).await.unwrap();
    log::info!("Queue connected...");

    let mut pool = AsyncWorkerPool::builder().queue(queue.clone()).build();

    log::info!("Pool created ...");

    pool.start().await;
    log::info!("Workers started ...");

    let task1 = MyTask::new(0);
    let task2 = MyTask::new(20_000);

    queue
        .insert_task(&task1 as &dyn AsyncRunnable)
        .await
        .unwrap();
    queue
        .insert_task(&task2 as &dyn AsyncRunnable)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(100)).await;
}
