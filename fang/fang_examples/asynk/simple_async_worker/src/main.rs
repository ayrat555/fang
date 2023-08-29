use connection::Connection;
use diesel::connection;
use diesel::PgConnection;
use dotenvy::dotenv;
use fang::asynk::async_queue::AsyncQueue;
use fang::asynk::async_queue::AsyncQueueable;
use fang::asynk::async_worker_pool::AsyncWorkerPool;
use fang::run_migrations_postgres;
use fang::AsyncRunnable;
use fang::BackendSqlXPg;
use simple_async_worker::MyFailingTask;
use simple_async_worker::MyTask;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let mut connection = PgConnection::establish(&database_url).unwrap();

    log::info!("Running migrations");

    run_migrations_postgres(&mut connection).unwrap();

    log::info!("Migrations done :D");

    drop(connection);

    log::info!("Starting...");
    let max_pool_size: u32 = 3;
    let mut queue = AsyncQueue::builder()
        .uri(database_url)
        .max_pool_size(max_pool_size)
        .backend(BackendSqlXPg {})
        .build();

    queue.connect().await.unwrap();
    log::info!("Queue connected...");

    let mut pool: AsyncWorkerPool<AsyncQueue<BackendSqlXPg>> = AsyncWorkerPool::builder()
        .number_of_workers(10_u32)
        .queue(queue.clone())
        .build();

    log::info!("Pool created ...");

    pool.start().await;
    log::info!("Workers started ...");

    let task1 = MyTask::new(0);
    let task2 = MyTask::new(20_000);
    let task3 = MyFailingTask::new(50_000);

    queue
        .insert_task(&task1 as &dyn AsyncRunnable)
        .await
        .unwrap();

    queue
        .insert_task(&task2 as &dyn AsyncRunnable)
        .await
        .unwrap();

    queue
        .insert_task(&task3 as &dyn AsyncRunnable)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(100)).await;
}
