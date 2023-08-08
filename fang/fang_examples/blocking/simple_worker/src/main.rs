use diesel::r2d2;
use dotenv::dotenv;
use fang::PgConnection;
use fang::Queue;
use fang::Queueable;
use fang::RetentionMode;
use fang::WorkerPool;
use simple_worker::MyFailingTask;
use simple_worker::MyTask;
use std::env;
use std::thread::sleep;
use std::time::Duration;

pub fn connection_pool(pool_size: u32) -> r2d2::Pool<r2d2::ConnectionManager<PgConnection>> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let manager = r2d2::ConnectionManager::<PgConnection>::new(database_url);

    r2d2::Pool::builder()
        .max_size(pool_size)
        .build(manager)
        .unwrap()
}

fn main() {
    dotenv().ok();

    env_logger::init();

    let queue = Queue::builder().connection_pool(connection_pool(3)).build();

    let mut worker_pool = WorkerPool::<Queue>::builder()
        .queue(queue)
        .retention_mode(RetentionMode::KeepAll)
        .number_of_workers(3_u32)
        .task_type("worker_pool_test".to_string())
        .build();

    worker_pool.queue.insert_task(&MyTask::new(1)).unwrap();
    worker_pool.queue.insert_task(&MyTask::new(1000)).unwrap();

    worker_pool
        .queue
        .insert_task(&MyFailingTask::new(5000))
        .unwrap();

    worker_pool.start().unwrap();

    sleep(Duration::from_secs(100))
}
