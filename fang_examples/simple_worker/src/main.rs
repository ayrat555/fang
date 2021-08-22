use dotenv::dotenv;
use fang::Queue;
use fang::RetentionMode;
use fang::WorkerParams;
use fang::WorkerPool;
use simple_worker::MyJob;

fn main() {
    dotenv().ok();

    env_logger::init();

    let mut worker_params = WorkerParams::new();
    worker_params.set_retention_mode(RetentionMode::KeepAll);

    WorkerPool::new_with_params(2, worker_params).start();

    let queue = Queue::new();

    queue.push_task(&MyJob::new(1)).unwrap();
    queue.push_task(&MyJob::new(1000)).unwrap();

    std::thread::park();
}
