use dotenv::dotenv;
use fang::Queue;
use fang::RetentionMode;
use fang::WorkerParams;
use fang::WorkerPool;
use simple_worker::MyJob;
use signal_hook::{consts::signal::*, iterator::Signals};

fn main() {
    dotenv().ok();

    env_logger::init();

    let mut worker_params = WorkerParams::new();
    worker_params.set_retention_mode(RetentionMode::KeepAll);

    let mut worker_pool = WorkerPool::new_with_params(2, worker_params);
    worker_pool.start().unwrap();

    let queue = Queue::new();

    queue.push_task(&MyJob::new(1)).unwrap();
    queue.push_task(&MyJob::new(1000)).unwrap();

    let mut signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    for sig in signals.forever() {
        match sig {
            SIGINT => {
                println!("Received SIGINT");
                worker_pool.shutdown().unwrap();
                break;
            },
            SIGTERM => {
                println!("Received SIGTERM");
                worker_pool.shutdown().unwrap();
                break;
            },
            _ => unreachable!(format!("Received unexpected signal: {:?}", sig)),
        }
    }
}
