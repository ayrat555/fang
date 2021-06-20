use crate::executor::Executor;
use crate::postgres::Postgres;
use std::thread;
use std::thread::JoinHandle;

struct Scheduler {
    pub number_of_workers: u16,
    pub handles: Option<Vec<JoinHandle<()>>>,
}

impl Scheduler {
    pub fn new(number_of_workers: u16) -> Self {
        Self {
            number_of_workers,
            handles: None,
        }
    }

    pub fn start(&mut self) {
        let mut handles: Vec<JoinHandle<()>> = vec![];

        for _ in 1..self.number_of_workers {
            let handle = thread::spawn(|| {
                let postgres = Postgres::new(None);

                Executor::new(postgres).run_tasks()
            });

            handles.push(handle);
        }

        self.handles = Some(handles);
    }
}
