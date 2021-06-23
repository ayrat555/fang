use crate::executor::Executor;
use crate::postgres::Postgres;
use std::thread;

struct JobPool {
    pub number_of_workers: u16,
    pub name: String,
}

struct JobThread {
    pub name: String,
}

impl JobPool {
    pub fn new(number_of_workers: u16, name: String) -> Self {
        Self {
            number_of_workers,
            name,
        }
    }

    pub fn start(&self) {
        for idx in 1..self.number_of_workers {
            let name = format!("{}{}", self.name, idx);

            spawn_in_pool(name)
        }
    }
}

impl JobThread {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Drop for JobThread {
    fn drop(&mut self) {
        spawn_in_pool(self.name.clone())
    }
}

fn spawn_in_pool(name: String) {
    let mut builder = thread::Builder::new().name(name.clone());
    builder = builder;

    builder
        .spawn(move || {
            // when _job is dropped, it will be restarted (see Drop trait impl)
            let _job = JobThread::new(name);

            let postgres = Postgres::new(None);

            Executor::new(postgres).run_tasks()
        })
        .unwrap();
}
