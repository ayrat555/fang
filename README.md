<p align="center"><img src="logo.png" alt="fang" height="300px"></p>

[![Crates.io][s1]][ci] [![docs page][docs-badge]][docs] ![test][ga-test] ![style][ga-style]

# Fang

Background job processing library for Rust. It uses Postgres DB as a task queue.


## Installation

1. Add this to your Cargo.toml


```toml
[dependencies]
fang = "0.4.1"
serde = { version = "1.0", features = ["derive"] }
```

2. Create `fang_tasks` table in the Postgres database. The migration can be found in [the migrations directory](https://github.com/ayrat555/fang/blob/master/migrations/2021-06-05-112912_create_fang_tasks/up.sql).

## Usage

### Defining a job

Every job should implement `fang::Runnable` trait which is used by `fang` to execute it.

```rust
use fang::Error;
use fang::Runnable;
use fang::typetag;
use fang::PgConnection;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Job {
    pub number: u16,
}

#[typetag::serde]
impl Runnable for Job {
    fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
        println!("the number is {}", self.number);

        Ok(())
    }
}
```

As you can see from the example above, the trait implementation has `#[typetag::serde]` attribute which is used to deserialize the job.

The second parameter  of the `run` function is diesel's PgConnection, You can re-use it to manipulate the job queue, for example, to add a new job during the current job's execution. Or you can just re-use it in your own queries if you're using diesel. If you don't need it, just ignore it.

### Enqueuing a job

To enqueue a job use `Queue::enqueue_task`


```rust
use fang::Queue;

...

Queue::enqueue_task(&Job { number: 10 }).unwrap();

```

The example above creates a new postgres connection on every call. If you want to reuse the same postgres connection to enqueue several jobs use Postgres struct instance:

```rust
let queue = Queue::new();

for id in &unsynced_feed_ids {
    queue.push_task(&SyncFeedJob { feed_id: *id }).unwrap();
}

```

Or you can use `PgConnection` struct:

```rust
Queue::push_task_query(pg_connection, &new_job).unwrap();
```

### Starting workers

Every worker runs in a separate thread. In case of panic, they are always restarted.

Use `WorkerPool` to start workers. `WorkerPool::new` accepts one parameter - the number of workers.


```rust
use fang::WorkerPool;

WorkerPool::new(10).start();
```

Using a library like [signal-hook][signal-hook], it's possible to gracefully shutdown a worker. See the
Simple Worker for an example implementation.

Check out:

- [Simple Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/simple_worker) - simple worker example
- [El Monitorro](https://github.com/ayrat555/el_monitorro) - telegram feed reader. It uses Fang to synchronize feeds and deliver updates to users.

### Configuration

To configure workers, instead of `WorkerPool::new` which uses default values, use `WorkerPool.new_with_params`. It accepts two parameters - the number of workers and `WorkerParams` struct.

### Configuring the type of workers

You can start workers for a specific types of tasks. These workers will be executing only tasks of the specified type.

Add `task_type` method to the `Runnable` trait implementation:

```rust
...

#[typetag::serde]
impl Runnable for Job {
    fn run(&self) -> Result<(), Error> {
        println!("the number is {}", self.number);

        Ok(())
    }

    fn task_type(&self) -> String {
        "number".to_string()
    }
}
```

Set `task_type` to the `WorkerParamas`:

```rust
let mut worker_params = WorkerParams::new();
worker_params.set_task_type("number".to_string());

WorkerPool::new_with_params(10, worker_params).start();
```

Without setting `task_type` workers will be executing any type of task.


### Configuring retention mode

By default, all successfully finished tasks are removed from the DB, failed tasks aren't.

There are three retention modes you can use:

```rust
pub enum RetentionMode {
    KeepAll,        \\ doesn't remove tasks
    RemoveAll,      \\ removes all tasks
    RemoveFinished, \\ default value
}
```

Set retention mode with `set_retention_mode`:

```rust
let mut worker_params = WorkerParams::new();
worker_params.set_retention_mode(RetentionMode::RemoveAll);

WorkerPool::new_with_params(10, worker_params).start();
```

### Configuring sleep values

You can use use `SleepParams` to confugure sleep values:

```rust
pub struct SleepParams {
    pub sleep_period: u64,     \\ default value is 5
    pub max_sleep_period: u64, \\ default value is 15
    pub min_sleep_period: u64, \\ default value is 5
    pub sleep_step: u64,       \\ default value is 5
}p
```

If there are no tasks in the DB, a worker sleeps for `sleep_period` and each time this value increases by `sleep_step` until it reaches `max_sleep_period`. `min_sleep_period` is the initial value for `sleep_period`. All values are in seconds.


Use `set_sleep_params` to set it:
```rust
let sleep_params = SleepParams {
    sleep_period: 2,
    max_sleep_period: 6,
    min_sleep_period: 2,
    sleep_step: 1,
};
let mut worker_params = WorkerParams::new();
worker_params.set_sleep_params(sleep_params);

WorkerPool::new_with_params(10, worker_params).start();
```

## Periodic Tasks

Fang can add tasks to `fang_tasks` periodically. To use this feature first run [the migration with `fang_periodic_tasks` table](https://github.com/ayrat555/fang/tree/master/migrations/2021-07-24-050243_create_fang_periodic_tasks/up.sql).

Usage example:

```rust
use fang::Scheduler;
use fang::Queue;

let queue = Queue::new();

queue
     .push_periodic_task(&SyncJob::default(), 120)
     .unwrap();

queue
     .push_periodic_task(&DeliverJob::default(), 60)
     .unwrap();

Scheduler::start(10, 5);
```

In the example above, `push_periodic_task` is used to save the specified task to the `fang_periodic_tasks` table which will be enqueued (saved to `fang_tasks` table) every specied number of seconds.

`Scheduler::start(10, 5)` starts scheduler. It accepts two parameters:
- Db check period in seconds
- Acceptable error limit in seconds - |current_time - scheduled_time| < error

## Contributing

1. [Fork it!](https://github.com/ayrat555/fang/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

### Running tests locally

```
cargo install diesel_cli

docker run --rm -d --name postgres -p 5432:5432 \
  -e POSTGRES_DB=fang \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest

DATABASE_URL=postgres://postgres:postgres@localhost/fang diesel migration run

// Run regular tests
cargo test --all-features

// Run dirty/long tests, DB must be recreated afterwards
cargo test --all-features -- --ignored --test-threads=1

docker kill postgres
```

## Author

Ayrat Badykov (@ayrat555)


[s1]: https://img.shields.io/crates/v/fang.svg
[docs-badge]: https://img.shields.io/badge/docs-website-blue.svg
[ci]: https://crates.io/crates/fang
[docs]: https://docs.rs/fang/
[ga-test]: https://github.com/ayrat555/fang/actions/workflows/rust.yml/badge.svg
[ga-style]: https://github.com/ayrat555/fang/actions/workflows/style.yml/badge.svg
[signal-hook]: https://crates.io/crates/signal-hook
