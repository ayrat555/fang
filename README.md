<p align="center"><img src="logo.png" alt="fang" height="300px"></p>

[![Crates.io][s1]][ci] [![docs page][docs-badge]][docs] ![test][ga-test] ![style][ga-style]

# Fang

Background task processing library for Rust. It uses Postgres DB as a task queue.

## Features
- Asynk feature uses `tokio`. Workers are started in tokio tasks.
- Blocking feature uses `std::thread`. Workers are started in a separated threads.

## Installation

1. Add this to your Cargo.toml


#### Blocking feature
```toml
[dependencies]
fang = { version = "0.7" , features = ["blocking"], default-features = false }
```

#### Asynk feature
```toml
[dependencies]
fang = { version = "0.7" , features = ["asynk"], default-features = false }
```

#### Both features
```toml
fang = { version = "0.7" }
```

*Supports rustc 1.62+*

2. Create `fang_tasks` table in the Postgres database. The migration can be found in [the migrations directory](https://github.com/ayrat555/fang/blob/master/migrations/2021-06-05-112912_create_fang_tasks/up.sql).

## Usage

### Defining a task

#### Blocking feature
Every task should implement `fang::Runnable` trait which is used by `fang` to execute it.

```rust
use fang::Error;
use fang::Runnable;
use fang::typetag;
use fang::PgConnection;
use fang::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
struct MyTask {
    pub number: u16,
}

#[typetag::serde]
impl Runnable for MyTask {
    fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
        println!("the number is {}", self.number);

        Ok(())
    }
}
```

As you can see from the example above, the trait implementation has `#[typetag::serde]` attribute which is used to deserialize the task.

The second parameter  of the `run` function is diesel's PgConnection, You can re-use it to manipulate the task queue, for example, to add a new job during the current job's execution. Or you can just re-use it in your own queries if you're using diesel. If you don't need it, just ignore it.


#### Asynk feature
Every task should implement `fang::AsyncRunnable` trait which is used by `fang` to execute it.

Also be careful to not to call with the same name two impl of AsyncRunnable, because will cause a fail with typetag.
```rust
use fang::AsyncRunnable;
use fang::asynk::async_queue::AsyncQueueable;
use fang::serde::{Deserialize, Serialize};
use fang::async_trait;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
struct AsyncTask {
  pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncTask {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
        Ok(())
    }
    // this func is optional to impl
    // Default task-type it is common
    fn task_type(&self) -> String {
        "my-task-type".to_string()
    }
}
```
### Enqueuing a task

#### Blocking feature
To enqueue a task use `Queue::enqueue_task`


```rust
use fang::Queue;

...

Queue::enqueue_task(&MyTask { number: 10 }).unwrap();

```

The example above creates a new postgres connection on every call. If you want to reuse the same postgres connection to enqueue several tasks use Postgres struct instance:

```rust
let queue = Queue::new();

for id in &unsynced_feed_ids {
    queue.push_task(&SyncFeedMyTask { feed_id: *id }).unwrap();
}

```

Or you can use `PgConnection` struct:

```rust
Queue::push_task_query(pg_connection, &new_task).unwrap();
```

#### Asynk feature
To enqueue a task use `AsyncQueueable::insert_task`,
depending of the backend that you prefer you will need to do it with a specific queue.

For Postgres backend.
```rust
use fang::asynk::async_queue::AsyncQueue;
use fang::NoTls;
use fang::AsyncRunnable;

// Create a AsyncQueue
let max_pool_size: u32 = 2;

let mut queue = AsyncQueue::builder()
    // Postgres database url
    .uri("postgres://postgres:postgres@localhost/fang")
    // Max number of connections that are allowed
    .max_pool_size(max_pool_size)
    // false if would like Uniqueness in tasks
    .duplicated_tasks(true)
    .build();

// Always connect first in order to perform any operation
queue.connect(NoTls).await.unwrap();

```
For easy example we are using NoTls type, if for some reason you would like to encrypt postgres traffic.

You can implement a Tls type.

It is well documented for [openssl](https://docs.rs/postgres-openssl/latest/postgres_openssl/) and [native-tls](https://docs.rs/postgres-native-tls/latest/postgres_native_tls/)

```rust
// AsyncTask from first example
let task = AsyncTask { 8 };
let task_returned = queue
  .insert_task(&task as &dyn AsyncRunnable)
  .await
  .unwrap();
```

### Starting workers

#### Blocking feature
Every worker runs in a separate thread. In case of panic, they are always restarted.

Use `WorkerPool` to start workers. `WorkerPool::new` accepts one parameter - the number of workers.


```rust
use fang::WorkerPool;

WorkerPool::new(10).start();
```

Use `shutdown` to stop worker threads, they will try to finish in-progress tasks.

```rust

use fang::WorkerPool;

worker_pool = WorkerPool::new(10).start().unwrap;

worker_pool.shutdown()
```

Using a library like [signal-hook][signal-hook], it's possible to gracefully shutdown a worker. See the
Simple Worker for an example implementation.

#### Asynk feature
Every worker runs in a separate tokio task. In case of panic, they are always restarted.
Use `AsyncWorkerPool` to start workers.

```rust
use fang::asynk::async_worker_pool::AsyncWorkerPool;

// Need to create a queue
// Also insert some tasks

let mut pool: AsyncWorkerPool<AsyncQueue<NoTls>> = AsyncWorkerPool::builder()
        .number_of_workers(max_pool_size)
        .queue(queue.clone())
        .build();

pool.start().await;
```


Check out:

- [Simple Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/simple_worker) - simple worker example
- [Simple Async Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/simple_async_worker) - simple async worker example
- [El Monitorro](https://github.com/ayrat555/el_monitorro) - telegram feed reader. It uses Fang to synchronize feeds and deliver updates to users.

### Configuration

#### Blocking feature

To configure workers, instead of `WorkerPool::new` which uses default values, use `WorkerPool.new_with_params`. It accepts two parameters - the number of workers and `WorkerParams` struct.

#### Asynk feature

Just use `TypeBuilder` done for `AsyncWorkerPool`.

### Configuring the type of workers

#### Blocking feature

You can start workers for a specific types of tasks. These workers will be executing only tasks of the specified type.

Add `task_type` method to the `Runnable` trait implementation:

```rust
...

#[typetag::serde]
impl Runnable for MyTask {
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


#### Asynk feature

Same as Blocking feature.

Use `TypeBuilder` for `AsyncWorker`.

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

#### Blocking feature

```rust
let mut worker_params = WorkerParams::new();
worker_params.set_retention_mode(RetentionMode::RemoveAll);

WorkerPool::new_with_params(10, worker_params).start();
```
#### Asynk feature

Set it in `AsyncWorker` `TypeBuilder`.

### Configuring sleep values

#### Blocking feature

You can use use `SleepParams` to confugure sleep values:

```rust
pub struct SleepParams {
    pub sleep_period: Duration,     \\ default value is 5 seconds
    pub max_sleep_period: Duration, \\ default value is 15 seconds
    pub min_sleep_period: Duration, \\ default value is 5 seconds
    pub sleep_step: Duration,       \\ default value is 5 seconds
}
```

If there are no tasks in the DB, a worker sleeps for `sleep_period` and each time this value increases by `sleep_step` until it reaches `max_sleep_period`. `min_sleep_period` is the initial value for `sleep_period`. All values are in seconds.


Use `set_sleep_params` to set it:
```rust
let sleep_params = SleepParams {
    sleep_period: Duration::from_secs(2),
    max_sleep_period: Duration::from_secs(6),
    min_sleep_period: Duration::from_secs(2),
    sleep_step: Duration::from_secs(1),
};
let mut worker_params = WorkerParams::new();
worker_params.set_sleep_params(sleep_params);

WorkerPool::new_with_params(10, worker_params).start();
```
#### Asynk feature

Set it in `AsyncWorker` `TypeBuilder`.

## Periodic Tasks

Fang can add tasks to `fang_tasks` periodically. To use this feature first run [the migration with `fang_periodic_tasks` table](https://github.com/ayrat555/fang/tree/master/migrations/2021-07-24-050243_create_fang_periodic_tasks/up.sql).

Usage example:

#### Blocking feature

```rust
use fang::Scheduler;
use fang::Queue;

let queue = Queue::new();

queue
     .push_periodic_task(&SyncMyTask::default(), 120000)
     .unwrap();

queue
     .push_periodic_task(&DeliverMyTask::default(), 60000)
     .unwrap();

Scheduler::start(Duration::from_secs(10), Duration::from_secs(5));
```

In the example above, `push_periodic_task` is used to save the specified task to the `fang_periodic_tasks` table which will be enqueued (saved to `fang_tasks` table) every specied number of milliseconds.

`Scheduler::start(Duration::from_secs(10), Duration::from_secs(5))` starts scheduler. It accepts two parameters:
- Db check period
- Acceptable error limit - |current_time - scheduled_time| < error

#### Asynk feature
```rust
use fang::asynk::async_scheduler::Scheduler;
use fang::asynk::async_queue::AsyncQueueable;
use fang::asynk::async_queue::AsyncQueue;
use std::time::Duration; 
use chrono::Duration as OtherDuration;

// Build a AsyncQueue as before

let schedule_in_future = Utc::now() + OtherDuration::seconds(5);

let _periodic_task = queue.insert_periodic_task(
    &AsyncTask { number: 1 },
    schedule_in_future,
    10000,  // period in milliseconds
)
.await;

let check_period: u64 = 1;
let error_margin_seconds: u64 = 2;

let mut scheduler = Scheduler::builder()
    .check_period(Duration::from_secs(check_period))
    .error_margin_seconds(Duration::from_secs(error_margin_seconds))
    .queue(&mut queue as &mut dyn AsyncQueueable)
    .build();

// Add some more task in other thread or before loop

// Scheduler Loop
scheduler.start().await.unwrap();
```


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

## Authors

- Ayrat Badykov (@ayrat555)

- Pepe Márquez (@pxp9)


[s1]: https://img.shields.io/crates/v/fang.svg
[docs-badge]: https://img.shields.io/badge/docs-website-blue.svg
[ci]: https://crates.io/crates/fang
[docs]: https://docs.rs/fang/
[ga-test]: https://github.com/ayrat555/fang/actions/workflows/rust.yml/badge.svg
[ga-style]: https://github.com/ayrat555/fang/actions/workflows/style.yml/badge.svg
[signal-hook]: https://crates.io/crates/signal-hook
