<p align="center"><img src="https://raw.githubusercontent.com/ayrat555/fang/master/logo.png" alt="fang" height="300px"></p>

[![Crates.io][s1]][ci] [![docs page][docs-badge]][docs] ![test][ga-test] ![style][ga-style]

# Fang

Background task processing library for Rust. It can use PostgreSQL, SQLite or MySQL as an asyncronous task queue.

## Key Features

Here are some of the fang's key features:

- Async and threaded workers.
  Workers can be started in threads (threaded workers) or `tokio` tasks (async workers)
- Scheduled tasks.
  Tasks can be scheduled at any time in the future
- Periodic (CRON) tasks.
  Tasks can be scheduled using cron expressions
- Unique tasks.
  Tasks are not duplicated in the queue if they are unique
- Single-purpose workers.
  Tasks are stored in a single table but workers can execute only tasks of the specific type
- Retries.
  Tasks can be retried with a custom backoff mode

## Installation

1. Add this to your Cargo.toml

#### the Blocking feature

```toml
[dependencies]
fang = { version = "0.11.0-rc1" , features = ["blocking"], default-features = false }
```

#### the Asynk feature

- PostgreSQL as a queue

```toml
[dependencies]
fang = { version = "0.11.0-rc1" , features = ["asynk-postgres"], default-features = false }
```

- SQLite as a queue

```toml
[dependencies]
fang = { version = "0.11.0-rc1" , features = ["asynk-sqlite"], default-features = false }
```

- MySQL as a queue

```toml
[dependencies]
fang = { version = "0.11.0-rc1" , features = ["asynk-mysql"], default-features = false }
```

#### the Asynk feature with derive macro

Substitute `database` with your desired backend.

```toml
[dependencies]
fang = { version = "0.11.0-rc1" , features = ["asynk-{database}", "derive-error" ], default-features = false }
```

#### All features

```toml
fang = { version = "0.11.0-rc1" }
```

_Supports rustc 1.77+_

1. Create the `fang_tasks` table in the database. The migration of each database can be found in `fang/{database}-migrations` where `database` is `postgres`, `mysql` or `sqlite`.

Migrations can be also run as code, importing the feature `migrations-{database}` being the `database` the backend queue you want to use.

```toml
[dependencies]
fang = { version = "0.11.0-rc1" , features = ["asynk-postgres", "migrations-postgres" ], default-features = false }
```

```rust
use fang::run_migrations_postgres;
run_migrations_postgres(&mut connection).unwrap();
```

## Usage

### Defining a task

#### Blocking feature

Every task should implement the `fang::Runnable` trait which is used by `fang` to execute it.

If you have a `CustomError`, it is recommended to implement `From<FangError>`. So this way you can use [? operator](https://stackoverflow.com/questions/42917566/what-is-this-question-mark-operator-about#42921174) inside the `run` function available in `fang::Runnable` trait.

You can easily implement it with the macro `ToFangError`. This macro is only available in the feature `derive-error`.

```rust
use fang::FangError;
use fang::Runnable;
use fang::typetag;
use fang::PgConnection;
use fang::serde::{Deserialize, Serialize};
use fang::ToFangError;
use std::fmt::Debug;


#[derive(Debug, ToFangError)]
enum CustomError {
    ErrorOne(String),
    ErrorTwo(u32),
}

fn my_func(num : u16) -> Result<(), CustomError> {
    if num == 0 {
        Err(CustomError::ErrorOne("is zero".to_string()))
    }

    if num > 500 {
        Err(CustomError::ErrorTwo(num))
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
struct MyTask {
    pub number: u16,
}

#[typetag::serde]
impl Runnable for MyTask {
    fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
        println!("the number is {}", self.number);

        my_func(self.number)?;
        // You can use ? operator because
        // From<FangError> is implemented thanks to ToFangError derive macro.

        Ok(())
    }

    // If `uniq` is set to true and the task is already in the storage, it won't be inserted again
    // The existing record will be returned for for any insertions operaiton
    fn uniq(&self) -> bool {
        true
    }

    // This will be useful if you want to filter tasks.
    // the default value is `common`
    fn task_type(&self) -> String {
        "my_task".to_string()
    }

    // This will be useful if you would like to schedule tasks.
    // default value is None (the task is not scheduled, it's just executed as soon as it's inserted)
    fn cron(&self) -> Option<Scheduled> {
        let expression = "0/20 * * * Aug-Sep * 2022/1";
        Some(Scheduled::CronPattern(expression.to_string()))
    }

    // the maximum number of retries. Set it to 0 to make it not retriable
    // the default value is 20
    fn max_retries(&self) -> i32 {
        20
    }

    // backoff mode for retries
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
```

As you can see from the example above, the trait implementation has `#[typetag::serde]` attribute which is used to deserialize the task.

The second parameter of the `run` function is a struct that implements `fang::Queueable`. You can re-use it to manipulate the task queue, for example, to add a new job during the current job's execution. If you don't need it, just ignore it.

#### Asynk feature

Every task should implement `fang::AsyncRunnable` trait which is used by `fang` to execute it.

Be careful not to call two implementations of the AsyncRunnable trait with the same name, because it will cause a failure in the `typetag` crate.

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
    // this func is optional
    // Default task_type is common
    fn task_type(&self) -> String {
        "my-task-type".to_string()
    }


    // If `uniq` is set to true and the task is already in the storage, it won't be inserted again
    // The existing record will be returned for for any insertions operaiton
    fn uniq(&self) -> bool {
        true
    }

    // This will be useful if you would like to schedule tasks.
    // default value is None (the task is not scheduled, it's just executed as soon as it's inserted)
    fn cron(&self) -> Option<Scheduled> {
        let expression = "0/20 * * * Aug-Sep * 2022/1";
        Some(Scheduled::CronPattern(expression.to_string()))
    }

    // the maximum number of retries. Set it to 0 to make it not retriable
    // the default value is 20
    fn max_retries(&self) -> i32 {
        20
    }

    // backoff mode for retries
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
```

In both modules, tasks can be scheduled to be executed once. Use `Scheduled::ScheduleOnce` enum variant.

Datetimes and cron patterns are interpreted in the UTC timezone. So you should introduce the offset to schedule in a different timezone.

Example:

If your timezone is UTC + 2 and you want to schedule at 11:00:

```rust
let expression = "0 0 9 * * * *";
```

### Enqueuing a task

#### the Blocking feature

To enqueue a task use `Queue::enqueue_task`

```rust
use fang::Queue;

// create a r2d2 pool

// create a fang queue

let queue = Queue::builder().connection_pool(pool).build();

let task_inserted = queue.insert_task(&MyTask::new(1)).unwrap();
```

#### the Asynk feature

To enqueue a task use `AsyncQueueable::insert_task`.

For Postgres backend:

```rust
use fang::asynk::async_queue::AsyncQueue;
use fang::AsyncRunnable;

// Create an AsyncQueue
let max_pool_size: u32 = 2;

let mut queue = AsyncQueue::builder()
    // Postgres database url
    .uri("postgres://postgres:postgres@localhost/fang")
    // Max number of connections that are allowed
    .max_pool_size(max_pool_size)
    .build();

// Always connect first in order to perform any operation
queue.connect().await.unwrap();
```

Encryption is always used with crate `rustls`. We plan to add the possibility of disabling it in the future.

```rust
// AsyncTask from the first example
let task = AsyncTask { 8 };
let task_returned = queue
    .insert_task(&task as &dyn AsyncRunnable)
    .await
    .unwrap();
```

### Starting workers

#### the Blocking feature

Every worker runs in a separate thread. In case of panic, they are always restarted.

Use `WorkerPool` to start workers. Use `WorkerPool::builder` to create your worker pool and run tasks.

```rust
use fang::WorkerPool;
use fang::Queue;

// create a Queue

let mut worker_pool = WorkerPool::<Queue>::builder()
    .queue(queue)
    .number_of_workers(3_u32)
    // if you want to run tasks of the specific kind
    .task_type("my_task_type")
    .build();

worker_pool.start();
```

#### the Asynk feature

Every worker runs in a separate `tokio` task. In case of panic, they are always restarted.
Use `AsyncWorkerPool` to start workers.

```rust
use fang::asynk::async_worker_pool::AsyncWorkerPool;

// Need to create a queue
// Also insert some tasks

let mut pool: AsyncWorkerPool<AsyncQueue> = AsyncWorkerPool::builder()
        .number_of_workers(max_pool_size)
        .queue(queue.clone())
        // if you want to run tasks of the specific kind
        .task_type("my_task_type")
        .build();

pool.start().await;
```

Check out:

- [Simple Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/blocking/simple_worker) - simple worker example
- [Simple Cron Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/blocking/simple_cron_worker) - simple worker example
- [Simple Async Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/asynk/simple_async_worker) - simple async worker example
- [Simple Cron Async Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/asynk/simple_cron_async_worker) - simple async worker example
- [El Monitorro](https://github.com/ayrat555/el_monitorro) - telegram feed reader. It uses the Fang's blocking module to synchronize feeds and deliver updates to users.
- [weather_bot_rust](https://github.com/pxp9/weather_bot_rust) - A bot that provides weather info. It uses the Fang's asynk module to process updates from Telegram users and schedule weather info.

### Configuration

#### Blocking feature

Just use `TypeBuilder` for `WorkerPool`.

#### Asynk feature

Just use `TypeBuilder` for `AsyncWorkerPool`.

### Configuring the type of workers

### Configuring retention mode

By default, all successfully finished tasks are removed from the DB, failed tasks aren't.

There are three retention modes you can use:

```rust
pub enum RetentionMode {
    KeepAll,        // doesn't remove tasks
    RemoveAll,      // removes all tasks
    RemoveFinished, // default value
}
```

Set retention mode with worker pools `TypeBuilder` in both modules.

### Configuring sleep values

#### Blocking feature

You can use use `SleepParams` to configure sleep values:

```rust
pub struct SleepParams {
    pub sleep_period: Duration,     // default value is 5 seconds
    pub max_sleep_period: Duration, // default value is 15 seconds
    pub min_sleep_period: Duration, // default value is 5 seconds
    pub sleep_step: Duration,       // default value is 5 seconds
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
```

Set sleep params with worker pools `TypeBuilder` in both modules.

## Contributing

1. [Fork it!](https://github.com/ayrat555/fang/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

### Running tests locally

- Install diesel_cli.

```sh
cargo install diesel_cli --no-default-features --features "postgres sqlite mysql"
```

- Install docker on your machine.

- Install SQLite 3 on your machine.

- Setup databases for testing.

```sh
make -j db
```

- Run tests. `make db` does not need to be run in between each test cycle.

```sh
make -j tests
```

- Run dirty/long tests.

```sh
make -j ignored
```

- Take down databases.

```sh
make -j stop
```

The `-j` flag in the above examples enables parallelism for `make`, is not necessary but highly recommended.

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
