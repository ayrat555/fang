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
fang = { version = "0.9" , features = ["blocking"], default-features = false }
```

#### Asynk feature
```toml
[dependencies]
fang = { version = "0.9" , features = ["asynk"], default-features = false }
```

#### Both features
```toml
fang = { version = "0.9" }
```

*Supports rustc 1.62+*

2. Create `fang_tasks` table in the Postgres database. The migration can be found in [the migrations directory](https://github.com/ayrat555/fang/blob/master/migrations/2022-08-20-151615_create_fang_tasks/up.sql).

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
    fn run(&self, _queue: &dyn Queueable) -> Result<(), Error> {
        println!("the number is {}", self.number);

        Ok(())
    }
    
    // If you want to make the tasks of this type uniq.
    fn uniq(&self) -> bool {
      true
    }
    
    // This will be useful if you want to filter tasks.
    // default value: "common".to_string()
    fn task_type(&self) -> String {
      "my_task".to_string()
    }
    
    // This will be useful if you would like to schedule tasks.
    // default value: None (task is not schedule just executes when it is fetched)
    fn cron(&self) -> Option<Scheduled> {
        //               sec  min   hour   day of month   month   day of week   year
        //               be careful works only with UTC hour.
        //               https://www.timeanddate.com/worldclock/timezone/utc
        let expression = "0/20 * * * Aug-Sep * 2022/1";
        Some(Scheduled::CronPattern(expression.to_string()))
    }
}
```

As you can see from the example above, the trait implementation has `#[typetag::serde]` attribute which is used to deserialize the task.

The second parameter  of the `run` function is a is an struct that implements fang::Queueable (fang::Queue for example), You can re-use it to manipulate the task queue, for example, to add a new job during the current job's execution. If you don't need it, just ignore it.


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


    // If you want to make the tasks of this type uniq.
    fn uniq(&self) -> bool {
      true
    }

    // This will be useful if you would like to schedule tasks.
    // default value: None (task is not schedule just executes when it is fetched)
    fn cron(&self) -> Option<Scheduled> {
        //               sec  min   hour   day of month   month   day of week   year
        //               be careful works only with UTC hour.
        //               https://www.timeanddate.com/worldclock/timezone/utc
        let expression = "0/20 * * * Aug-Sep * 2022/1";
        Some(Scheduled::CronPattern(expression.to_string()))
    }
}
```

In both modules, tasks can be schedule to be execute once. Use ```Scheduled::ScheduleOnce``` enum variant to schedule in specific datetime.

Datetimes and cron pattern are interpreted in UTC timezone. So you should introduce an offset to schedule in the desire hour.

Example:

If your hour is UTC + 2 and you would like to schedule at 11:00 all days, your expression will be this one.

```rust
 let expression = "0 0 9 * * * *";
```


### Enqueuing a task

#### Blocking feature
To enqueue a task use `Queue::enqueue_task`


For Postgres Backend.
```rust
use fang::Queue;

// create a r2d2 pool

// create a fang queue

 let queue = Queue::builder().connection_pool(pool).build();

 let task_inserted = queue.insert_task(&MyTask::new(1)).unwrap();

```

`Queue::insert_task` method will insert a task with uniqueness or not it depends on `uniq` method defined in a task.
If uniq is set to true and the task is already in storage this will return the task in the storage.


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

Use `WorkerPool` to start workers. Use `WorkerPool::builder` to create your worker pool and run tasks.


```rust
use fang::WorkerPool;
use fang::Queue;

// create a Queue

let mut worker_pool = WorkerPool::<Queue>::builder()
    .queue(queue)
    .number_of_workers(3_u32)
    .build();

worker_pool.start();
```

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

- [Simple Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/blocking/simple_worker) - simple worker example
- [Simple Cron Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/blocking/simple_cron_worker) - simple worker example
- [Simple Async Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/asynk/simple_async_worker) - simple async worker example
- [Simple Cron Async Worker Example](https://github.com/ayrat555/fang/tree/master/fang_examples/asynk/simple_cron_async_worker) - simple async worker example
- [El Monitorro](https://github.com/ayrat555/el_monitorro) - telegram feed reader. It uses Fang blocking module to synchronize feeds and deliver updates to users.
- [weather_bot_rust](https://github.com/pxp9/weather_bot_rust) - A bot that provides weather info. It uses Fang asynk module to process updates from Telegram users and schedule weather info.

### Configuration

#### Blocking feature

Just use `TypeBuilder` done for `WorkerPool`.

#### Asynk feature

Just use `TypeBuilder` done for `AsyncWorkerPool`.

### Configuring the type of workers

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

Set retention mode with worker pools `TypeBuilder` in both modules.

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
```

Set sleep params with worker pools `TypeBuilder` in both modules.

## Contributing

1. [Fork it!](https://github.com/ayrat555/fang/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

### Running tests locally
- Install diesel_cli.
```
cargo install diesel_cli
```
- Install docker in your machine.

- Run a Postgres docker container. (See in Makefile.)
```
make db
```

- Run the migrations
```
make diesel
```

- Run tests
```
make tests
```

- Run dirty//long tests, DB must be recreated afterwards.
```
make ignored
```

- Kill docker container
```
make stop
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
