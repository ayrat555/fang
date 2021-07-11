<p align="center"><img src="logo.png" alt="fang" height="300px"></p>

[![Crates.io][s1]][ci] [![docs page][docs-badge]][docs] ![test][ga-test] ![style][ga-style]

# Fang

Background job processing library for Rust.

Currently, it uses Postgres to store state. But in the future, more backends will be supported.

Note that the README follows the master branch, to see instructions for the latest published version, check [crates.io](https://crates.io/crates/fang).

## Installation

1. Add this to your Cargo.toml


```toml
[dependencies]
fang = "0.3.1"
```

2. Create `fang_tasks` table in the Postgres database. The migration can be found in [the migrations directory](https://github.com/ayrat555/fang/blob/master/migrations/2021-06-05-112912_create_fang_tasks/up.sql).

## Usage

### Defining a job

Every job should implement `fang::Runnable` trait which is used by `fang` to execute it.


```rust
use fang::Error;
use fang::Runnable;
use fang::{Deserialize, Serialize};
use fang::typetag;


#[derive(Serialize, Deserialize)]
struct Job {
    pub number: u16,
}

#[typetag::serde]
impl Runnable for Job {
    fn run(&self) -> Result<(), Error> {
        println!("the number is {}", self.number);

        Ok(())
    }
}
```

As you can see from the example above, the trait implementation has `#[typetag::serde]` attribute which is used to deserialize the job.

### Enqueuing a job

To enqueue a job use `Postgres::enqueue_task`


```rust
use fang::Postgres;

...

Postgres::enqueue_task(&Job { number: 10 }).unwrap();

```

The example above creates a new postgres connection on every call. If you want to reuse the same postgres connection to enqueue several jobs use Postgres struct instance:

```rust
let postgres = Postgres::new();

for id in &unsynced_feed_ids {
    postgres.push_task(&SyncFeedJob { feed_id: *id }).unwrap();
}

```

### Starting workers

Every worker runs in a separate thread. In case of panic, they are always restarted.

Use `WorkerPool` to start workers. `WorkerPool::new` accepts one parameter - the number of workers.


```rust
use fang::WorkerPool;

WorkerPool::new(10).start();
```

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

## Potential/future features

  * Retries
  * Scheduled tasks
  * Extendable/new backends

## Contributing

1. [Fork it!](https://github.com/ayrat555/fang/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Author

Ayrat Badykov (@ayrat555)


[s1]: https://img.shields.io/crates/v/fang.svg
[docs-badge]: https://img.shields.io/badge/docs-website-blue.svg
[ci]: https://crates.io/crates/fang
[docs]: https://docs.rs/fang/
[ga-test]: https://github.com/ayrat555/fang/actions/workflows/rust.yml/badge.svg
[ga-style]: https://github.com/ayrat555/fang/actions/workflows/style.yml/badge.svg
