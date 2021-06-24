<p align="center"><img src="logo.png" alt="fang" height="300px"></p>

# Fang

Background job processing library for Rust.

Currently, it uses postgres to store state. But in the future, more backends will be supported.

## Installation

1. Add this to your Cargo.toml


```toml
[dependencies]
fang = "0.1"
typetag = "0.1"
serde = { version = "1.0", features = ["derive"] }
```

2. Create `fang_tasks` table in the postgres database. The migration can be found in [the migrations directory](https://github.com/ayrat555/fang/blob/master/migrations/2021-06-05-112912_create_fang_tasks/up.sql).

## Usage

### Defining a job

Every job should implement `fang::Runnable` trait which is used by `fang` to execut it.


```rust
    use fang::Error;
    use fang::Runnable;
    use serde::{Deserialize, Serialize};


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

As you can see from the example above, trait implementation has `#[typetag::serde]` which is used to deserialize the job.

### Enqueuing a job

To enqueue a job use `Postgres::enqueue_task`


```rust
use fang::Postgres;

...

Postgres::enqueue_task(&Job { number: 10 }).unwrap();

```


### Starting workers

Every worker is executed in a separate thread. In case of panic, they are always restarted.

Use `WorkerPool::new` to start workers. It accepts two parameters - the number of workers and the prefix for worker thread name.


```rust
use fang::WorkerPool;

WorkerPool::new(10, "sync".to_string()).start();
```

## Potential/future features

  * Extendable/new backends
  * Workers for specific types of tasks. Currently, each workers execute all types of tasks
  * Configurable db records retention. Currently, fang doesn't remove tasks from the db.

## Contributing

1. [Fork it!](https://github.com/ayrat555/fang/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Author

Ayrat Badykov (@ayrat555)
