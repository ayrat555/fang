+++
title = "Fang, async background processing for Rust"
description = "Async background processing for rust with tokio and postgres"
date = 2022-08-06T08:00:00+00:00
updated = 2022-08-06T08:00:00+00:00
template = "blog/page.html"
sort_by = "weight"
weight = 1
draft = false

[taxonomies]
authors = ["Ayrat Badykov", "Pepe Márquez"]

[extra]
lead = 'Async background processing for rust with tokio and postgres'
images = []
+++

Even though the first stable version of Rust was released in 2015, there are still some holes in its ecosystem for solving common tasks. One of which is background processing.

In software engineering background processing is a common approach for solving several problems:

- Carry out periodic tasks. For example, deliver notifications, update cached values.
- Defer expensive work so your application stays responsive while performing calculations in the background

Most programming languages have go-to background processing frameworks/libraries. For example:

- Ruby - [sidekiq](https://github.com/mperham/sidekiq). It uses Redis as a job queue.
- Python - [dramatiq](https://github.com/Bogdanp/dramatiq). It uses RabbitMQ as a job queue.
- Elixir - [oban](https://github.com/sorentwo/oban). It uses a Postgres DB as a job queue.

The async programming (async/await) can be used for background processing but it has several major disadvantages if used directly:

- It doesn't give control of the number of tasks that are being executed at any given time. So a lot of spawned tasks can overload a thread/threads that they're started on.
- It doesn't provide any monitoring which can be useful to investigate your system and find bottlenecks
- Tasks are not persistent. So all enqueued tasks are lost on every application restart

To solve these shortcomings of the async programming we implemented the async processing in [the fang library](https://github.com/ayrat555/fang).

## Threaded Fang

Fang is a background processing library for rust. The first version of Fang was released exactly one year ago. Its key features were:

- Each worker is started in a separate thread
- A Postgres table is used as the task queue

This implementation was written for a specific use case - [el monitorro bot](https://github.com/ayrat555/el_monitorro). This specific implementation of background processing was proved by time. Each day it processes more and more feeds every minute (the current number is more than 3000). Some users host the bot on their infrastructure.

You can find out more about the threaded processing in fang in [this blog post](https://www.badykov.com/rust/fang/).

## Async Fang

<blockquote>
  <p>
Async provides significantly reduced CPU and memory overhead, especially for workloads with a large amount of IO-bound tasks, such as servers and databases. All else equal, you can have orders of magnitude more tasks than OS threads, because an async runtime uses a small amount of (expensive) threads to handle a large amount of (cheap) tasks
  </p>
  <footer><cite title="Async book">From the Rust's Async book</cite></footer>
</blockquote>

For some lightweight background tasks, it's cheaper to run them on the same thread using async instead of starting one thread per worker. That's why we implemented this kind of processing in fang. Its key features:

- Each worker is started as a tokio task
- If any worker fails during task execution, it's restarted
- Tasks are saved to a Postgres database. Instead of diesel, [tokio-postgres](https://github.com/sfackler/rust-postgres) is used to interact with a db. The threaded processing uses the [diesel](https://github.com/diesel-rs/diesel) ORM which blocks the thread.
- The implementation is based on traits so it's easy to implement additional backends (redis, in-memory) to store tasks.

## Usage

The usage is straightforward:

1. Define a serializable task by adding `serde` derives to a task struct.
2. Implement `AsyncRunnable` runnable trait for fang to be able to run it.
3. Start workers.
4. Enqueue tasks.

Let's go over each step.

### Define a job

```rust
use fang::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyTask {
    pub number: u16,
}

impl MyTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}
```

Fang re-exports `serde` so it's not required to add it to the `Cargo.toml` file


### Implement the AsyncRunnable trait

```rust
use fang::async_trait;
use fang::typetag;
use fang::AsyncRunnable;
use std::time::Duration;

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyTask {
    async fn run(&self, queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
        let new_task = MyTask::new(self.number + 1);
        queue
            .insert_task(&new_task as &dyn AsyncRunnable)
            .await
            .unwrap();

        log::info!("the current number is {}", self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(())
    }
}

```

- Fang uses the [typetag library](https://github.com/dtolnay/typetag) to serialize trait objects and save them to the queue.
- The [async-trait](https://github.com/dtolnay/async-trait) is used for implementing async traits

### Init queue

```rust
use fang::asynk::async_queue::AsyncQueue;

let max_pool_size: u32 = 2;
let mut queue = AsyncQueue::builder()
    .uri("postgres://postgres:postgres@localhost/fang")
    .max_pool_size(max_pool_size)
    .duplicated_tasks(true)
    .build();
```


### Start workers

```rust
use fang::asynk::async_worker_pool::AsyncWorkerPool;
use fang::NoTls;

let mut pool: AsyncWorkerPool<AsyncQueue<NoTls>> = AsyncWorkerPool::builder()
    .number_of_workers(10_u32)
    .queue(queue.clone())
    .build();

pool.start().await;
```

### Insert tasks

```rust
let task = MyTask::new(0);

queue
    .insert_task(&task1 as &dyn AsyncRunnable)
    .await
    .unwrap();
```

## Pitfalls

The async processing is suitable for lightweight tasks. But for heavier tasks it's advised to use one of the following approaches:

- start a separate tokio runtime to run fang workers
- use the threaded processing feature implemented in fang instead of the async processing

## Future directions

There are a couple of features planned for fang:

- Retries with different backoff modes
- Additional backends (in-memory, redis)
- Graceful shutdown for async workers (for the threaded processing this feature is implemented)
- Cron jobs

## Conclusion

The project is available on [GitHub](https://github.com/ayrat555/fang)

The async feature and this post is written in collaboration between [Ayrat Badykov](https://www.badykov.com/) ([github](https://github.com/ayrat555)) and [Pepe Márquez Romero](https://pxp9.github.io/) ([github](https://github.com/pxp9))

