+++
title = "Fang 0.9 release."
description = "What's new with the release of fang 0.9"
date = 2022-09-08T16:45:22+00:00
updated = 2022-09-08T16:45:22+00:00
template = "blog/page.html"
sort_by = "weight"
weight = 1
draft = false

[taxonomies]
authors = ["Ayrat Badykov", "Pepe Márquez"]

[extra]
lead = "What's new with the release of fang 0.9"
images = []
+++

## Major changes

- Simplify schema.
- Improve the way that tasks are scheduled.
- Added Cron tasks support to both modules (asynk and blocking)
- Diesel crate 2.0 update (only blocking module).
- Major refactoring of the blocking module.

### Simplify schema

We have completely eliminated the table of periodic tasks from the schema.

So schema only has one table.

```sql
CREATE TABLE fang_tasks (
     id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
     metadata jsonb NOT NULL,
     error_message TEXT,
     state fang_task_state DEFAULT 'new' NOT NULL,
     task_type VARCHAR DEFAULT 'common' NOT NULL,
     uniq_hash CHAR(64),
     scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
     created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

### Schedule tasks

This is the way to schedule tasks with new Fang 0.9

You will only need to define a new function called `cron` in the trait that implements the task.
`Runnable` for blocking and `AsyncRunnable` for asynk.

Take a look this example for `AsyncRunnable`.

```rust
impl AsyncRunnable for MyCronTask {
  async fn run(&self, _queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
    log::info!("CRON!!!!!!!!!!!!!!!",);

    Ok(())
  }

  // you must use fang::Scheduled enum.
  fn cron(&self) -> Option<Scheduled> {
    // cron expression to execute a task every 20 seconds.
    let expression = "0/20 * * * * * *";
    Some(Scheduled::CronPattern(expression.to_string()))
  }

  fn uniq(&self) -> bool {
    true
  }
}
```

Also it is possible to schedule the task only once.

```rust
impl AsyncRunnable for MyCronTask {
  async fn run(&self, _queue: &mut dyn AsyncQueueable) -> Result<(), Error> {
    log::info!("CRON!!!!!!!!!!!!!!!",);

    Ok(())
  }

  // you must use fang::Scheduled enum.
  fn cron(&self) -> Option<Scheduled> {
    // You must use DateTime<Utc> to specify 
    // when in the future you would like schedule the task.
  
    // This will schedule the task for within 7 seconds.
    Some(Scheduled::ScheduleOnce(Utc::now() + Duration::seconds(7i64)))
  }

  fn uniq(&self) -> bool {
    true
  }
}
```

You can check more [fang examples](https://github.com/ayrat555/fang/tree/master/fang_examples)

It is no longer needed to start the schedule process, 
because the scheduled tasks are going to be executed a `WorkerPool` or `AsyncWorkerPool` start.

### Blocking refactor

We have deleted graceful shutdown for blocking module but will be reimplemented at the same time that asynk graceful shutdown.

We have completely changed most of the blocking module API.

The reason for this change is because we want to unify the APIs of blocking and asynk so, 
if you would like to change the fang behaviour(change asynk module by blocking module or vice versa) 
in your software will be easier to change between modules.

Another reason is we want to do a trait to define a queue in blocking module. 
So this way we open the possibility to implement new backends in blocking module.

This will be the new API for blocking queue.

```rust
pub trait Queueable {
    fn fetch_and_touch_task(&self, task_type: String) -> Result<Option<Task>, QueueError>;

    fn insert_task(&self, params: &dyn Runnable) -> Result<Task, QueueError>;

    fn remove_all_tasks(&self) -> Result<usize, QueueError>;

    fn remove_all_scheduled_tasks(&self) -> Result<usize, QueueError>;

    fn remove_tasks_of_type(&self, task_type: &str) -> Result<usize, QueueError>;

    fn remove_task(&self, id: Uuid) -> Result<usize, QueueError>;

    fn find_task_by_id(&self, id: Uuid) -> Option<Task>;

    fn update_task_state(&self, task: &Task, state: FangTaskState) -> Result<Task, QueueError>;

    fn fail_task(&self, task: &Task, error: String) -> Result<Task, QueueError>;

    fn schedule_task(&self, task: &dyn Runnable) -> Result<Task, QueueError>;
}
```


Another change to highlight is that we updated to Diesel 2.0 so that affect directly to blocking module.

Before Fang 0.9 release we also have done many unit tests to prove that Fang works with Diesel 2.0.

Pre 0.9 release has been tested in real proyects called [el_monitorro](https://github.com/ayrat555/el_monitorro/) and [weather_bot_rust](https://github.com/pxp9/weather_bot_rust/).

## Future directions

There are a couple of features planned for fang:

- Retries with different backoff modes
- Additional backends (in-memory, redis)
- Graceful shutdown for both modules.

## Conclusion

The project is available on [GitHub](https://github.com/ayrat555/fang)

The new release and this post is written in collaboration between [Ayrat Badykov](https://www.badykov.com/) ([github](https://github.com/ayrat555)) and [Pepe Márquez Romero](https://pxp9.github.io/) ([github](https://github.com/pxp9))

