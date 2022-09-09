+++
title = "Fang 0.9"
description = "What's new with the release of fang 0.9"
date = 2022-09-09T16:45:22+00:00
updated = 2022-09-09T16:45:22+00:00
template = "blog/page.html"
sort_by = "weight"
weight = 1
draft = false

[taxonomies]
authors = ["Pepe Márquez", "Ayrat Badykov"]

[extra]
lead = "What's new with the release of fang 0.9"
images = []
+++

## Major changes

- Simplify the database schema
- Improve the way tasks are scheduled
- Add CRON tasks support to both modules (asynk and blocking)
- Update the diesel crate to 2.0 (used only by blocking module)
- Major refactoring of the blocking module

### Simplify the DB schema

We got rid of the periodic tasks table. Now periodic, scheduled and one-time tasks are stored in the same table (`fang_tasks`).

We added two new fields to the `fang_tasks` table

- `scheduled_at` - based on this table tasks are scheduled. Workers fetch tasks with `scheduled_at` <= `current_time`
- `uniq_hash` - hash calculated from the JSON metadata of the task. Based on this field tasks are deduplicated.

So changed schema is looking like this:

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

Let's examine how periodic tasks can be created with fang 0.9.

The only method that should be defined is the `cron` method in the `Runnable`(blocking)/`AsyncRunnable`(asynk) trait implementation.

Let's take a look at an example:

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

Also, it is possible to schedule a task only once.

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

More examples are available at [fang examples](https://github.com/ayrat555/fang/tree/master/fang_examples)

It is no longer needed to start the scheduler process, the scheduled tasks will be executed by `WorkerPool` or `AsyncWorkerPool`. If a task is periodic, it will be re-scheduled before its next execution.

### Blocking refactor

- We deleted the graceful shutdown feature of the blocking module. But we're planning to re-implement it in the future.

- We completely changed most of the blocking module's API.

The reason for this change is to unify the APIs of the blocking and the asynk modules. So users can easily switch between blocking and async workers.

Another reason is we wanted to do a trait for the task queue in the blocking module. It opens a  possibility to implement new backends for the blocking module.

A new API of the blocking queues looks like this:

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

- Another change we want to highlight is that we updated Diesel to 2.0 (used only in the blocking module to interact with the DB)

Pre 0.9 release was tested in real projects:

- [el_monitorro](https://github.com/ayrat555/el_monitorro/)
- [weather_bot_rust](https://github.com/pxp9/weather_bot_rust/).

## Future directions

- Retries with different backoff modes
- Additional backends (in-memory, redis)
- Graceful shutdown for both modules

## Conclusion

The project is available on [GitHub](https://github.com/ayrat555/fang)

The new release and this post is written in collaboration between  [Pepe Márquez Romero](https://pxp9.github.io/) ([github](https://github.com/pxp9)) and [Ayrat Badykov](https://www.badykov.com/) ([github](https://github.com/ayrat555)).
