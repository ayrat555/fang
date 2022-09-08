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
lead = 'What's new with the release of fang 0.9'
images = []
+++

## Major changes

- Simplify schema.
- Improve the way that tasks are scheduled.
- Added Cron tasks support to both modules (asynk and blocking)
- Diesel crate 2.0 update (only blocking module).

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

You will only need to define a new function in the trait that implements the task.
`Runnable` for blocking and `AsyncRunnable` for async.

```rust
// you must use fang::Scheduled enum.
fn cron(&self) -> Option<Scheduled> {
        // cron expression to execute a task every 20 seconds.
        let expression = "0/20 * * * * * *";
        Some(Scheduled::CronPattern(expression.to_string()))
}
```

Also it is possible to schedule the task only once.

```rust
// you must use fang::Scheduled enum.
fn cron(&self) -> Option<Scheduled> {
        // You must use DateTime<Utc> to specify 
        // when in the future you would like schedule the task.

        // This will schedule the task for within 7 seconds.
        Some(Scheduled::ScheduleOnce(Utc::now() + Duration::seconds(7i64)))
}
```

It is no longer needed to start the schedule process, 
because the scheduled tasks are going to be executed a `WorkerPool` or `AsyncWorkerPool` start.

## Future directions

There are a couple of features planned for fang:

- Retries with different backoff modes
- Additional backends (in-memory, redis)
- Graceful shutdown for both modules.

## Conclusion

The project is available on [GitHub](https://github.com/ayrat555/fang)

The new release and this post is written in collaboration between [Ayrat Badykov](https://www.badykov.com/) ([github](https://github.com/ayrat555)) and [Pepe Márquez Romero](https://pxp9.github.io/) ([github](https://github.com/pxp9))

