#![doc = include_str!("../README.md")]

#[cfg(feature = "blocking")]
use diesel::{Identifiable, Queryable};
use std::time::Duration;
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(feature = "asynk")]
use postgres_types::{FromSql, ToSql};
/// Represents a schedule for scheduled tasks.
///
/// It's used in the [`AsyncRunnable::cron`] and [`Runnable::cron`]
#[derive(Debug, Clone)]
pub enum Scheduled {
    /// A cron pattern for a periodic task
    ///
    /// For example, `Scheduled::CronPattern("0/20 * * * * * *")`
    CronPattern(String),
    /// A datetime for a scheduled task that will be executed once
    ///
    /// For example, `Scheduled::ScheduleOnce(chrono::Utc::now() + std::time::Duration::seconds(7i64))`
    ScheduleOnce(DateTime<Utc>),
}

/// List of error types that can occur while working with cron schedules.
#[derive(Debug, Error)]
pub enum CronError {
    /// A problem occured during cron schedule parsing.
    #[error(transparent)]
    LibraryError(#[from] cron::error::Error),
    /// [`Scheduled`] enum variant is not provided
    #[error("You have to implement method `cron()` in your AsyncRunnable")]
    TaskNotSchedulableError,
    /// The next execution can not be determined using the current [`Scheduled::CronPattern`]
    #[error("No timestamps match with this cron pattern")]
    NoTimestampsError,
}

/// All possible options for retaining tasks in the db after their execution.
///
/// The default mode is [`RetentionMode::RemoveAll`]
#[derive(Clone, Debug, Default)]
pub enum RetentionMode {
    /// Keep all tasks
    KeepAll,
    /// Remove all tasks
    #[default]
    RemoveAll,
    /// Remove only successfully finished tasks
    RemoveFinished,
}

/// Configuration parameters for putting workers to sleep
/// while they don't have any tasks to execute
#[derive(Clone, Debug, TypedBuilder)]
pub struct SleepParams {
    /// the current sleep period
    pub sleep_period: Duration,
    /// the maximum period a worker is allowed to sleep.
    /// After this value is reached, `sleep_period` is not increased anymore
    pub max_sleep_period: Duration,
    /// the initial value of the `sleep_period`
    pub min_sleep_period: Duration,
    /// the step that `sleep_period` is increased by on every iteration
    pub sleep_step: Duration,
}

impl SleepParams {
    /// Reset the `sleep_period` if `sleep_period` > `min_sleep_period`
    pub fn maybe_reset_sleep_period(&mut self) {
        if self.sleep_period != self.min_sleep_period {
            self.sleep_period = self.min_sleep_period;
        }
    }

    /// Increase the `sleep_period` by the `sleep_step` if the `max_sleep_period` is not reached
    pub fn maybe_increase_sleep_period(&mut self) {
        if self.sleep_period < self.max_sleep_period {
            self.sleep_period += self.sleep_step;
        }
    }
}

impl Default for SleepParams {
    fn default() -> Self {
        SleepParams {
            sleep_period: Duration::from_secs(5),
            max_sleep_period: Duration::from_secs(15),
            min_sleep_period: Duration::from_secs(5),
            sleep_step: Duration::from_secs(5),
        }
    }
}

/// An error that can happen during executing of tasks
#[derive(Debug)]
pub struct FangError {
    /// A description of an error
    pub description: String,
}

/// Possible states of the task
#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(feature = "blocking", derive(diesel_derive_enum::DbEnum))]
#[cfg_attr(feature = "asynk", derive(ToSql, FromSql, Default))]
#[cfg_attr(feature = "asynk", postgres(name = "fang_task_state"))]
#[cfg_attr(
    feature = "blocking",
    ExistingTypePath = "crate::postgres_schema::sql_types::FangTaskState"
)]
pub enum FangTaskState {
    /// The task is ready to be executed
    #[cfg_attr(feature = "asynk", postgres(name = "new"))]
    #[cfg_attr(feature = "asynk", default)]
    New,
    /// The task is being executed.
    ///
    /// The task may stay in this state forever
    /// if an unexpected error happened
    #[cfg_attr(feature = "asynk", postgres(name = "in_progress"))]
    InProgress,
    /// The task failed
    #[cfg_attr(feature = "asynk", postgres(name = "failed"))]
    Failed,
    /// The task finished successfully
    #[cfg_attr(feature = "asynk", postgres(name = "finished"))]
    Finished,
    /// The task is being retried. It means it failed but it's scheduled to be executed again
    #[cfg_attr(feature = "asynk", postgres(name = "retried"))]
    Retried,
}

#[derive(Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[cfg_attr(feature = "blocking", derive(Queryable, Identifiable))]
#[cfg_attr(feature = "blocking",  diesel(table_name = fang_tasks))]
pub struct Task {
    #[builder(setter(into))]
    pub id: Uuid,
    #[builder(setter(into))]
    pub metadata: serde_json::Value,
    #[builder(setter(into))]
    pub error_message: Option<String>,
    #[builder(setter(into))]
    pub state: FangTaskState,
    #[builder(setter(into))]
    pub task_type: String,
    #[builder(setter(into))]
    pub uniq_hash: Option<String>,
    #[builder(setter(into))]
    pub retries: i32,
    #[builder(setter(into))]
    pub scheduled_at: DateTime<Utc>,
    #[builder(setter(into))]
    pub created_at: DateTime<Utc>,
    #[builder(setter(into))]
    pub updated_at: DateTime<Utc>,
}

#[doc(hidden)]
#[cfg(feature = "blocking")]
extern crate diesel;

#[doc(hidden)]
#[cfg(feature = "blocking")]
pub use diesel::pg::PgConnection;

#[doc(hidden)]
pub use typetag;

#[doc(hidden)]
pub extern crate serde;

#[doc(hidden)]
pub extern crate chrono;

#[doc(hidden)]
pub use serde_derive::{Deserialize, Serialize};

#[doc(hidden)]
pub use chrono::DateTime;
#[doc(hidden)]
pub use chrono::Utc;

#[cfg(feature = "blocking")]
pub mod blocking;

#[cfg(feature = "blocking")]
pub use blocking::*;

#[cfg(feature = "asynk")]
pub mod asynk;

#[cfg(feature = "asynk")]
pub use asynk::*;

#[cfg(feature = "asynk")]
#[doc(hidden)]
pub use bb8_postgres::tokio_postgres::tls::NoTls;

#[cfg(feature = "asynk")]
#[doc(hidden)]
pub use async_trait::async_trait;

#[cfg(feature = "derive-error")]
pub use fang_derive_error::ToFangError;
