#![doc = include_str!("../README.md")]

#[cfg(feature = "blocking")]
use diesel::{Identifiable, Queryable};
#[cfg(feature = "asynk-sqlx")]
use sqlx::any::AnyRow;
#[cfg(feature = "asynk-sqlx")]
use sqlx::FromRow;
#[cfg(feature = "asynk-sqlx")]
use sqlx::Row;
use std::time::Duration;
use thiserror::Error;
use typed_builder::TypedBuilder;
///
/// Represents a schedule for scheduled tasks.
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
#[cfg_attr(
    feature = "blocking",
    ExistingTypePath = "crate::postgres_schema::sql_types::FangTaskState"
)]
pub enum FangTaskState {
    /// The task is ready to be executed
    New,
    /// The task is being executed.
    ///
    /// The task may stay in this state forever
    /// if an unexpected error happened
    InProgress,
    /// The task failed
    Failed,
    /// The task finished successfully
    Finished,
    /// The task is being retried. It means it failed but it's scheduled to be executed again
    Retried,
}

impl<S: AsRef<str>> From<S> for FangTaskState {
    fn from(str: S) -> Self {
        let str = str.as_ref();
        match str {
            "new" => FangTaskState::New,
            "in_progress" => FangTaskState::InProgress,
            "failed" => FangTaskState::Failed,
            "finished" => FangTaskState::Finished,
            "retried" => FangTaskState::Retried,
            _ => unreachable!(),
        }
    }
}

impl From<FangTaskState> for &str {
    fn from(state: FangTaskState) -> Self {
        match state {
            FangTaskState::New => "new",
            FangTaskState::InProgress => "in_progress",
            FangTaskState::Failed => "failed",
            FangTaskState::Finished => "finished",
            FangTaskState::Retried => "retried",
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[cfg_attr(feature = "blocking", derive(Queryable, Identifiable))]
#[diesel(table_name = fang_tasks)]
pub struct Task {
    #[builder(setter(into))]
    pub id: Vec<u8>,
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

#[cfg(feature = "asynk-sqlx")]
impl<'a> FromRow<'a, AnyRow> for Task {
    fn from_row(row: &'a AnyRow) -> Result<Self, sqlx::Error> {
        let id: Vec<u8> = row.get("id");

        let raw: &str = row.get("metadata"); // will work if database cast json to string
        let raw = raw.replace('\\', "");

        // -- SELECT metadata->>'type' FROM fang_tasks ; this works because jsonb casting
        let metadata: serde_json::Value = serde_json::from_str(&raw).unwrap();

        // This should be changed when issue https://github.com/launchbadge/sqlx/issues/2416 is fixed
        // Fixed in pxp9's fork
        let error_message: Option<String> = row.get("error_message");

        let state_str: &str = row.get("state"); // will work if database cast json to string

        let state: FangTaskState = state_str.into();

        let task_type: String = row.get("task_type");

        // This should be changed when issue https://github.com/launchbadge/sqlx/issues/2416 is fixed
        // Fixed in pxp9's fork
        let uniq_hash: Option<String> = row.get("uniq_hash");

        let retries: i32 = row.get("retries");

        let scheduled_at_str: &str = row.get("scheduled_at");

        println!("{}", scheduled_at_str);

        let scheduled_at: DateTime<Utc> = DateTime::parse_from_str(scheduled_at_str, "%F %T%.f%#z")
            .unwrap()
            .into();

        let created_at_str: &str = row.get("created_at");

        let created_at: DateTime<Utc> = DateTime::parse_from_str(created_at_str, "%F %T%.f%#z")
            .unwrap()
            .into();

        let updated_at_str: &str = row.get("updated_at");

        let updated_at: DateTime<Utc> = DateTime::parse_from_str(updated_at_str, "%F %T%.f%#z")
            .unwrap()
            .into();

        Ok(Task::builder()
            .id(id)
            .metadata(metadata)
            .error_message(error_message)
            .state(state)
            .task_type(task_type)
            .uniq_hash(uniq_hash)
            .retries(retries)
            .scheduled_at(scheduled_at)
            .created_at(created_at)
            .updated_at(updated_at)
            .build())
    }
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
pub use async_trait::async_trait;

#[cfg(feature = "derive-error")]
pub use fang_derive_error::ToFangError;

#[cfg(feature = "migrations")]
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

#[cfg(feature = "migrations")]
use std::error::Error as SomeError;

#[cfg(feature = "migrations-postgres")]
use diesel::pg::Pg;

#[cfg(feature = "migrations-postgres")]
pub const MIGRATIONS_POSTGRES: EmbeddedMigrations =
    embed_migrations!("postgres_migrations/migrations");

#[cfg(feature = "migrations-postgres")]
pub fn run_migrations_postgres(
    connection: &mut impl MigrationHarness<Pg>,
) -> Result<(), Box<dyn SomeError + Send + Sync + 'static>> {
    connection.run_pending_migrations(MIGRATIONS_POSTGRES)?;

    Ok(())
}

#[cfg(feature = "migrations-mysql")]
use diesel::mysql::Mysql;

#[cfg(feature = "migrations-mysql")]
pub const MIGRATIONS_MYSQL: EmbeddedMigrations = embed_migrations!("mysql_migrations/migrations");

#[cfg(feature = "migrations-mysql")]
pub fn run_migrations_mysql(
    connection: &mut impl MigrationHarness<Mysql>,
) -> Result<(), Box<dyn SomeError + Send + Sync + 'static>> {
    connection.run_pending_migrations(MIGRATIONS_MYSQL)?;

    Ok(())
}

#[cfg(feature = "migrations-sqlite")]
use diesel::sqlite::Sqlite;

#[cfg(feature = "migrations-sqlite")]
pub const MIGRATIONS_SQLITE: EmbeddedMigrations = embed_migrations!("sqlite_migrations/migrations");

#[cfg(feature = "migrations-sqlite")]
pub fn run_migrations_sqlite(
    connection: &mut impl MigrationHarness<Sqlite>,
) -> Result<(), Box<dyn SomeError + Send + Sync + 'static>> {
    connection.run_pending_migrations(MIGRATIONS_SQLITE)?;

    Ok(())
}
