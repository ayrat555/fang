// #![allow(clippy::nonstandard_macro_braces)]
#![allow(clippy::extra_unused_lifetimes)]

// pub mod error;
// pub mod executor;
// pub mod queue;
// pub mod scheduler;
// pub mod schema;
// pub mod worker_pool;

// pub use error::FangError;
// pub use executor::*;
// pub use queue::*;
// pub use scheduler::*;
// pub use schema::*;
// pub use worker_pool::*;
use chrono::DateTime;
use chrono::Utc;
use uuid::Uuid;

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[table_name = "fang_tasks"]
pub struct Task {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub error_message: Option<String>,
    pub state: FangTaskState,
    pub task_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[table_name = "fang_periodic_tasks"]
pub struct PeriodicTask {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub period_in_seconds: i32,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "fang_tasks"]
pub struct NewTask {
    pub metadata: serde_json::Value,
    pub task_type: String,
}

#[derive(Insertable)]
#[table_name = "fang_periodic_tasks"]
pub struct NewPeriodicTask {
    pub metadata: serde_json::Value,
    pub period_in_seconds: i32,
}

#[macro_use]
extern crate diesel;

#[doc(hidden)]
pub use diesel::pg::PgConnection;
#[doc(hidden)]
pub use typetag;

pub mod sync;
pub use sync::*;

pub mod asynk;
