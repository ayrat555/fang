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

#[macro_use]
extern crate diesel;

#[doc(hidden)]
pub use diesel::pg::PgConnection;
#[doc(hidden)]
pub use typetag;

pub mod sync;
pub use sync::*;
