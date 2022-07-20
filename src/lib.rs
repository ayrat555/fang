#![allow(clippy::extra_unused_lifetimes)]

#[macro_use]
#[cfg(feature = "blocking")]
extern crate diesel;

#[doc(hidden)]
#[cfg(feature = "blocking")]
pub use diesel::pg::PgConnection;

#[doc(hidden)]
pub use typetag;

#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "blocking")]
pub use blocking::*;

#[cfg(feature = "asynk")]
pub mod asynk;
