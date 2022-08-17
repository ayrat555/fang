#![allow(clippy::extra_unused_lifetimes)]

use std::time::Duration;

#[derive(Clone, Debug)]
pub enum RetentionMode {
    KeepAll,
    RemoveAll,
    RemoveFinished,
}
impl Default for RetentionMode {
    fn default() -> Self {
        RetentionMode::RemoveAll
    }
}
#[derive(Clone, Debug)]
pub struct SleepParams {
    pub sleep_period: Duration,
    pub max_sleep_period: Duration,
    pub min_sleep_period: Duration,
    pub sleep_step: Duration,
}

impl SleepParams {
    pub fn maybe_reset_sleep_period(&mut self) {
        if self.sleep_period != self.min_sleep_period {
            self.sleep_period = self.min_sleep_period;
        }
    }

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

#[macro_use]
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
pub use serde_derive::{Deserialize, Serialize};

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
