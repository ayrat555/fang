#![allow(clippy::extra_unused_lifetimes)]
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
    pub sleep_period: u64,
    pub max_sleep_period: u64,
    pub min_sleep_period: u64,
    pub sleep_step: u64,
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
            sleep_period: 5,
            max_sleep_period: 15,
            min_sleep_period: 5,
            sleep_step: 5,
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

#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "blocking")]
pub use blocking::*;

#[cfg(feature = "asynk")]
pub mod asynk;

#[cfg(feature = "asynk")]
pub use asynk::*;
