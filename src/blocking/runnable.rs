use crate::queue::Queueable;
use crate::Error;
use chrono::DateTime;
use chrono::Utc;

pub const COMMON_TYPE: &str = "common";

pub enum Scheduled {
    CronPattern(String),
    ScheduleOnce(DateTime<Utc>),
}

#[typetag::serde(tag = "type")]
pub trait Runnable {
    fn run(&self, _queueable: &dyn Queueable) -> Result<(), Error>;

    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }

    fn uniq(&self) -> bool {
        false
    }

    fn cron(&self) -> Option<Scheduled> {
        None
    }
}
