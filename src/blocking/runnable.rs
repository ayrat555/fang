use crate::queue::Queueable;
use crate::FangError;
use crate::Scheduled;

pub const COMMON_TYPE: &str = "common";
pub const RETRIES_NUMBER: i32 = 20;

#[typetag::serde(tag = "type")]
pub trait Runnable {
    fn run(&self, _queueable: &dyn Queueable) -> Result<(), FangError>;

    fn task_type(&self) -> String {
        COMMON_TYPE.to_string()
    }

    fn uniq(&self) -> bool {
        false
    }

    fn cron(&self) -> Option<Scheduled> {
        None
    }

    fn max_retries(&self) -> i32 {
        RETRIES_NUMBER
    }

    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
