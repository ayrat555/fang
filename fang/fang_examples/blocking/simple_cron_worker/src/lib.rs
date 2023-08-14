use fang::runnable::Runnable;
use fang::serde::{Deserialize, Serialize};
use fang::typetag;
use fang::FangError;
use fang::Queueable;
use fang::Scheduled;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyCronTask {}

#[typetag::serde]
impl Runnable for MyCronTask {
    fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
        log::info!("CRON !!!!!!!!!!!!!!!!!");

        Ok(())
    }

    fn task_type(&self) -> String {
        "cron_test".to_string()
    }

    fn cron(&self) -> Option<Scheduled> {
        //               sec  min   hour   day of month   month   day of week   year
        //               be careful works only with UTC hour.
        //               https://www.timeanddate.com/worldclock/timezone/utc
        let expression = "0/20 * * * Aug-Sep * 2022/1";
        Some(Scheduled::CronPattern(expression.to_string()))
    }

    fn uniq(&self) -> bool {
        true
    }
}
