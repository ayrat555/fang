use fang::async_trait;
use fang::asynk::async_queue::AsyncQueueable;
use fang::serde::{Deserialize, Serialize};
use fang::typetag;
use fang::AsyncRunnable;
use fang::FangError;
use fang::Scheduled;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
pub struct MyCronTask {}

#[async_trait]
#[typetag::serde]
impl AsyncRunnable for MyCronTask {
    async fn run(&self, _queue: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        log::info!("CRON!!!!!!!!!!!!!!!",);

        Ok(())
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
