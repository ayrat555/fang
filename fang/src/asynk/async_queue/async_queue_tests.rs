use super::AsyncQueueable;
use crate::asynk::AsyncRunnable;
use crate::FangError;
use crate::Scheduled;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct AsyncTask {
    pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncTask {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AsyncUniqTask {
    pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncUniqTask {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        Ok(())
    }

    fn uniq(&self) -> bool {
        true
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AsyncTaskSchedule {
    pub number: u16,
    pub datetime: String,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncTaskSchedule {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FangError> {
        Ok(())
    }

    fn cron(&self) -> Option<Scheduled> {
        let datetime = self.datetime.parse::<DateTime<Utc>>().ok()?;
        Some(Scheduled::ScheduleOnce(datetime))
    }
}

/// This macro creates a module with tests for a `Queueable` type.
///
/// Arguments:
/// + `$mod`: Name for the module
/// + `$q`: Full path to type that implements `AsyncQueueable`
/// + `$e`: An expression that returns a value of `$q` suitable for testing.
///   + Multiple values returned by `$e` must be able to be interacted with concurrently without interfering with each other.
macro_rules! test_asynk_queue {
    ($mod:ident, $q:ty, $e:expr) => {
        mod $mod {
            use chrono::Duration;
            use chrono::SubsecRound;
            use chrono::Utc;
            use $crate::async_queue::async_queue_tests::{
                AsyncTask, AsyncTaskSchedule, AsyncUniqTask,
            };
            use $crate::asynk::async_queue::AsyncQueueable;
            use $crate::FangTaskState;

            #[tokio::test]
            async fn insert_task_creates_new_task() {
                let mut test: $q = $e.await;

                let task = test.insert_task(&AsyncTask { number: 1 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);
            }

            #[tokio::test]
            async fn update_task_state_test() {
                let mut test: $q = $e.await;

                let task = test.insert_task(&AsyncTask { number: 1 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();
                let id: &[u8] = &task.id;

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let finished_task = test
                    .update_task_state(&task, FangTaskState::Finished)
                    .await
                    .unwrap();

                assert_eq!(id, finished_task.id);
                assert_eq!(FangTaskState::Finished, finished_task.state);
            }

            #[tokio::test]
            async fn failed_task_query_test() {
                let mut test: $q = $e.await;

                let task = test.insert_task(&AsyncTask { number: 1 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();
                let id: &[u8] = &task.id;

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let failed_task = test.fail_task(&task, "Some error").await.unwrap();

                assert_eq!(id, failed_task.id);
                assert_eq!(Some("Some error"), failed_task.error_message.as_deref());
                assert_eq!(FangTaskState::Failed, failed_task.state);
            }

            #[tokio::test]
            async fn remove_all_tasks_test() {
                let mut test: $q = $e.await;

                let task = test.insert_task(&AsyncTask { number: 1 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let task = test.insert_task(&AsyncTask { number: 2 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(2), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let result = test.remove_all_tasks().await.unwrap();
                assert_eq!(2, result);
            }

            #[tokio::test]
            async fn schedule_task_test() {
                let mut test: $q = $e.await;

                let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

                let task = &AsyncTaskSchedule {
                    number: 1,
                    datetime: datetime.to_string(),
                };

                let task = test.schedule_task(task).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTaskSchedule"), type_task);
                assert_eq!(task.scheduled_at, datetime);
            }

            #[tokio::test]
            async fn remove_all_scheduled_tasks_test() {
                let mut test: $q = $e.await;

                let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

                let task1 = &AsyncTaskSchedule {
                    number: 1,
                    datetime: datetime.to_string(),
                };

                let task2 = &AsyncTaskSchedule {
                    number: 2,
                    datetime: datetime.to_string(),
                };

                test.schedule_task(task1).await.unwrap();
                test.schedule_task(task2).await.unwrap();

                let number = test.remove_all_scheduled_tasks().await.unwrap();

                assert_eq!(2, number);
            }

            #[tokio::test]
            async fn fetch_and_touch_test() {
                let mut test: $q = $e.await;

                let task = test.insert_task(&AsyncTask { number: 1 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let task = test.insert_task(&AsyncTask { number: 2 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(2), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let task = test.fetch_and_touch_task(None).await.unwrap().unwrap(); // This fails if this FOR UPDATE SKIP LOCKED is set in query fetch task type

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let task = test.fetch_and_touch_task(None).await.unwrap().unwrap();
                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(2), number);
                assert_eq!(Some("AsyncTask"), type_task);
            }

            #[tokio::test]
            async fn remove_tasks_type_test() {
                let mut test: $q = $e.await;

                let task = test.insert_task(&AsyncTask { number: 1 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let task = test.insert_task(&AsyncTask { number: 2 }).await.unwrap();

                let metadata = task.metadata.as_object().unwrap();

                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(2), number);
                assert_eq!(Some("AsyncTask"), type_task);

                let result = test
                    .remove_tasks_type("mytype")
                    .await
                    .expect("el numero salio bad");

                assert_eq!(0, result);

                let result = test.remove_tasks_type("common").await.unwrap();
                assert_eq!(2, result);
            }

            #[tokio::test]
            async fn remove_tasks_by_metadata() {
                let mut test: $q = $e.await;

                let task = test
                    .insert_task(&AsyncUniqTask { number: 1 })
                    .await
                    .unwrap();

                let metadata = task.metadata.as_object().expect("here 1");
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(1), number);
                assert_eq!(Some("AsyncUniqTask"), type_task);

                let task = test
                    .insert_task(&AsyncUniqTask { number: 2 })
                    .await
                    .unwrap();

                let metadata = task.metadata.as_object().expect("here 2");
                let number = metadata["number"].as_u64();
                let type_task = metadata["type"].as_str();

                assert_eq!(Some(2), number);
                assert_eq!(Some("AsyncUniqTask"), type_task);

                let result = test
                    .remove_task_by_metadata(&AsyncUniqTask { number: 0 })
                    .await
                    .unwrap();
                assert_eq!(0, result);

                let result = test
                    .remove_task_by_metadata(&AsyncUniqTask { number: 1 })
                    .await
                    .unwrap();
                assert_eq!(1, result);
            }
        }
    };
}

pub(crate) use test_asynk_queue;
