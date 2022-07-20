use crate::asynk::async_queue::AsyncQueue;
use crate::asynk::async_queue::FangTaskState;
use crate::asynk::async_queue::Task;
use crate::asynk::async_runnable::AsyncRunnable;
use crate::asynk::Error;
use bb8_postgres::tokio_postgres::tls::MakeTlsConnect;
use bb8_postgres::tokio_postgres::tls::TlsConnect;
use bb8_postgres::tokio_postgres::Socket;
use log::error;
use std::time::Duration;
use tokio::time::sleep;
use typed_builder::TypedBuilder;
#[derive(Clone, Debug)]
pub enum RetentionMode {
    KeepAll,
    RemoveAll,
    RemoveFinished,
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
#[derive(TypedBuilder, Debug)]
pub struct AsyncWorker<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[builder(setter(into))]
    pub queue: AsyncQueue<Tls>,
    #[builder(setter(into))]
    pub task_type: Option<String>,
    #[builder(setter(into))]
    pub sleep_params: SleepParams,
    #[builder(setter(into))]
    pub retention_mode: RetentionMode,
}
impl<Tls> AsyncWorker<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn run(&mut self, task: Task) {
        let result = self.execute_task(task).await;
        self.finalize_task(result).await
    }
    async fn execute_task(&mut self, task: Task) -> Result<Task, (Task, String)> {
        let actual_task: Box<dyn AsyncRunnable> =
            serde_json::from_value(task.metadata.clone()).unwrap();
        let task_result = actual_task.run(&self.queue.pool).await;
        match task_result {
            Ok(()) => Ok(task),
            Err(error) => Err((task, error.description)),
        }
    }
    async fn finalize_task(&mut self, result: Result<Task, (Task, String)>) {
        match self.retention_mode {
            RetentionMode::KeepAll => {
                match result {
                    Ok(task) => self
                        .queue
                        .update_task_state(task, FangTaskState::Finished)
                        .await
                        .unwrap(),
                    Err((task, error)) => self.queue.fail_task(task, &error).await.unwrap(),
                };
            }
            RetentionMode::RemoveAll => {
                match result {
                    Ok(task) => self.queue.remove_task(task).await.unwrap(),
                    Err((task, _error)) => self.queue.remove_task(task).await.unwrap(),
                };
            }
            RetentionMode::RemoveFinished => match result {
                Ok(task) => {
                    self.queue.remove_task(task).await.unwrap();
                }
                Err((task, error)) => {
                    self.queue.fail_task(task, &error).await.unwrap();
                }
            },
        }
    }
    pub fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        sleep(Duration::from_secs(self.sleep_params.sleep_period));
    }
    pub async fn run_tasks(&mut self) -> Result<(), Error> {
        //loop {}
        // Not sure how to do the loop here
        match self
            .queue
            .fetch_and_touch_task(&self.task_type.clone())
            .await
        {
            Ok(Some(task)) => {
                self.sleep_params.maybe_reset_sleep_period();
                self.run(task).await;
            }
            Ok(None) => {
                self.sleep();
            }

            Err(error) => {
                error!("Failed to fetch a task {:?}", error);

                self.sleep();
            }
        };
        Ok(())
    }
}
