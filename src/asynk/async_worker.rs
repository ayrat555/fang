use crate::asynk::async_queue::AsyncQueue;
use crate::asynk::async_queue::AsyncQueueable;
use crate::asynk::async_queue::FangTaskState;
use crate::asynk::async_queue::Task;
use crate::asynk::async_runnable::AsyncRunnable;
use crate::asynk::Error;
use crate::{RetentionMode, SleepParams};
use bb8_postgres::tokio_postgres::tls::MakeTlsConnect;
use bb8_postgres::tokio_postgres::tls::TlsConnect;
use bb8_postgres::tokio_postgres::Socket;
use log::error;
use std::time::Duration;
use typed_builder::TypedBuilder;

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

        let task_result = actual_task.run(&mut self.queue).await;
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
    pub async fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        tokio::time::sleep(Duration::from_secs(self.sleep_params.sleep_period)).await;
    }
    pub async fn run_tasks(&mut self) -> Result<(), Error> {
        loop {
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
                    self.sleep().await;
                }

                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);

                    self.sleep().await;
                }
            };
        }
    }
}
