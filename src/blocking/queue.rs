use crate::runnable::Runnable;
use crate::schema::fang_tasks;
use crate::schema::FangTaskState;
use crate::CronError;
use crate::Scheduled::*;
use chrono::DateTime;
use chrono::Utc;
use cron::Schedule;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::PoolError;
use diesel::r2d2::PooledConnection;
use diesel::result::Error as DieselError;
use dotenv::dotenv;
use sha2::Digest;
use sha2::Sha256;
use std::env;
use std::str::FromStr;
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

type PoolConnection = PooledConnection<ConnectionManager<PgConnection>>;

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[table_name = "fang_tasks"]
pub struct Task {
    #[builder(setter(into))]
    pub id: Uuid,
    #[builder(setter(into))]
    pub metadata: serde_json::Value,
    #[builder(setter(into))]
    pub error_message: Option<String>,
    #[builder(setter(into))]
    pub state: FangTaskState,
    #[builder(setter(into))]
    pub task_type: String,
    #[builder(setter(into))]
    pub uniq_hash: Option<String>,
    #[builder(setter(into))]
    pub scheduled_at: DateTime<Utc>,
    #[builder(setter(into))]
    pub created_at: DateTime<Utc>,
    #[builder(setter(into))]
    pub updated_at: DateTime<Utc>,
}

#[derive(Insertable, Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[table_name = "fang_tasks"]
pub struct NewTask {
    #[builder(setter(into))]
    metadata: serde_json::Value,
    #[builder(setter(into))]
    task_type: String,
    #[builder(setter(into))]
    uniq_hash: Option<String>,
    #[builder(setter(into))]
    scheduled_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error(transparent)]
    DieselError(#[from] DieselError),
    #[error(transparent)]
    PoolError(#[from] PoolError),
    #[error(transparent)]
    CronError(#[from] CronError),
}

impl From<cron::error::Error> for QueueError {
    fn from(error: cron::error::Error) -> Self {
        QueueError::CronError(CronError::LibraryError(error))
    }
}

pub trait Queueable {
    fn fetch_and_touch_task(&self, task_type: String) -> Result<Option<Task>, QueueError>;

    fn insert_task(&self, params: &dyn Runnable) -> Result<Task, QueueError>;

    fn remove_all_tasks(&self) -> Result<usize, QueueError>;

    fn remove_tasks_of_type(&self, task_type: &str) -> Result<usize, QueueError>;

    fn remove_task(&self, id: Uuid) -> Result<usize, QueueError>;

    fn find_task_by_id(&self, id: Uuid) -> Option<Task>;

    fn update_task_state(&self, task: &Task, state: FangTaskState) -> Result<Task, QueueError>;

    fn fail_task(&self, task: &Task, error: String) -> Result<Task, QueueError>;

    fn schedule_task(&self, task: &dyn Runnable) -> Result<Task, QueueError>;
}

#[derive(Clone, TypedBuilder)]
pub struct Queue {
    #[builder(setter(into))]
    pub connection_pool: r2d2::Pool<r2d2::ConnectionManager<PgConnection>>,
}

impl Queueable for Queue {
    fn fetch_and_touch_task(&self, task_type: String) -> Result<Option<Task>, QueueError> {
        let connection = self.get_connection()?;

        Self::fetch_and_touch_query(&connection, task_type)
    }

    fn insert_task(&self, params: &dyn Runnable) -> Result<Task, QueueError> {
        let connection = self.get_connection()?;

        Self::insert_query(&connection, params, Utc::now())
    }
    fn schedule_task(&self, params: &dyn Runnable) -> Result<Task, QueueError> {
        let connection = self.get_connection()?;

        Self::schedule_task_query(&connection, params)
    }
    fn remove_all_tasks(&self) -> Result<usize, QueueError> {
        let connection = self.get_connection()?;

        Self::remove_all_tasks_query(&connection)
    }

    fn remove_tasks_of_type(&self, task_type: &str) -> Result<usize, QueueError> {
        let connection = self.get_connection()?;

        Self::remove_tasks_of_type_query(&connection, task_type)
    }

    fn remove_task(&self, id: Uuid) -> Result<usize, QueueError> {
        let connection = self.get_connection()?;

        Self::remove_task_query(&connection, id)
    }

    fn update_task_state(&self, task: &Task, state: FangTaskState) -> Result<Task, QueueError> {
        let connection = self.get_connection()?;

        Self::update_task_state_query(&connection, task, state)
    }

    fn fail_task(&self, task: &Task, error: String) -> Result<Task, QueueError> {
        let connection = self.get_connection()?;

        Self::fail_task_query(&connection, task, error)
    }

    fn find_task_by_id(&self, id: Uuid) -> Option<Task> {
        let connection = self.get_connection().unwrap();

        Self::find_task_by_id_query(&connection, id)
    }
}

impl Queue {
    pub fn get_connection(&self) -> Result<PoolConnection, QueueError> {
        let result = self.connection_pool.get();

        if let Err(err) = result {
            log::error!("Failed to get a db connection {:?}", err);
            return Err(QueueError::PoolError(err));
        }

        Ok(result.unwrap())
    }

    pub fn schedule_task_query(
        connection: &PgConnection,
        params: &dyn Runnable,
    ) -> Result<Task, QueueError> {
        let scheduled_at = match params.cron() {
            Some(scheduled) => match scheduled {
                CronPattern(cron_pattern) => {
                    let schedule = Schedule::from_str(&cron_pattern)?;
                    let mut iterator = schedule.upcoming(Utc);

                    iterator
                        .next()
                        .ok_or(QueueError::CronError(CronError::NoTimestampsError))?
                }
                ScheduleOnce(datetime) => datetime,
            },
            None => {
                return Err(QueueError::CronError(CronError::TaskNotSchedulableError));
            }
        };

        Self::insert_query(connection, params, scheduled_at)
    }

    pub fn insert_query(
        connection: &PgConnection,
        params: &dyn Runnable,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, QueueError> {
        if !params.uniq() {
            let new_task = NewTask::builder()
                .scheduled_at(scheduled_at)
                .uniq_hash(None)
                .task_type(params.task_type())
                .metadata(serde_json::to_value(params).unwrap())
                .build();

            Ok(diesel::insert_into(fang_tasks::table)
                .values(new_task)
                .get_result::<Task>(connection)?)
        } else {
            let metadata = serde_json::to_value(params).unwrap();

            let mut hasher = Sha256::new();
            hasher.update(metadata.to_string().as_bytes());
            let result = hasher.finalize();
            let uniq_hash = hex::encode(result);

            match Self::find_task_by_uniq_hash_query(connection, &uniq_hash) {
                Some(task) => Ok(task),
                None => {
                    let new_task = NewTask::builder()
                        .scheduled_at(scheduled_at)
                        .uniq_hash(Some(uniq_hash))
                        .task_type(params.task_type())
                        .metadata(serde_json::to_value(params).unwrap())
                        .build();

                    Ok(diesel::insert_into(fang_tasks::table)
                        .values(new_task)
                        .get_result::<Task>(connection)?)
                }
            }
        }
    }

    pub fn fetch_task_query(connection: &PgConnection, task_type: String) -> Option<Task> {
        Self::fetch_task_of_type_query(connection, &task_type)
    }

    pub fn fetch_and_touch_query(
        connection: &PgConnection,
        task_type: String,
    ) -> Result<Option<Task>, QueueError> {
        connection.transaction::<Option<Task>, QueueError, _>(|| {
            let found_task = Self::fetch_task_query(connection, task_type);

            if found_task.is_none() {
                return Ok(None);
            }

            match Self::update_task_state_query(
                connection,
                &found_task.unwrap(),
                FangTaskState::InProgress,
            ) {
                Ok(updated_task) => Ok(Some(updated_task)),
                Err(err) => Err(err),
            }
        })
    }

    pub fn find_task_by_id_query(connection: &PgConnection, id: Uuid) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::id.eq(id))
            .first::<Task>(connection)
            .ok()
    }

    pub fn remove_all_tasks_query(connection: &PgConnection) -> Result<usize, QueueError> {
        Ok(diesel::delete(fang_tasks::table).execute(connection)?)
    }

    pub fn remove_tasks_of_type_query(
        connection: &PgConnection,
        task_type: &str,
    ) -> Result<usize, QueueError> {
        let query = fang_tasks::table.filter(fang_tasks::task_type.eq(task_type));

        Ok(diesel::delete(query).execute(connection)?)
    }

    pub fn remove_task_query(connection: &PgConnection, id: Uuid) -> Result<usize, QueueError> {
        let query = fang_tasks::table.filter(fang_tasks::id.eq(id));

        Ok(diesel::delete(query).execute(connection)?)
    }

    pub fn update_task_state_query(
        connection: &PgConnection,
        task: &Task,
        state: FangTaskState,
    ) -> Result<Task, QueueError> {
        Ok(diesel::update(task)
            .set((
                fang_tasks::state.eq(state),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(connection)?)
    }

    pub fn fail_task_query(
        connection: &PgConnection,
        task: &Task,
        error: String,
    ) -> Result<Task, QueueError> {
        Ok(diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Failed),
                fang_tasks::error_message.eq(error),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(connection)?)
    }

    fn current_time() -> DateTime<Utc> {
        Utc::now()
    }

    #[cfg(test)]
    pub fn connection_pool(pool_size: u32) -> r2d2::Pool<r2d2::ConnectionManager<PgConnection>> {
        dotenv().ok();

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        let manager = r2d2::ConnectionManager::<PgConnection>::new(database_url);

        r2d2::Pool::builder()
            .max_size(pool_size)
            .build(manager)
            .unwrap()
    }

    fn fetch_task_of_type_query(connection: &PgConnection, task_type: &str) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .limit(1)
            .filter(fang_tasks::scheduled_at.le(Utc::now()))
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .filter(fang_tasks::task_type.eq(task_type))
            .for_update()
            .skip_locked()
            .get_result::<Task>(connection)
            .ok()
    }
    fn find_task_by_uniq_hash_query(connection: &PgConnection, uniq_hash: &str) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::uniq_hash.eq(uniq_hash))
            .filter(
                fang_tasks::state
                    .eq(FangTaskState::New)
                    .or(fang_tasks::state.eq(FangTaskState::InProgress)),
            )
            .first::<Task>(connection)
            .ok()
    }
}

#[cfg(test)]
mod queue_tests {
    use super::Queue;
    use super::Queueable;
    use super::Task;
    use crate::runnable::Runnable;
    use crate::schema::fang_tasks;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use crate::worker::Error as ExecutorError;
    use crate::NewTask;
    use chrono::prelude::*;
    use chrono::{DateTime, Duration, Utc};
    use diesel::connection::Connection;
    use diesel::prelude::*;
    use diesel::result::Error;
    use serde::{Deserialize, Serialize};
    use std::time::Duration as StdDuration;
    /*
        #[test]
        fn insert_inserts_task() {
            let queue = Queue::new();

            let new_task = NewTask {
                metadata: serde_json::json!(true),
                task_type: "common".to_string(),
                scheduled_at: Utc::now()
            };

            let result = queue
                .connection
                .test_transaction::<Task, Error, _>(|| queue.insert(&new_task));

            assert_eq!(result.state, FangTaskState::New);
            assert_eq!(result.error_message, None);
        }
    */
    /*    #[test]
        fn fetch_task_fetches_the_oldest_task() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let timestamp1 = Utc::now() - Duration::hours(40);

                let task1 = insert_task(serde_json::json!(true), timestamp1, &queue.connection);

                let timestamp2 = Utc::now() - Duration::hours(20);

                insert_task(serde_json::json!(false), timestamp2, &queue.connection);

                let found_task = queue.fetch_task(&None).unwrap();

                assert_eq!(found_task.id, task1.id);

                Ok(())
            });
        }
    */
    /*  #[test]
        fn finish_task_updates_state_field() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task = insert_new_task(&queue.connection);

                let updated_task = queue.finish_task(&task).unwrap();

                assert_eq!(FangTaskState::Finished, updated_task.state);

                Ok(())
            });
        }
    */
    /*  #[test]
        fn fail_task_updates_state_field_and_sets_error_message() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task = insert_new_task(&queue.connection);
                let error = "Failed".to_string();

                let updated_task = queue.fail_task(&task, error.clone()).unwrap();

                assert_eq!(FangTaskState::Failed, updated_task.state);
                assert_eq!(error, updated_task.error_message.unwrap());

                Ok(())
            });
        }
    */
    /*  #[test]
        fn fetch_and_touch_updates_state() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let _task = insert_new_task(&queue.connection);

                let updated_task = queue.fetch_and_touch(&None).unwrap().unwrap();

                assert_eq!(FangTaskState::InProgress, updated_task.state);

                Ok(())
            });
        }
    */
    /*  #[test]
        fn fetch_and_touch_returns_none() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task = queue.fetch_and_touch(&None).unwrap();

                assert_eq!(None, task);

                Ok(())
            });
        }
    */
    /*  #[test]
        fn push_task_serializes_and_inserts_task() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task = PepeTask { number: 10 };
                let task = queue.push_task(&task).unwrap();

                let mut m = serde_json::value::Map::new();
                m.insert(
                    "number".to_string(),
                    serde_json::value::Value::Number(10.into()),
                );
                m.insert(
                    "type".to_string(),
                    serde_json::value::Value::String("PepeTask".to_string()),
                );

                assert_eq!(task.metadata, serde_json::value::Value::Object(m));

                Ok(())
            });
        }
    */
    /*  #[test]
        fn push_task_does_not_insert_the_same_task() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task = PepeTask { number: 10 };
                let task2 = queue.push_task(&task).unwrap();

                let task1 = queue.push_task(&task).unwrap();

                assert_eq!(task1.id, task2.id);

                Ok(())
            });
        }
    */
    /*  #[test]
        fn remove_all_tasks() {
            let queue = Queue::new();

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task = insert_task(serde_json::json!(true), Utc::now(), &queue.connection);
                let result = queue.remove_all_tasks().unwrap();

                assert_eq!(1, result);

                assert_eq!(None, queue.find_task_by_id(task.id));

                Ok(())
            });
        }
    */
    /*  #[test]
        fn remove_task() {
            let queue = Queue::new();

            let new_task1 = NewTask {
                metadata: serde_json::json!(true),
                task_type: "common".to_string(),
            };

            let new_task2 = NewTask {
                metadata: serde_json::json!(true),
                task_type: "common".to_string(),
            };

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task1 = queue.insert(&new_task1).unwrap();
                assert!(queue.find_task_by_id(task1.id).is_some());

                let task2 = queue.insert(&new_task2).unwrap();
                assert!(queue.find_task_by_id(task2.id).is_some());

                queue.remove_task(task1.id).unwrap();
                assert!(queue.find_task_by_id(task1.id).is_none());
                assert!(queue.find_task_by_id(task2.id).is_some());

                queue.remove_task(task2.id).unwrap();
                assert!(queue.find_task_by_id(task2.id).is_none());

                Ok(())
            });
        }
    */
    /*  #[test]
        fn remove_task_of_type() {
            let queue = Queue::new();

            let new_task1 = NewTask {
                metadata: serde_json::json!(true),
                task_type: "type1".to_string(),
            };

            let new_task2 = NewTask {
                metadata: serde_json::json!(true),
                task_type: "type2".to_string(),
            };

            queue.connection.test_transaction::<(), Error, _>(|| {
                let task1 = queue.insert(&new_task1).unwrap();
                assert!(queue.find_task_by_id(task1.id).is_some());

                let task2 = queue.insert(&new_task2).unwrap();
                assert!(queue.find_task_by_id(task2.id).is_some());

                queue.remove_tasks_of_type("type1").unwrap();
                assert!(queue.find_task_by_id(task1.id).is_none());
                assert!(queue.find_task_by_id(task2.id).is_some());

                Ok(())
            });
        }
    */
    /*  // this test is ignored because it commits data to the db
        #[test]
        #[ignore]
        fn fetch_task_locks_the_record() {
            let queue = Queue::new();
            let timestamp1 = Utc::now() - Duration::hours(40);

            let task1 = insert_task(
                serde_json::json!(PepeTask { number: 12 }),
                timestamp1,
                &queue.connection,
            );

            let task1_id = task1.id;

            let timestamp2 = Utc::now() - Duration::hours(20);

            let task2 = insert_task(
                serde_json::json!(PepeTask { number: 11 }),
                timestamp2,
                &queue.connection,
            );

            let thread = std::thread::spawn(move || {
                let queue = Queue::new();

                queue.connection.transaction::<(), Error, _>(|| {
                    let found_task = queue.fetch_task(&None).unwrap();

                    assert_eq!(found_task.id, task1.id);

                    std::thread::sleep(std::time::Duration::from_millis(5000));

                    Ok(())
                })
            });

            std::thread::sleep(std::time::Duration::from_millis(1000));

            let found_task = queue.fetch_task(&None).unwrap();

            assert_eq!(found_task.id, task2.id);

            let _result = thread.join();

            // returns unlocked record

            let found_task = queue.fetch_task(&None).unwrap();

            assert_eq!(found_task.id, task1_id);
        }
    */
    #[derive(Serialize, Deserialize)]
    struct PepeTask {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for PepeTask {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), ExecutorError> {
            println!("the number is {}", self.number);

            Ok(())
        }
    }

    fn insert_task(
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        connection: &PgConnection,
    ) -> Task {
        diesel::insert_into(fang_tasks::table)
            .values(&vec![(
                fang_tasks::metadata.eq(metadata),
                fang_tasks::created_at.eq(timestamp),
            )])
            .get_result::<Task>(connection)
            .unwrap()
    }

    fn insert_new_task(connection: &PgConnection) -> Task {
        diesel::insert_into(fang_tasks::table)
            .values(&vec![(fang_tasks::metadata.eq(serde_json::json!(true)),)])
            .get_result::<Task>(connection)
            .unwrap()
    }
}
