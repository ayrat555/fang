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
use sha2::Digest;
use sha2::Sha256;
use std::str::FromStr;
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(test)]
use dotenv::dotenv;
#[cfg(test)]
use std::env;

pub type PoolConnection = PooledConnection<ConnectionManager<PgConnection>>;

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[diesel(table_name = fang_tasks)]
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
#[diesel(table_name = fang_tasks)]
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

    fn remove_all_scheduled_tasks(&self) -> Result<usize, QueueError>;

    fn remove_tasks_of_type(&self, task_type: &str) -> Result<usize, QueueError>;

    fn remove_task(&self, id: Uuid) -> Result<usize, QueueError>;

    fn remove_task_by_uniq_hash(&mut self, task: &dyn Runnable) -> Result<usize, QueueError>;

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
        let mut connection = self.get_connection()?;

        Self::fetch_and_touch_query(&mut connection, task_type)
    }

    fn insert_task(&self, params: &dyn Runnable) -> Result<Task, QueueError> {
        let mut connection = self.get_connection()?;

        Self::insert_query(&mut connection, params, Utc::now())
    }
    fn schedule_task(&self, params: &dyn Runnable) -> Result<Task, QueueError> {
        let mut connection = self.get_connection()?;

        Self::schedule_task_query(&mut connection, params)
    }

    fn remove_all_scheduled_tasks(&self) -> Result<usize, QueueError> {
        let mut connection = self.get_connection()?;

        Self::remove_all_scheduled_tasks_query(&mut connection)
    }

    fn remove_all_tasks(&self) -> Result<usize, QueueError> {
        let mut connection = self.get_connection()?;

        Self::remove_all_tasks_query(&mut connection)
    }

    fn remove_tasks_of_type(&self, task_type: &str) -> Result<usize, QueueError> {
        let mut connection = self.get_connection()?;

        Self::remove_tasks_of_type_query(&mut connection, task_type)
    }

    fn remove_task(&self, id: Uuid) -> Result<usize, QueueError> {
        let mut connection = self.get_connection()?;

        Self::remove_task_query(&mut connection, id)
    }

    fn update_task_state(&self, task: &Task, state: FangTaskState) -> Result<Task, QueueError> {
        let mut connection = self.get_connection()?;

        Self::update_task_state_query(&mut connection, task, state)
    }

    fn fail_task(&self, task: &Task, error: String) -> Result<Task, QueueError> {
        let mut connection = self.get_connection()?;

        Self::fail_task_query(&mut connection, task, error)
    }

    fn find_task_by_id(&self, id: Uuid) -> Option<Task> {
        let mut connection = self.get_connection().unwrap();

        Self::find_task_by_id_query(&mut connection, id)
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
        connection: &mut PgConnection,
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

    fn calculate_hash(json: String) -> String {
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    pub fn insert_query(
        connection: &mut PgConnection,
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

            let uniq_hash = Self::calculate_hash(metadata.to_string());

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

    pub fn fetch_task_query(connection: &mut PgConnection, task_type: String) -> Option<Task> {
        Self::fetch_task_of_type_query(connection, &task_type)
    }

    pub fn fetch_and_touch_query(
        connection: &mut PgConnection,
        task_type: String,
    ) -> Result<Option<Task>, QueueError> {
        connection.transaction::<Option<Task>, QueueError, _>(|conn| {
            let found_task = Self::fetch_task_query(conn, task_type);

            if found_task.is_none() {
                return Ok(None);
            }

            match Self::update_task_state_query(
                conn,
                &found_task.unwrap(),
                FangTaskState::InProgress,
            ) {
                Ok(updated_task) => Ok(Some(updated_task)),
                Err(err) => Err(err),
            }
        })
    }

    pub fn find_task_by_id_query(connection: &mut PgConnection, id: Uuid) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::id.eq(id))
            .first::<Task>(connection)
            .ok()
    }

    pub fn remove_all_tasks_query(connection: &mut PgConnection) -> Result<usize, QueueError> {
        Ok(diesel::delete(fang_tasks::table).execute(connection)?)
    }

    pub fn remove_all_scheduled_tasks_query(
        connection: &mut PgConnection,
    ) -> Result<usize, QueueError> {
        let query = fang_tasks::table.filter(fang_tasks::scheduled_at.gt(Utc::now()));

        Ok(diesel::delete(query).execute(connection)?)
    }

    pub fn remove_tasks_of_type_query(
        connection: &mut PgConnection,
        task_type: &str,
    ) -> Result<usize, QueueError> {
        let query = fang_tasks::table.filter(fang_tasks::task_type.eq(task_type));

        Ok(diesel::delete(query).execute(connection)?)
    }

    pub fn remove_task_query(connection: &mut PgConnection, id: Uuid) -> Result<usize, QueueError> {
        let query = fang_tasks::table.filter(fang_tasks::id.eq(id));

        Ok(diesel::delete(query).execute(connection)?)
    }

    pub fn update_task_state_query(
        connection: &mut PgConnection,
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
        connection: &mut PgConnection,
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

    fn fetch_task_of_type_query(connection: &mut PgConnection, task_type: &str) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .order(fang_tasks::scheduled_at.asc())
            .limit(1)
            .filter(fang_tasks::scheduled_at.le(Utc::now()))
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .filter(fang_tasks::task_type.eq(task_type))
            .for_update()
            .skip_locked()
            .get_result::<Task>(connection)
            .ok()
    }

    fn find_task_by_uniq_hash_query(
        connection: &mut PgConnection,
        uniq_hash: &str,
    ) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::uniq_hash.eq(uniq_hash))
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .first::<Task>(connection)
            .ok()
    }
}

#[cfg(test)]
mod queue_tests {
    use super::Queue;
    use super::Queueable;
    use crate::chrono::SubsecRound;
    use crate::runnable::Runnable;
    use crate::runnable::COMMON_TYPE;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use crate::FangError;
    use crate::Scheduled;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use diesel::connection::Connection;
    use diesel::result::Error;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct PepeTask {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for PepeTask {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            println!("the number is {}", self.number);

            Ok(())
        }
        fn uniq(&self) -> bool {
            true
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AyratTask {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for AyratTask {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            println!("the number is {}", self.number);

            Ok(())
        }
        fn uniq(&self) -> bool {
            true
        }

        fn task_type(&self) -> String {
            "weirdo".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct ScheduledPepeTask {
        pub number: u16,
        pub datetime: String,
    }

    #[typetag::serde]
    impl Runnable for ScheduledPepeTask {
        fn run(&self, _queue: &dyn Queueable) -> Result<(), FangError> {
            println!("the number is {}", self.number);

            Ok(())
        }
        fn uniq(&self) -> bool {
            true
        }

        fn task_type(&self) -> String {
            "scheduled".to_string()
        }

        fn cron(&self) -> Option<Scheduled> {
            let datetime = self.datetime.parse::<DateTime<Utc>>().ok()?;
            Some(Scheduled::ScheduleOnce(datetime))
        }
    }

    #[test]
    fn insert_task_test() {
        let task = PepeTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task = Queue::insert_query(conn, &task, Utc::now()).unwrap();

            let metadata = task.metadata.as_object().unwrap();
            let number = metadata["number"].as_u64();
            let type_task = metadata["type"].as_str();

            assert_eq!(task.error_message, None);
            assert_eq!(FangTaskState::New, task.state);
            assert_eq!(Some(10), number);
            assert_eq!(Some("PepeTask"), type_task);

            Ok(())
        });
    }

    #[test]
    fn fetch_task_fetches_the_oldest_task() {
        let task1 = PepeTask { number: 10 };
        let task2 = PepeTask { number: 11 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task1 = Queue::insert_query(conn, &task1, Utc::now()).unwrap();
            let _task2 = Queue::insert_query(conn, &task2, Utc::now()).unwrap();

            let found_task = Queue::fetch_task_query(conn, COMMON_TYPE.to_string()).unwrap();
            assert_eq!(found_task.id, task1.id);
            Ok(())
        });
    }

    #[test]
    fn update_task_state_test() {
        let task = PepeTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task = Queue::insert_query(conn, &task, Utc::now()).unwrap();

            let found_task =
                Queue::update_task_state_query(conn, &task, FangTaskState::Finished).unwrap();

            let metadata = found_task.metadata.as_object().unwrap();
            let number = metadata["number"].as_u64();
            let type_task = metadata["type"].as_str();

            assert_eq!(found_task.id, task.id);
            assert_eq!(found_task.state, FangTaskState::Finished);
            assert_eq!(Some(10), number);
            assert_eq!(Some("PepeTask"), type_task);

            Ok(())
        });
    }

    #[test]
    fn fail_task_updates_state_field_and_sets_error_message() {
        let task = PepeTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task = Queue::insert_query(conn, &task, Utc::now()).unwrap();

            let error = "Failed".to_string();

            let found_task = Queue::fail_task_query(conn, &task, error.clone()).unwrap();

            let metadata = found_task.metadata.as_object().unwrap();
            let number = metadata["number"].as_u64();
            let type_task = metadata["type"].as_str();

            assert_eq!(found_task.id, task.id);
            assert_eq!(found_task.state, FangTaskState::Failed);
            assert_eq!(Some(10), number);
            assert_eq!(Some("PepeTask"), type_task);
            assert_eq!(found_task.error_message.unwrap(), error);

            Ok(())
        });
    }

    #[test]
    fn fetch_and_touch_updates_state() {
        let task = PepeTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task = Queue::insert_query(conn, &task, Utc::now()).unwrap();

            let found_task = Queue::fetch_and_touch_query(conn, COMMON_TYPE.to_string())
                .unwrap()
                .unwrap();

            let metadata = found_task.metadata.as_object().unwrap();
            let number = metadata["number"].as_u64();
            let type_task = metadata["type"].as_str();

            assert_eq!(found_task.id, task.id);
            assert_eq!(found_task.state, FangTaskState::InProgress);
            assert_eq!(Some(10), number);
            assert_eq!(Some("PepeTask"), type_task);

            Ok(())
        });
    }

    #[test]
    fn fetch_and_touch_returns_none() {
        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let found_task = Queue::fetch_and_touch_query(conn, COMMON_TYPE.to_string()).unwrap();

            assert_eq!(None, found_task);

            Ok(())
        });
    }

    #[test]
    fn insert_task_uniq_test() {
        let task = PepeTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task1 = Queue::insert_query(conn, &task, Utc::now()).unwrap();
            let task2 = Queue::insert_query(conn, &task, Utc::now()).unwrap();

            assert_eq!(task2.id, task1.id);
            Ok(())
        });
    }

    #[test]
    fn schedule_task_test() {
        let pool = Queue::connection_pool(5);
        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

            let task = &ScheduledPepeTask {
                number: 10,
                datetime: datetime.to_string(),
            };

            let task = Queue::schedule_task_query(conn, task).unwrap();

            let metadata = task.metadata.as_object().unwrap();
            let number = metadata["number"].as_u64();
            let type_task = metadata["type"].as_str();

            assert_eq!(Some(10), number);
            assert_eq!(Some("ScheduledPepeTask"), type_task);
            assert_eq!(task.scheduled_at, datetime);

            Ok(())
        });
    }

    #[test]
    fn remove_all_scheduled_tasks_test() {
        let pool = Queue::connection_pool(5);
        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

            let task1 = &ScheduledPepeTask {
                number: 10,
                datetime: datetime.to_string(),
            };

            let task2 = &ScheduledPepeTask {
                number: 11,
                datetime: datetime.to_string(),
            };

            Queue::schedule_task_query(conn, task1).unwrap();
            Queue::schedule_task_query(conn, task2).unwrap();

            let number = Queue::remove_all_scheduled_tasks_query(conn).unwrap();

            assert_eq!(2, number);

            Ok(())
        });
    }

    #[test]
    fn remove_all_tasks_test() {
        let task1 = PepeTask { number: 10 };
        let task2 = PepeTask { number: 11 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task1 = Queue::insert_query(conn, &task1, Utc::now()).unwrap();
            let task2 = Queue::insert_query(conn, &task2, Utc::now()).unwrap();

            let result = Queue::remove_all_tasks_query(conn).unwrap();

            assert_eq!(2, result);
            assert_eq!(None, Queue::find_task_by_id_query(conn, task1.id));
            assert_eq!(None, Queue::find_task_by_id_query(conn, task2.id));

            Ok(())
        });
    }

    #[test]
    fn remove_task() {
        let task1 = PepeTask { number: 10 };
        let task2 = PepeTask { number: 11 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task1 = Queue::insert_query(conn, &task1, Utc::now()).unwrap();
            let task2 = Queue::insert_query(conn, &task2, Utc::now()).unwrap();

            assert!(Queue::find_task_by_id_query(conn, task1.id).is_some());
            assert!(Queue::find_task_by_id_query(conn, task2.id).is_some());

            Queue::remove_task_query(conn, task1.id).unwrap();

            assert!(Queue::find_task_by_id_query(conn, task1.id).is_none());
            assert!(Queue::find_task_by_id_query(conn, task2.id).is_some());

            Ok(())
        });
    }

    #[test]
    fn remove_task_of_type() {
        let task1 = PepeTask { number: 10 };
        let task2 = AyratTask { number: 10 };

        let pool = Queue::connection_pool(5);

        let queue = Queue::builder().connection_pool(pool).build();

        let mut queue_pooled_connection = queue.connection_pool.get().unwrap();

        queue_pooled_connection.test_transaction::<(), Error, _>(|conn| {
            let task1 = Queue::insert_query(conn, &task1, Utc::now()).unwrap();
            let task2 = Queue::insert_query(conn, &task2, Utc::now()).unwrap();

            assert!(Queue::find_task_by_id_query(conn, task1.id).is_some());
            assert!(Queue::find_task_by_id_query(conn, task2.id).is_some());

            Queue::remove_tasks_of_type_query(conn, "weirdo").unwrap();

            assert!(Queue::find_task_by_id_query(conn, task1.id).is_some());
            assert!(Queue::find_task_by_id_query(conn, task2.id).is_none());

            Ok(())
        });
    }
}
