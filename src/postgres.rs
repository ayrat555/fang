use crate::executor::Runnable;
use crate::schema::fang_periodic_tasks;
use crate::schema::fang_tasks;
use crate::schema::FangTaskState;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error;
use dotenv::dotenv;
use std::env;
use uuid::Uuid;

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[table_name = "fang_tasks"]
pub struct Task {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub error_message: Option<String>,
    pub state: FangTaskState,
    pub task_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[table_name = "fang_periodic_tasks"]
pub struct PeriodicTask {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub period_in_seconds: i32,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "fang_tasks"]
pub struct NewTask {
    pub metadata: serde_json::Value,
    pub task_type: String,
}

#[derive(Insertable)]
#[table_name = "fang_periodic_tasks"]
pub struct NewPeriodicTask {
    pub metadata: serde_json::Value,
    pub period_in_seconds: i32,
}

pub struct Postgres {
    pub connection: PgConnection,
}

impl Default for Postgres {
    fn default() -> Self {
        Self::new()
    }
}

impl Postgres {
    pub fn new() -> Self {
        let connection = Self::pg_connection(None);

        Self { connection }
    }

    pub fn new_with_url(database_url: String) -> Self {
        let connection = Self::pg_connection(Some(database_url));

        Self { connection }
    }

    pub fn push_task(&self, job: &dyn Runnable) -> Result<Task, Error> {
        let json_job = serde_json::to_value(job).unwrap();

        let new_task = NewTask {
            metadata: json_job,
            task_type: job.task_type(),
        };

        self.insert(&new_task)
    }

    pub fn push_periodic_task(
        &self,
        job: &dyn Runnable,
        period: i32,
    ) -> Result<PeriodicTask, Error> {
        let json_job = serde_json::to_value(job).unwrap();

        let new_task = NewPeriodicTask {
            metadata: json_job,
            period_in_seconds: period,
        };

        diesel::insert_into(fang_periodic_tasks::table)
            .values(new_task)
            .get_result::<PeriodicTask>(&self.connection)
    }

    pub fn enqueue_task(job: &dyn Runnable) -> Result<Task, Error> {
        Self::new().push_task(job)
    }

    pub fn insert(&self, params: &NewTask) -> Result<Task, Error> {
        diesel::insert_into(fang_tasks::table)
            .values(params)
            .get_result::<Task>(&self.connection)
    }

    pub fn fetch_task(&self, task_type: &Option<String>) -> Option<Task> {
        match task_type {
            None => self.fetch_any_task(),
            Some(task_type_str) => self.fetch_task_of_type(task_type_str),
        }
    }

    pub fn fetch_and_touch(&self, task_type: &Option<String>) -> Result<Option<Task>, Error> {
        self.connection.transaction::<Option<Task>, Error, _>(|| {
            let found_task = self.fetch_task(task_type);

            if found_task.is_none() {
                return Ok(None);
            }

            match self.start_processing_task(&found_task.unwrap()) {
                Ok(updated_task) => Ok(Some(updated_task)),
                Err(err) => Err(err),
            }
        })
    }

    pub fn find_task_by_id(&self, id: Uuid) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::id.eq(id))
            .first::<Task>(&self.connection)
            .ok()
    }

    pub fn find_periodic_task_by_id(&self, id: Uuid) -> Option<PeriodicTask> {
        fang_periodic_tasks::table
            .filter(fang_periodic_tasks::id.eq(id))
            .first::<PeriodicTask>(&self.connection)
            .ok()
    }

    pub fn fetch_periodic_tasks(&self, error_margin_seconds: i64) -> Option<Vec<PeriodicTask>> {
        let current_time = Self::current_time();

        let low_limit = current_time - Duration::seconds(error_margin_seconds);
        let high_limit = current_time + Duration::seconds(error_margin_seconds);

        fang_periodic_tasks::table
            .filter(
                fang_periodic_tasks::scheduled_at
                    .gt(low_limit)
                    .and(fang_periodic_tasks::scheduled_at.lt(high_limit)),
            )
            .or_filter(fang_periodic_tasks::scheduled_at.is_null())
            .load::<PeriodicTask>(&self.connection)
            .ok()
    }

    pub fn schedule_next_task_execution(&self, task: &PeriodicTask) -> Result<PeriodicTask, Error> {
        let current_time = Self::current_time();
        let scheduled_at = current_time + Duration::seconds(task.period_in_seconds.into());

        diesel::update(task)
            .set((
                fang_periodic_tasks::scheduled_at.eq(scheduled_at),
                fang_periodic_tasks::updated_at.eq(current_time),
            ))
            .get_result::<PeriodicTask>(&self.connection)
    }

    pub fn remove_all_tasks(&self) -> Result<usize, Error> {
        diesel::delete(fang_tasks::table).execute(&self.connection)
    }

    pub fn remove_all_periodic_tasks(&self) -> Result<usize, Error> {
        diesel::delete(fang_periodic_tasks::table).execute(&self.connection)
    }

    pub fn remove_task(&self, id: Uuid) -> Result<usize, Error> {
        let query = fang_tasks::table.filter(fang_tasks::id.eq(id));

        diesel::delete(query).execute(&self.connection)
    }

    pub fn finish_task(&self, task: &Task) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Finished),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(&self.connection)
    }

    pub fn start_processing_task(&self, task: &Task) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::InProgress),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(&self.connection)
    }

    pub fn fail_task(&self, task: &Task, error: String) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Failed),
                fang_tasks::error_message.eq(error),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(&self.connection)
    }

    fn current_time() -> DateTime<Utc> {
        Utc::now()
    }

    fn pg_connection(database_url: Option<String>) -> PgConnection {
        dotenv().ok();

        let url = match database_url {
            Some(string_url) => string_url,
            None => env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        };

        PgConnection::establish(&url).unwrap_or_else(|_| panic!("Error connecting to {}", url))
    }

    fn fetch_any_task(&self) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .limit(1)
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .for_update()
            .skip_locked()
            .get_result::<Task>(&self.connection)
            .ok()
    }

    fn fetch_task_of_type(&self, task_type: &str) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .limit(1)
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .filter(fang_tasks::task_type.eq(task_type))
            .for_update()
            .skip_locked()
            .get_result::<Task>(&self.connection)
            .ok()
    }
}

#[cfg(test)]
mod postgres_tests {
    use super::NewTask;
    use super::PeriodicTask;
    use super::Postgres;
    use super::Task;
    use crate::executor::Error as ExecutorError;
    use crate::executor::Runnable;
    use crate::schema::fang_periodic_tasks;
    use crate::schema::fang_tasks;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use crate::{Deserialize, Serialize};
    use chrono::prelude::*;
    use chrono::{DateTime, Duration, Utc};
    use diesel::connection::Connection;
    use diesel::prelude::*;
    use diesel::result::Error;

    #[test]
    fn insert_inserts_task() {
        let postgres = Postgres::new();

        let new_task = NewTask {
            metadata: serde_json::json!(true),
            task_type: "common".to_string(),
        };

        let result = postgres
            .connection
            .test_transaction::<Task, Error, _>(|| postgres.insert(&new_task));

        assert_eq!(result.state, FangTaskState::New);
        assert_eq!(result.error_message, None);
    }

    #[test]
    fn fetch_task_fetches_the_oldest_task() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let timestamp1 = Utc::now() - Duration::hours(40);

            let task1 = insert_job(serde_json::json!(true), timestamp1, &postgres.connection);

            let timestamp2 = Utc::now() - Duration::hours(20);

            insert_job(serde_json::json!(false), timestamp2, &postgres.connection);

            let found_task = postgres.fetch_task(&None).unwrap();

            assert_eq!(found_task.id, task1.id);

            Ok(())
        });
    }

    #[test]
    fn finish_task_updates_state_field() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_new_job(&postgres.connection);

            let updated_task = postgres.finish_task(&task).unwrap();

            assert_eq!(FangTaskState::Finished, updated_task.state);

            Ok(())
        });
    }

    #[test]
    fn fail_task_updates_state_field_and_sets_error_message() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_new_job(&postgres.connection);
            let error = "Failed".to_string();

            let updated_task = postgres.fail_task(&task, error.clone()).unwrap();

            assert_eq!(FangTaskState::Failed, updated_task.state);
            assert_eq!(error, updated_task.error_message.unwrap());

            Ok(())
        });
    }

    #[test]
    fn fetch_and_touch_updates_state() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let _task = insert_new_job(&postgres.connection);

            let updated_task = postgres.fetch_and_touch(&None).unwrap().unwrap();

            assert_eq!(FangTaskState::InProgress, updated_task.state);

            Ok(())
        });
    }

    #[test]
    fn fetch_and_touch_returns_none() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = postgres.fetch_and_touch(&None).unwrap();

            assert_eq!(None, task);

            Ok(())
        });
    }

    #[test]
    fn push_task_serializes_and_inserts_task() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task = postgres.push_task(&job).unwrap();

            let mut m = serde_json::value::Map::new();
            m.insert(
                "number".to_string(),
                serde_json::value::Value::Number(10.into()),
            );
            m.insert(
                "type".to_string(),
                serde_json::value::Value::String("Job".to_string()),
            );

            assert_eq!(task.metadata, serde_json::value::Value::Object(m));

            Ok(())
        });
    }

    #[test]
    fn push_periodic_task() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task = postgres.push_periodic_task(&job, 60).unwrap();

            assert_eq!(task.period_in_seconds, 60);
            assert!(postgres.find_periodic_task_by_id(task.id).is_some());

            Ok(())
        });
    }

    #[test]
    fn fetch_periodic_tasks_fetches_periodic_task_without_scheduled_at() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task = postgres.push_periodic_task(&job, 60).unwrap();

            let schedule_in_future = Utc::now() + Duration::hours(100);

            insert_periodic_job(
                serde_json::json!(true),
                schedule_in_future,
                100,
                &postgres.connection,
            );

            let tasks = postgres.fetch_periodic_tasks(100).unwrap();

            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].id, task.id);

            Ok(())
        });
    }

    #[test]
    fn schedule_next_task_execution() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_periodic_job(
                serde_json::json!(true),
                Utc::now(),
                100,
                &postgres.connection,
            );

            let updated_task = postgres.schedule_next_task_execution(&task).unwrap();

            let next_schedule = (task.scheduled_at.unwrap()
                + Duration::seconds(task.period_in_seconds.into()))
            .round_subsecs(0);

            assert_eq!(
                next_schedule,
                updated_task.scheduled_at.unwrap().round_subsecs(0)
            );

            Ok(())
        });
    }

    #[test]
    fn remove_all_periodic_tasks() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_periodic_job(
                serde_json::json!(true),
                Utc::now(),
                100,
                &postgres.connection,
            );

            let result = postgres.remove_all_periodic_tasks().unwrap();

            assert_eq!(1, result);

            assert_eq!(None, postgres.find_periodic_task_by_id(task.id));

            Ok(())
        });
    }

    #[test]
    fn remove_all_tasks() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_job(serde_json::json!(true), Utc::now(), &postgres.connection);
            let result = postgres.remove_all_tasks().unwrap();

            assert_eq!(1, result);

            assert_eq!(None, postgres.find_task_by_id(task.id));

            Ok(())
        });
    }

    #[test]
    fn fetch_periodic_tasks() {
        let postgres = Postgres::new();

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let schedule_in_future = Utc::now() + Duration::hours(100);

            insert_periodic_job(
                serde_json::json!(true),
                schedule_in_future,
                100,
                &postgres.connection,
            );

            let task = insert_periodic_job(
                serde_json::json!(true),
                Utc::now(),
                100,
                &postgres.connection,
            );

            let tasks = postgres.fetch_periodic_tasks(100).unwrap();

            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].id, task.id);

            Ok(())
        });
    }

    #[test]
    fn remove_task() {
        let postgres = Postgres::new();

        let new_task1 = NewTask {
            metadata: serde_json::json!(true),
            task_type: "common".to_string(),
        };

        let new_task2 = NewTask {
            metadata: serde_json::json!(true),
            task_type: "common".to_string(),
        };

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task1 = postgres.insert(&new_task1).unwrap();
            assert!(postgres.find_task_by_id(task1.id).is_some());

            let task2 = postgres.insert(&new_task2).unwrap();
            assert!(postgres.find_task_by_id(task2.id).is_some());

            postgres.remove_task(task1.id).unwrap();
            assert!(postgres.find_task_by_id(task1.id).is_none());
            assert!(postgres.find_task_by_id(task2.id).is_some());

            postgres.remove_task(task2.id).unwrap();
            assert!(postgres.find_task_by_id(task2.id).is_none());

            Ok(())
        });
    }

    // this test is ignored because it commits data to the db
    #[test]
    #[ignore]
    fn fetch_task_locks_the_record() {
        let postgres = Postgres::new();
        let timestamp1 = Utc::now() - Duration::hours(40);

        let task1 = insert_job(serde_json::json!(true), timestamp1, &postgres.connection);

        let task1_id = task1.id;

        let timestamp2 = Utc::now() - Duration::hours(20);

        let task2 = insert_job(serde_json::json!(false), timestamp2, &postgres.connection);

        let thread = std::thread::spawn(move || {
            let postgres = Postgres::new();

            postgres.connection.transaction::<(), Error, _>(|| {
                let found_task = postgres.fetch_task(&None).unwrap();

                assert_eq!(found_task.id, task1.id);

                std::thread::sleep(std::time::Duration::from_millis(5000));

                Ok(())
            })
        });

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task = postgres.fetch_task(&None).unwrap();

        assert_eq!(found_task.id, task2.id);

        let _result = thread.join();

        // returns unlocked record

        let found_task = postgres.fetch_task(&None).unwrap();

        assert_eq!(found_task.id, task1_id);
    }

    #[derive(Serialize, Deserialize)]
    struct Job {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for Job {
        fn run(&self) -> Result<(), ExecutorError> {
            println!("the number is {}", self.number);

            Ok(())
        }
    }

    fn insert_job(
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

    fn insert_periodic_job(
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        period_in_seconds: i32,
        connection: &PgConnection,
    ) -> PeriodicTask {
        diesel::insert_into(fang_periodic_tasks::table)
            .values(&vec![(
                fang_periodic_tasks::metadata.eq(metadata),
                fang_periodic_tasks::scheduled_at.eq(timestamp),
                fang_periodic_tasks::period_in_seconds.eq(period_in_seconds),
            )])
            .get_result::<PeriodicTask>(connection)
            .unwrap()
    }

    fn insert_new_job(connection: &PgConnection) -> Task {
        diesel::insert_into(fang_tasks::table)
            .values(&vec![(fang_tasks::metadata.eq(serde_json::json!(true)),)])
            .get_result::<Task>(connection)
            .unwrap()
    }
}
