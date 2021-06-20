use crate::schema::fang_tasks;
use crate::schema::FangTaskState;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error;
use dotenv::dotenv;
use std::env;
use uuid::Uuid;

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq)]
#[table_name = "fang_tasks"]
pub struct Task {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub error_message: Option<String>,
    pub state: FangTaskState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "fang_tasks"]
pub struct NewTask {
    pub metadata: serde_json::Value,
}

pub struct Postgres {
    pub database_url: String,
    pub connection: PgConnection,
}

impl Postgres {
    pub fn new(database_url: Option<String>) -> Self {
        dotenv().ok();

        let url = match database_url {
            Some(string_url) => string_url,
            None => {
                let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

                url
            }
        };

        let connection =
            PgConnection::establish(&url).expect(&format!("Error connecting to {}", url));

        Self {
            connection,
            database_url: url,
        }
    }

    pub fn insert(&self, params: &NewTask) -> Result<Task, Error> {
        diesel::insert_into(fang_tasks::table)
            .values(params)
            .get_result::<Task>(&self.connection)
    }

    pub fn fetch_task(&self) -> Option<Task> {
        match fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .limit(1)
            .for_update()
            .skip_locked()
            .get_result::<Task>(&self.connection)
        {
            Ok(record) => Some(record),
            _ => None,
        }
    }

    pub fn find_task_by_id(&self, id: Uuid) -> Option<Task> {
        match fang_tasks::table
            .filter(fang_tasks::id.eq(id))
            .first::<Task>(&self.connection)
        {
            Ok(record) => Some(record),
            _ => None,
        }
    }

    pub fn finish_task(&self, task: &Task) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Finished),
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

    pub fn current_time() -> DateTime<Utc> {
        Utc::now()
    }
}

#[cfg(test)]
mod postgres_tests {
    use super::NewTask;
    use super::Postgres;
    use super::Task;
    use crate::schema::fang_tasks;
    use crate::schema::FangTaskState;
    use chrono::{DateTime, Duration, Utc};
    use diesel::connection::Connection;
    use diesel::prelude::*;
    use diesel::result::Error;

    #[test]
    fn insert_inserts_task() {
        let postgres = Postgres::new(None);

        let new_task = NewTask {
            metadata: serde_json::json!(true),
        };

        let result = postgres
            .connection
            .test_transaction::<Task, Error, _>(|| postgres.insert(&new_task));

        assert_eq!(result.state, FangTaskState::New);
        assert_eq!(result.error_message, None);
    }

    #[test]
    fn fetch_task_fetches_the_oldest_task() {
        let postgres = Postgres::new(None);

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let timestamp1 = Utc::now() - Duration::hours(40);

            let task1 = insert_job(serde_json::json!(true), timestamp1, &postgres.connection);

            let timestamp2 = Utc::now() - Duration::hours(20);

            insert_job(serde_json::json!(false), timestamp2, &postgres.connection);

            let found_task = postgres.fetch_task().unwrap();

            assert_eq!(found_task.id, task1.id);

            Ok(())
        });
    }

    #[test]
    fn finish_task_updates_state_field() {
        let postgres = Postgres::new(None);

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_new_job(&postgres.connection);

            let updated_task = postgres.finish_task(&task).unwrap();

            assert_eq!(FangTaskState::Finished, updated_task.state);

            Ok(())
        });
    }

    #[test]
    fn fail_task_updates_state_field_and_sets_error_message() {
        let postgres = Postgres::new(None);

        postgres.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_new_job(&postgres.connection);
            let error = "Failed".to_string();

            let updated_task = postgres.fail_task(&task, error.clone()).unwrap();

            assert_eq!(FangTaskState::Failed, updated_task.state);
            assert_eq!(error, updated_task.error_message.unwrap());

            Ok(())
        });
    }

    // this test is ignored because it commits data to the db
    #[test]
    #[ignore]
    fn fetch_task_locks_the_record() {
        let postgres = Postgres::new(None);
        let timestamp1 = Utc::now() - Duration::hours(40);

        let task1 = insert_job(serde_json::json!(true), timestamp1, &postgres.connection);

        let task1_id = task1.id;

        let timestamp2 = Utc::now() - Duration::hours(20);

        let task2 = insert_job(serde_json::json!(false), timestamp2, &postgres.connection);

        let thread = std::thread::spawn(move || {
            let postgres = Postgres::new(None);

            postgres.connection.transaction::<(), Error, _>(|| {
                let found_task = postgres.fetch_task().unwrap();

                assert_eq!(found_task.id, task1.id);

                std::thread::sleep(std::time::Duration::from_millis(5000));

                Ok(())
            })
        });

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task = postgres.fetch_task().unwrap();

        assert_eq!(found_task.id, task2.id);

        let _result = thread.join();

        // returns unlocked record

        let found_task = postgres.fetch_task().unwrap();

        assert_eq!(found_task.id, task1_id);
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

    fn insert_new_job(connection: &PgConnection) -> Task {
        diesel::insert_into(fang_tasks::table)
            .values(&vec![(fang_tasks::metadata.eq(serde_json::json!(true)),)])
            .get_result::<Task>(connection)
            .unwrap()
    }
}
