use crate::schema::fang_tasks;
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
}
