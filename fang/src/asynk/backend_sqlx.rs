use chrono::{DateTime, Duration, Utc};
use sha2::Digest;
use sha2::Sha256;
use sqlx::any::AnyQueryResult;
use sqlx::database::HasArguments;
use sqlx::Database;
use sqlx::Encode;
use sqlx::Executor;
use sqlx::FromRow;
use sqlx::IntoArguments;
use sqlx::Pool;
use sqlx::Type;
use std::fmt::Debug;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(feature = "asynk-postgres")]
mod postgres;
#[cfg(feature = "asynk-postgres")]
use self::postgres::BackendSqlXPg;

#[cfg(feature = "asynk-sqlite")]
mod sqlite;
#[cfg(feature = "asynk-sqlite")]
use self::sqlite::BackendSqlXSQLite;
#[cfg(feature = "asynk-mysql")]
mod mysql;
#[cfg(feature = "asynk-mysql")]
use self::mysql::BackendSqlXMySQL;

#[derive(Debug, Clone)]
pub(crate) enum BackendSqlX {
    #[cfg(feature = "asynk-postgres")]
    Pg,

    #[cfg(feature = "asynk-sqlite")]
    Sqlite,

    #[cfg(feature = "asynk-mysql")]
    MySql,

    #[cfg(not(any(feature = "asynk-postgres", feature = "asynk-sqlite", feature = "asynk-mysql")))]
    #[allow(dead_code)]
    Dummy,
}

#[allow(dead_code)]
#[derive(TypedBuilder, Clone)]
pub(crate) struct QueryParams<'a> {
    #[builder(default, setter(strip_option))]
    uuid: Option<&'a Uuid>,
    #[builder(default, setter(strip_option))]
    metadata: Option<&'a serde_json::Value>,
    #[builder(default, setter(strip_option))]
    task_type: Option<&'a str>,
    #[builder(default, setter(strip_option))]
    scheduled_at: Option<&'a DateTime<Utc>>,
    #[builder(default, setter(strip_option))]
    state: Option<FangTaskState>,
    #[builder(default, setter(strip_option))]
    error_message: Option<&'a str>,
    #[builder(default, setter(strip_option))]
    runnable: Option<&'a dyn AsyncRunnable>,
    #[builder(default, setter(strip_option))]
    backoff_seconds: Option<u32>,
    #[builder(default, setter(strip_option))]
    task: Option<&'a Task>,
}

#[allow(dead_code)]
pub(crate) enum Res {
    Bigint(u64),
    Task(Task),
}

impl Res {
    pub(crate) fn unwrap_u64(self) -> u64 {
        match self {
            Res::Bigint(val) => val,
            _ => panic!("Can not unwrap a u64"),
        }
    }

    pub(crate) fn unwrap_task(self) -> Task {
        match self {
            Res::Task(task) => task,
            _ => panic!("Can not unwrap a task"),
        }
    }
}

impl BackendSqlX {
    pub(crate) async fn execute_query(
        &self,
        _query: SqlXQuery,
        _pool: &InternalPool,
        _params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match *self {
            #[cfg(feature = "asynk-postgres")]
            BackendSqlX::Pg => {
                BackendSqlXPg::execute_query(_query, _pool.unwrap_pg_pool(), _params).await
            }
            #[cfg(feature = "asynk-sqlite")]
            BackendSqlX::Sqlite => {
                BackendSqlXSQLite::execute_query(_query, _pool.unwrap_sqlite_pool(), _params).await
            }
            #[cfg(feature = "asynk-mysql")]
            BackendSqlX::MySql => {
                BackendSqlXMySQL::execute_query(_query, _pool.unwrap_mysql_pool(), _params).await
            }
            #[cfg(not(any(feature = "asynk-postgres", feature = "asynk-sqlite", feature = "asynk-mysql")))]
            BackendSqlX::Dummy => unreachable!(),
        }
    }

    // I think it is useful to have this method, although it is not used
    pub(crate) fn _name(&self) -> &str {
        match *self {
            #[cfg(feature = "asynk-postgres")]
            BackendSqlX::Pg => BackendSqlXPg::_name(),
            #[cfg(feature = "asynk-sqlite")]
            BackendSqlX::Sqlite => BackendSqlXSQLite::_name(),
            #[cfg(feature = "asynk-mysql")]
            BackendSqlX::MySql => BackendSqlXMySQL::_name(),
            #[cfg(not(any(feature = "asynk-postgres", feature = "asynk-sqlite", feature = "asynk-mysql")))]
            BackendSqlX::Dummy => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SqlXQuery {
    InsertTask,
    UpdateTaskState,
    FailTask,
    RemoveAllTask,
    RemoveAllScheduledTask,
    RemoveTask,
    RemoveTaskByMetadata,
    RemoveTaskType,
    FetchTaskType,
    FindTaskById,
    RetryTask,
    InsertTaskIfNotExists,
}

use crate::AsyncQueueError;
use crate::AsyncRunnable;
use crate::FangTaskState;
use crate::InternalPool;
use crate::Task;

#[allow(dead_code)]
pub(crate) fn calculate_hash(json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

trait FangQueryable<DB>
where
    DB: Database,
    for<'r> Task: FromRow<'r, <DB as sqlx::Database>::Row>,
    for<'r> std::string::String: Encode<'r, DB> + Type<DB>,
    for<'r> &'r str: Encode<'r, DB> + Type<DB>,
    for<'r> i32: Encode<'r, DB> + Type<DB>,
    for<'r> &'r Pool<DB>: Executor<'r, Database = DB>,
    for<'r> <DB as HasArguments<'r>>::Arguments: IntoArguments<'r, DB>,
    <DB as Database>::QueryResult: Into<AnyQueryResult>,
{
    async fn fetch_task_type(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        // Unwraps by QueryParams are safe because the responsibility is of the caller
        // and the caller is the library itself
        let task_type = params.task_type.unwrap();

        let now_str = format!("{}", Utc::now().format("%F %T%.f+00"));

        let task: Task = sqlx::query_as(query)
            .bind(task_type)
            .bind(now_str)
            .fetch_one(pool)
            .await?;

        Ok(task)
    }

    async fn find_task_by_uniq_hash(
        query: &str,
        pool: &Pool<DB>,
        params: &QueryParams<'_>,
    ) -> Option<Task> {
        let metadata = params.metadata.unwrap();

        let uniq_hash = calculate_hash(&metadata.to_string());

        sqlx::query_as(query)
            .bind(uniq_hash)
            .fetch_one(pool)
            .await
            .ok()
    }

    async fn find_task_by_id(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = params
            .uuid
            .unwrap()
            .as_hyphenated()
            .encode_lower(&mut buffer);

        let task: Task = sqlx::query_as(query)
            .bind(&*uuid_as_text)
            .fetch_one(pool)
            .await?;

        Ok(task)
    }

    async fn retry_task(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let now = Utc::now();
        let now_str = format!("{}", now.format("%F %T%.f+00"));

        let scheduled_at = now + Duration::seconds(params.backoff_seconds.unwrap() as i64);
        let scheduled_at_str = format!("{}", scheduled_at.format("%F %T%.f+00"));
        let retries = params.task.unwrap().retries + 1;

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = params
            .task
            .unwrap()
            .id
            .as_hyphenated()
            .encode_lower(&mut buffer);

        let error = params.error_message.unwrap();

        let failed_task: Task = sqlx::query_as(query)
            .bind(error)
            .bind(retries)
            .bind(scheduled_at_str)
            .bind(now_str)
            .bind(&*uuid_as_text)
            .fetch_one(pool)
            .await?;

        Ok(failed_task)
    }

    async fn insert_task_uniq(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let uuid = Uuid::new_v4();
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_str: &str = uuid.as_hyphenated().encode_lower(&mut buffer);

        let metadata = params.metadata.unwrap();

        let metadata_str = metadata.to_string();
        let scheduled_at_str = format!("{}", params.scheduled_at.unwrap().format("%F %T%.f+00"));

        let task_type = params.task_type.unwrap();

        let uniq_hash = calculate_hash(&metadata_str);

        let task: Task = sqlx::query_as(query)
            .bind(uuid_as_str)
            .bind(metadata_str)
            .bind(task_type)
            .bind(uniq_hash)
            .bind(scheduled_at_str)
            .fetch_one(pool)
            .await?;
        Ok(task)
    }

    async fn insert_task(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let uuid = Uuid::new_v4();
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_str: &str = uuid.as_hyphenated().encode_lower(&mut buffer);

        let scheduled_at_str = format!("{}", params.scheduled_at.unwrap().format("%F %T%.f+00"));

        let metadata_str = params.metadata.unwrap().to_string();
        let task_type = params.task_type.unwrap();

        let task: Task = sqlx::query_as(query)
            .bind(uuid_as_str)
            .bind(metadata_str)
            .bind(task_type)
            .bind(scheduled_at_str)
            .fetch_one(pool)
            .await?;

        Ok(task)
    }

    async fn update_task_state(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at_str = format!("{}", Utc::now().format("%F %T%.f+00"));

        let state_str: &str = params.state.unwrap().into();

        let uuid = params.uuid.unwrap();

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = uuid.as_hyphenated().encode_lower(&mut buffer);

        let task: Task = sqlx::query_as(query)
            .bind(state_str)
            .bind(updated_at_str)
            .bind(&*uuid_as_text)
            .fetch_one(pool)
            .await?;

        Ok(task)
    }

    async fn fail_task(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = format!("{}", Utc::now().format("%F %T%.f+00"));

        let id = params.task.unwrap().id;

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = id.as_hyphenated().encode_lower(&mut buffer);

        let error_message = params.error_message.unwrap();

        let failed_task: Task = sqlx::query_as(query)
            .bind(<&str>::from(FangTaskState::Failed))
            .bind(error_message)
            .bind(updated_at)
            .bind(&*uuid_as_text)
            .fetch_one(pool)
            .await?;

        Ok(failed_task)
    }

    async fn remove_all_task(query: &str, pool: &Pool<DB>) -> Result<u64, AsyncQueueError> {
        // This converts <DB>QueryResult to AnyQueryResult and then to u64
        // do not delete into() method and do not delete Into<AnyQueryResult> trait bound
        Ok(sqlx::query(query)
            .execute(pool)
            .await?
            .into()
            .rows_affected())
    }

    async fn remove_all_scheduled_tasks(
        query: &str,
        pool: &Pool<DB>,
    ) -> Result<u64, AsyncQueueError> {
        let now_str = format!("{}", Utc::now().format("%F %T%.f+00"));

        // This converts <DB>QueryResult to AnyQueryResult and then to u64
        // do not delete into() method and do not delete Into<AnyQueryResult> trait bound

        Ok(sqlx::query(query)
            .bind(now_str)
            .execute(pool)
            .await?
            .into()
            .rows_affected())
    }

    async fn remove_task(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<u64, AsyncQueueError> {
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = params
            .uuid
            .unwrap()
            .as_hyphenated()
            .encode_lower(&mut buffer);

        let result = sqlx::query(query)
            .bind(&*uuid_as_text)
            .execute(pool)
            .await?
            .into()
            .rows_affected();

        if result != 1 {
            Err(AsyncQueueError::ResultError {
                expected: 1,
                found: result,
            })
        } else {
            Ok(result)
        }
    }

    async fn remove_task_by_metadata(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(params.runnable.unwrap())?;

        let uniq_hash = calculate_hash(&metadata.to_string());

        Ok(sqlx::query(query)
            .bind(uniq_hash)
            .execute(pool)
            .await?
            .into()
            .rows_affected())
    }

    async fn remove_task_type(
        query: &str,
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<u64, AsyncQueueError> {
        let task_type = params.task_type.unwrap();

        Ok(sqlx::query(query)
            .bind(task_type)
            .execute(pool)
            .await?
            .into()
            .rows_affected())
    }

    async fn insert_task_if_not_exists(
        queries: (&str, &str),
        pool: &Pool<DB>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        match Self::find_task_by_uniq_hash(queries.0, pool, &params).await {
            Some(task) => Ok(task),
            None => Self::insert_task_uniq(queries.1, pool, params).await,
        }
    }
}
