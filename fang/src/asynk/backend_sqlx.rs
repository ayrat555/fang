use chrono::{DateTime, Duration, Utc};
use sha2::Digest;
use sha2::Sha256;
use sqlx::Any;
use sqlx::Pool;
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

    NoBackend,
}

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
    pub(crate) async fn execute_query<'a>(
        &self,
        _query: SqlXQuery,
        _pool: &Pool<Any>,
        _params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match self {
            #[cfg(feature = "asynk-postgres")]
            BackendSqlX::Pg => BackendSqlXPg::execute_query(_query, _pool, _params).await,
            #[cfg(feature = "asynk-sqlite")]
            BackendSqlX::Sqlite => BackendSqlXSQLite::execute_query(_query, _pool, _params).await,
            #[cfg(feature = "asynk-mysql")]
            BackendSqlX::MySql => BackendSqlXMySQL::execute_query(_query, _pool, _params).await,
            _ => unreachable!(),
        }
    }

    // I think it is useful to have this method, although it is not used
    pub(crate) fn _name(&self) -> &str {
        match self {
            #[cfg(feature = "asynk-postgres")]
            BackendSqlX::Pg => BackendSqlXPg::_name(),
            #[cfg(feature = "asynk-sqlite")]
            BackendSqlX::Sqlite => BackendSqlXSQLite::_name(),
            #[cfg(feature = "asynk-mysql")]
            BackendSqlX::MySql => BackendSqlXMySQL::_name(),
            _ => unreachable!(),
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

// Unwraps by QueryParams are safe because the responsibility is of the caller
// and the caller is the library itself

use crate::AsyncQueueError;
use crate::AsyncRunnable;
use crate::FangTaskState;
use crate::Task;

#[allow(dead_code)]
async fn general_any_impl_insert_task_if_not_exists(
    queries: (&str, &str),
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<Task, AsyncQueueError> {
    match general_any_impl_find_task_by_uniq_hash(queries.0, pool, &params).await {
        Some(task) => Ok(task),
        None => general_any_impl_insert_task_uniq(queries.1, pool, params).await,
    }
}

#[allow(dead_code)]
async fn general_any_impl_insert_task(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
pub(crate) fn calculate_hash(json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

#[allow(dead_code)]
async fn general_any_impl_insert_task_uniq(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
async fn general_any_impl_update_task_state(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
async fn general_any_impl_fail_task(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
async fn general_any_impl_remove_all_task(
    query: &str,
    pool: &Pool<Any>,
) -> Result<u64, AsyncQueueError> {
    Ok(sqlx::query(query).execute(pool).await?.rows_affected())
}

#[allow(dead_code)]
async fn general_any_impl_remove_all_scheduled_tasks(
    query: &str,
    pool: &Pool<Any>,
) -> Result<u64, AsyncQueueError> {
    let now_str = format!("{}", Utc::now().format("%F %T%.f+00"));

    Ok(sqlx::query(query)
        .bind(now_str)
        .execute(pool)
        .await?
        .rows_affected())
}

#[allow(dead_code)]
async fn general_any_impl_remove_task(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
async fn general_any_impl_remove_task_by_metadata(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<u64, AsyncQueueError> {
    let metadata = serde_json::to_value(params.runnable.unwrap())?;

    let uniq_hash = calculate_hash(&metadata.to_string());

    Ok(sqlx::query(query)
        .bind(uniq_hash)
        .execute(pool)
        .await?
        .rows_affected())
}

#[allow(dead_code)]
async fn general_any_impl_remove_task_type(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<u64, AsyncQueueError> {
    let task_type = params.task_type.unwrap();

    Ok(sqlx::query(query)
        .bind(task_type)
        .execute(pool)
        .await?
        .rows_affected())
}

#[allow(dead_code)]
async fn general_any_impl_fetch_task_type(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<Task, AsyncQueueError> {
    let task_type = params.task_type.unwrap();

    let now_str = format!("{}", Utc::now().format("%F %T%.f+00"));

    let task: Task = sqlx::query_as(query)
        .bind(task_type)
        .bind(now_str)
        .fetch_one(pool)
        .await?;

    Ok(task)
}

#[allow(dead_code)]
async fn general_any_impl_find_task_by_uniq_hash(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
async fn general_any_impl_find_task_by_id(
    query: &str,
    pool: &Pool<Any>,
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

#[allow(dead_code)]
async fn general_any_impl_retry_task(
    query: &str,
    pool: &Pool<Any>,
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
