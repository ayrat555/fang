use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Any;
use sqlx::Pool;
use std::fmt::Debug;
use typed_builder::TypedBuilder;
use uuid::Uuid;

const INSERT_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_POSTGRES: &str = include_str!("queries_postgres/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/update_task_state.sql");
const FAIL_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_POSTGRES: &str = include_str!("queries_postgres/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_POSTGRES: &str = include_str!("queries_postgres/find_task_by_id.sql");
const RETRY_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/retry_task.sql");

const INSERT_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_SQLITE: &str = include_str!("queries_sqlite/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_SQLITE: &str = include_str!("queries_sqlite/update_task_state.sql");
const FAIL_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE: &str =
    include_str!("queries_sqlite/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_SQLITE: &str =
    include_str!("queries_sqlite/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_SQLITE: &str = include_str!("queries_sqlite/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_SQLITE: &str = include_str!("queries_sqlite/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_SQLITE: &str =
    include_str!("queries_sqlite/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_SQLITE: &str = include_str!("queries_sqlite/find_task_by_id.sql");
const RETRY_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/retry_task.sql");

const INSERT_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_MYSQL: &str = include_str!("queries_mysql/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_MYSQL: &str = include_str!("queries_mysql/update_task_state.sql");
const FAIL_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_MYSQL: &str =
    include_str!("queries_mysql/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_MYSQL: &str =
    include_str!("queries_mysql/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_MYSQL: &str = include_str!("queries_mysql/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_MYSQL: &str = include_str!("queries_mysql/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_MYSQL: &str =
    include_str!("queries_mysql/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_MYSQL: &str = include_str!("queries_mysql/find_task_by_id.sql");
const RETRY_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/retry_task.sql");

#[derive(Debug, Clone)]
pub(crate) enum BackendSqlX {
    Pg,
    Sqlite,
    Mysql,
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
    pub fn _new_with_name(name: &str) -> BackendSqlX {
        match name {
            "PostgreSQL" => BackendSqlX::Pg,
            "SQLite" => BackendSqlX::Sqlite,
            "MySQL" => BackendSqlX::Mysql,
            _ => unreachable!(),
        }
    }

    pub(crate) async fn execute_query<'a>(
        &self,
        query: SqlXQuery,
        pool: &Pool<Any>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match self {
            BackendSqlX::Pg => BackendSqlXPg::execute_query(query, pool, params).await,
            BackendSqlX::Sqlite => BackendSqlXSQLite::execute_query(query, pool, params).await,
            BackendSqlX::Mysql => BackendSqlXMySQL::execute_query(query, pool, params).await,
            _ => unreachable!(),
        }
    }

    // I think it is useful to have this method, although it is not used
    pub(crate) fn _name(&self) -> &str {
        match self {
            BackendSqlX::Pg => BackendSqlXPg::_name(),
            BackendSqlX::Sqlite => BackendSqlXSQLite::_name(),
            BackendSqlX::Mysql => BackendSqlXMySQL::_name(),
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

#[derive(Debug, Clone)]
struct BackendSqlXPg {}

use SqlXQuery as Q;

use crate::AsyncRunnable;
use crate::FangTaskState;
use crate::{AsyncQueueError, Task};
impl BackendSqlXPg {
    async fn execute_query(
        query: SqlXQuery,
        pool: &Pool<Any>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match query {
            Q::InsertTask => {
                let task =
                    general_any_impl_insert_task(INSERT_TASK_QUERY_POSTGRES, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::UpdateTaskState => {
                let task = general_any_impl_update_task_state(
                    UPDATE_TASK_STATE_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FailTask => {
                let task =
                    general_any_impl_fail_task(FAIL_TASK_QUERY_POSTGRES, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::RemoveAllTask => {
                let affected_rows =
                    general_any_impl_remove_all_task(REMOVE_ALL_TASK_QUERY_POSTGRES, pool).await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveAllScheduledTask => {
                let affected_rows = general_any_impl_remove_all_scheduled_tasks(
                    REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES,
                    pool,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTask => {
                let affected_rows =
                    general_any_impl_remove_task(REMOVE_TASK_QUERY_POSTGRES, pool, params).await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskByMetadata => {
                let affected_rows = general_any_impl_remove_task_by_metadata(
                    REMOVE_TASK_BY_METADATA_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskType => {
                let affected_rows = general_any_impl_remove_task_type(
                    REMOVE_TASKS_TYPE_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::FetchTaskType => {
                let task =
                    general_any_impl_fetch_task_type(FETCH_TASK_TYPE_QUERY_POSTGRES, pool, params)
                        .await?;
                Ok(Res::Task(task))
            }
            Q::FindTaskById => {
                let task =
                    general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_POSTGRES, pool, params)
                        .await?;
                Ok(Res::Task(task))
            }
            Q::RetryTask => {
                let task =
                    general_any_impl_retry_task(RETRY_TASK_QUERY_POSTGRES, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::InsertTaskIfNotExists => {
                let task = general_any_impl_insert_task_if_not_exists(
                    (
                        FIND_TASK_BY_UNIQ_HASH_QUERY_POSTGRES,
                        INSERT_TASK_UNIQ_QUERY_POSTGRES,
                    ),
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
        }
    }

    fn _name() -> &'static str {
        "PostgreSQL"
    }
}

#[derive(Debug, Clone)]
struct BackendSqlXSQLite {}

impl BackendSqlXSQLite {
    async fn execute_query(
        query: SqlXQuery,
        pool: &Pool<Any>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match query {
            Q::InsertTask => {
                let task =
                    general_any_impl_insert_task(INSERT_TASK_QUERY_SQLITE, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::UpdateTaskState => {
                let task = general_any_impl_update_task_state(
                    UPDATE_TASK_STATE_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FailTask => {
                let task = general_any_impl_fail_task(FAIL_TASK_QUERY_SQLITE, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::RemoveAllTask => {
                let affected_rows =
                    general_any_impl_remove_all_task(REMOVE_ALL_TASK_QUERY_SQLITE, pool).await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveAllScheduledTask => {
                let affected_rows = general_any_impl_remove_all_scheduled_tasks(
                    REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE,
                    pool,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTask => {
                let affected_rows =
                    general_any_impl_remove_task(REMOVE_TASK_QUERY_SQLITE, pool, params).await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskByMetadata => {
                let affected_rows = general_any_impl_remove_task_by_metadata(
                    REMOVE_TASK_BY_METADATA_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskType => {
                let affected_rows =
                    general_any_impl_remove_task_type(REMOVE_TASKS_TYPE_QUERY_SQLITE, pool, params)
                        .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::FetchTaskType => {
                let task =
                    general_any_impl_fetch_task_type(FETCH_TASK_TYPE_QUERY_SQLITE, pool, params)
                        .await?;
                Ok(Res::Task(task))
            }
            Q::FindTaskById => {
                let task =
                    general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_SQLITE, pool, params)
                        .await?;
                Ok(Res::Task(task))
            }
            Q::RetryTask => {
                let task =
                    general_any_impl_retry_task(RETRY_TASK_QUERY_SQLITE, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::InsertTaskIfNotExists => {
                let task = general_any_impl_insert_task_if_not_exists(
                    (
                        FIND_TASK_BY_UNIQ_HASH_QUERY_SQLITE,
                        INSERT_TASK_UNIQ_QUERY_SQLITE,
                    ),
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
        }
    }

    fn _name() -> &'static str {
        "SQLite"
    }
}

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

pub(crate) fn calculate_hash(json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

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

async fn general_any_impl_remove_all_task(
    query: &str,
    pool: &Pool<Any>,
) -> Result<u64, AsyncQueueError> {
    Ok(sqlx::query(query).execute(pool).await?.rows_affected())
}

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

async fn general_any_impl_remove_task_by_metadata(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<u64, AsyncQueueError> {
    let metadata = serde_json::to_value(params.runnable.unwrap())?;

    let uniq_hash = calculate_hash(&metadata.to_string());

    println!("{query}");

    let adquire = pool;

    println!("Adquire {:?}", adquire);

    Ok(sqlx::query(query)
        .bind(uniq_hash)
        .execute(adquire)
        .await?
        .rows_affected())
}

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

#[derive(Debug, Clone)]
struct BackendSqlXMySQL {}

impl BackendSqlXMySQL {
    async fn execute_query(
        query: SqlXQuery,
        pool: &Pool<Any>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match query {
            Q::InsertTask => {
                let task = mysql_impl_insert_task(INSERT_TASK_QUERY_MYSQL, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::UpdateTaskState => {
                let task =
                    mysql_impl_update_task_state(UPDATE_TASK_STATE_QUERY_MYSQL, pool, params)
                        .await?;
                Ok(Res::Task(task))
            }

            Q::FailTask => {
                let task = mysql_impl_fail_task(FAIL_TASK_QUERY_MYSQL, pool, params).await?;

                Ok(Res::Task(task))
            }

            Q::RemoveAllTask => {
                let affected_rows =
                    general_any_impl_remove_all_task(REMOVE_ALL_TASK_QUERY_MYSQL, pool).await?;

                Ok(Res::Bigint(affected_rows))
            }

            Q::RemoveAllScheduledTask => {
                let affected_rows = general_any_impl_remove_all_scheduled_tasks(
                    REMOVE_ALL_SCHEDULED_TASK_QUERY_MYSQL,
                    pool,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }

            Q::RemoveTask => {
                let affected_rows =
                    general_any_impl_remove_task(REMOVE_TASK_QUERY_MYSQL, pool, params).await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskByMetadata => {
                let affected_rows = general_any_impl_remove_task_by_metadata(
                    REMOVE_TASK_BY_METADATA_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskType => {
                let affected_rows =
                    general_any_impl_remove_task_type(REMOVE_TASKS_TYPE_QUERY_MYSQL, pool, params)
                        .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::FetchTaskType => {
                let task =
                    general_any_impl_fetch_task_type(FETCH_TASK_TYPE_QUERY_MYSQL, pool, params)
                        .await?;
                Ok(Res::Task(task))
            }
            Q::FindTaskById => {
                let task: Task =
                    general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_MYSQL, pool, params)
                        .await?;

                Ok(Res::Task(task))
            }
            Q::RetryTask => {
                let task = mysql_impl_retry_task(RETRY_TASK_QUERY_MYSQL, pool, params).await?;

                Ok(Res::Task(task))
            }
            Q::InsertTaskIfNotExists => {
                let task = mysql_any_impl_insert_task_if_not_exists(
                    (
                        FIND_TASK_BY_UNIQ_HASH_QUERY_MYSQL,
                        INSERT_TASK_UNIQ_QUERY_MYSQL,
                    ),
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
        }
    }

    fn _name() -> &'static str {
        "MySQL"
    }
}

async fn mysql_impl_insert_task(
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

    let affected_rows = sqlx::query(query)
        .bind(uuid_as_str)
        .bind(metadata_str)
        .bind(task_type)
        .bind(scheduled_at_str)
        .execute(pool)
        .await?
        .rows_affected();

    if affected_rows != 1 {
        // here we should return an error
        panic!("fock")
    }

    let query_params = QueryParams::builder().uuid(&uuid).build();

    let task: Task =
        general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_MYSQL, pool, query_params).await?;

    Ok(task)
}

async fn mysql_impl_insert_task_uniq(
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

    println!("{} len : {}", uniq_hash, uniq_hash.len());

    println!("reach here");

    let affected_rows = sqlx::query(query)
        .bind(uuid_as_str)
        .bind(metadata_str)
        .bind(task_type)
        .bind(uniq_hash)
        .bind(scheduled_at_str)
        .execute(pool)
        .await?
        .rows_affected();

    println!("reach here 3");

    if affected_rows != 1 {
        // here we should return an error
        panic!("fock")
    }

    let query_params = QueryParams::builder().uuid(&uuid).build();

    let task: Task =
        general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_MYSQL, pool, query_params).await?;

    Ok(task)
}

async fn mysql_impl_update_task_state(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<Task, AsyncQueueError> {
    let updated_at_str = format!("{}", Utc::now().format("%F %T%.f+00"));

    let state_str: &str = params.state.unwrap().into();

    let uuid = params.uuid.unwrap();

    let mut buffer = Uuid::encode_buffer();
    let uuid_as_text = uuid.as_hyphenated().encode_lower(&mut buffer);

    let affected_rows = sqlx::query(query)
        .bind(state_str)
        .bind(updated_at_str)
        .bind(&*uuid_as_text)
        .execute(pool)
        .await?
        .rows_affected();

    if affected_rows != 1 {
        // here we should return an error
        panic!("fock")
    }

    let query_params = QueryParams::builder().uuid(params.uuid.unwrap()).build();

    let task: Task =
        general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_MYSQL, pool, query_params).await?;

    Ok(task)
}

async fn mysql_impl_fail_task(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<Task, AsyncQueueError> {
    let updated_at = format!("{}", Utc::now().format("%F %T%.f+00"));

    let id = params.task.unwrap().id;

    let mut buffer = Uuid::encode_buffer();
    let uuid_as_text = id.as_hyphenated().encode_lower(&mut buffer);

    let error_message = params.error_message.unwrap();

    let affected_rows = sqlx::query(query)
        .bind(<&str>::from(FangTaskState::Failed))
        .bind(error_message)
        .bind(updated_at)
        .bind(&*uuid_as_text)
        .execute(pool)
        .await?
        .rows_affected();

    if affected_rows != 1 {
        // here we should return an error
        panic!("fock")
    }

    let query_params = QueryParams::builder().uuid(&id).build();

    let failed_task: Task =
        general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_MYSQL, pool, query_params).await?;

    Ok(failed_task)
}

async fn mysql_impl_retry_task(
    query: &str,
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<Task, AsyncQueueError> {
    let now = Utc::now();
    let now_str = format!("{}", now.format("%F %T%.f+00"));

    let scheduled_at = now + Duration::seconds(params.backoff_seconds.unwrap() as i64);
    let scheduled_at_str = format!("{}", scheduled_at.format("%F %T%.f+00"));
    let retries = params.task.unwrap().retries + 1;

    let uuid = params.task.unwrap().id;

    let mut buffer = Uuid::encode_buffer();
    let uuid_as_text = uuid.as_hyphenated().encode_lower(&mut buffer);

    let error = params.error_message.unwrap();

    let affected_rows = sqlx::query(query)
        .bind(error)
        .bind(retries)
        .bind(scheduled_at_str)
        .bind(now_str)
        .bind(&*uuid_as_text)
        .execute(pool)
        .await?
        .rows_affected();

    if affected_rows != 1 {
        // here we should return an error
        panic!("fock")
    }

    let query_params = QueryParams::builder().uuid(&uuid).build();

    let failed_task: Task =
        general_any_impl_find_task_by_id(FIND_TASK_BY_ID_QUERY_MYSQL, pool, query_params).await?;

    Ok(failed_task)
}

async fn mysql_any_impl_insert_task_if_not_exists(
    queries: (&str, &str),
    pool: &Pool<Any>,
    params: QueryParams<'_>,
) -> Result<Task, AsyncQueueError> {
    match general_any_impl_find_task_by_uniq_hash(queries.0, pool, &params).await {
        Some(task) => Ok(task),
        None => mysql_impl_insert_task_uniq(queries.1, pool, params).await,
    }
}
