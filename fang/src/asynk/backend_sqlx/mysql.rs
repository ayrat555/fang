const INSERT_TASK_QUERY_MYSQL: &str = include_str!("../queries_mysql/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_MYSQL: &str = include_str!("../queries_mysql/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_MYSQL: &str = include_str!("../queries_mysql/update_task_state.sql");
const FAIL_TASK_QUERY_MYSQL: &str = include_str!("../queries_mysql/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_MYSQL: &str = include_str!("../queries_mysql/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_MYSQL: &str =
    include_str!("../queries_mysql/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_MYSQL: &str = include_str!("../queries_mysql/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_MYSQL: &str =
    include_str!("../queries_mysql/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_MYSQL: &str = include_str!("../queries_mysql/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_MYSQL: &str = include_str!("../queries_mysql/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_MYSQL: &str =
    include_str!("../queries_mysql/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_MYSQL: &str = include_str!("../queries_mysql/find_task_by_id.sql");
const RETRY_TASK_QUERY_MYSQL: &str = include_str!("../queries_mysql/retry_task.sql");

use super::general_any_impl_fetch_task_type;
use super::general_any_impl_find_task_by_id;
use super::general_any_impl_find_task_by_uniq_hash;
use super::general_any_impl_remove_all_scheduled_tasks;
use super::general_any_impl_remove_all_task;
use super::general_any_impl_remove_task;
use super::general_any_impl_remove_task_by_metadata;
use super::general_any_impl_remove_task_type;
use super::{calculate_hash, QueryParams, Res, SqlXQuery};
use crate::{AsyncQueueError, FangTaskState, Task};
use SqlXQuery as Q;

use chrono::Duration;
use chrono::Utc;
use sqlx::{Any, Pool};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct BackendSqlXMySQL {}

impl BackendSqlXMySQL {
    pub(super) async fn execute_query(
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

    pub(super) fn _name() -> &'static str {
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
        return Err(AsyncQueueError::ResultError {
            expected: 1,
            found: affected_rows,
        });
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

    let affected_rows = sqlx::query(query)
        .bind(uuid_as_str)
        .bind(metadata_str)
        .bind(task_type)
        .bind(uniq_hash)
        .bind(scheduled_at_str)
        .execute(pool)
        .await?
        .rows_affected();

    if affected_rows != 1 {
        return Err(AsyncQueueError::ResultError {
            expected: 1,
            found: affected_rows,
        });
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
        return Err(AsyncQueueError::ResultError {
            expected: 1,
            found: affected_rows,
        });
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
        return Err(AsyncQueueError::ResultError {
            expected: 1,
            found: affected_rows,
        });
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
        return Err(AsyncQueueError::ResultError {
            expected: 1,
            found: affected_rows,
        });
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