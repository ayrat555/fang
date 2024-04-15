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

use chrono::Duration;
use chrono::{DateTime, Utc};
use sqlx::mysql::MySqlQueryResult;
use sqlx::mysql::MySqlRow;
use sqlx::FromRow;
use sqlx::MySql;
use sqlx::Pool;
use sqlx::Row;
use uuid::Uuid;
use SqlXQuery as Q;

use super::FangQueryable;
use super::{calculate_hash, QueryParams, Res, SqlXQuery};
use crate::{AsyncQueueError, FangTaskState, Task};

#[derive(Debug, Clone)]
pub(super) struct BackendSqlXMySQL {}

impl<'a> FromRow<'a, MySqlRow> for Task {
    fn from_row(row: &'a MySqlRow) -> Result<Self, sqlx::Error> {
        let uuid_as_text: &str = row.get("id");

        let id = Uuid::parse_str(uuid_as_text).unwrap();

        let raw: &str = row.get("metadata"); // will work if database cast json to string
        let raw = raw.replace('\\', "");

        // -- SELECT metadata->>'type' FROM fang_tasks ; this works because jsonb casting
        let metadata: serde_json::Value = serde_json::from_str(&raw).unwrap();

        // Be careful with this if we update sqlx, https://github.com/launchbadge/sqlx/issues/2416
        let error_message: Option<String> = row.get("error_message");

        let state_str: &str = row.get("state"); // will work if database cast json to string

        let state: FangTaskState = state_str.into();

        let task_type: String = row.get("task_type");

        // Be careful with this if we update sqlx, https://github.com/launchbadge/sqlx/issues/2416
        let uniq_hash: Option<String> = row.get("uniq_hash");

        let retries: i32 = row.get("retries");

        let scheduled_at: DateTime<Utc> = row.get("scheduled_at");

        let created_at: DateTime<Utc> = row.get("created_at");

        let updated_at: DateTime<Utc> = row.get("updated_at");

        Ok(Task::builder()
            .id(id)
            .metadata(metadata)
            .error_message(error_message)
            .state(state)
            .task_type(task_type)
            .uniq_hash(uniq_hash)
            .retries(retries)
            .scheduled_at(scheduled_at)
            .created_at(created_at)
            .updated_at(updated_at)
            .build())
    }
}

impl FangQueryable<MySql> for BackendSqlXMySQL {
    async fn insert_task(
        query: &str,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let uuid = Uuid::new_v4();
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_str: &str = uuid.as_hyphenated().encode_lower(&mut buffer);

        let scheduled_at = params.scheduled_at.unwrap();

        let metadata_str = params.metadata.unwrap().to_string();
        let task_type = params.task_type.unwrap();

        let affected_rows = Into::<MySqlQueryResult>::into(
            sqlx::query(query)
                .bind(uuid_as_str)
                .bind(metadata_str)
                .bind(task_type)
                .bind(scheduled_at)
                .execute(pool)
                .await?,
        )
        .rows_affected();

        if affected_rows != 1 {
            return Err(AsyncQueueError::ResultError {
                expected: 1,
                found: affected_rows,
            });
        }

        let query_params = QueryParams::builder().uuid(&uuid).build();

        let task: Task = <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_id(
            FIND_TASK_BY_ID_QUERY_MYSQL,
            pool,
            query_params,
        )
        .await?;

        Ok(task)
    }

    async fn update_task_state(
        query: &str,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now();

        let state_str: &str = params.state.unwrap().into();

        let uuid = params.uuid.unwrap();

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = uuid.as_hyphenated().encode_lower(&mut buffer);

        let affected_rows = Into::<MySqlQueryResult>::into(
            sqlx::query(query)
                .bind(state_str)
                .bind(updated_at)
                .bind(&*uuid_as_text)
                .execute(pool)
                .await?,
        )
        .rows_affected();

        if affected_rows != 1 {
            return Err(AsyncQueueError::ResultError {
                expected: 1,
                found: affected_rows,
            });
        }

        let query_params = QueryParams::builder().uuid(params.uuid.unwrap()).build();

        let task: Task = <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_id(
            FIND_TASK_BY_ID_QUERY_MYSQL,
            pool,
            query_params,
        )
        .await?;

        Ok(task)
    }

    async fn insert_task_uniq(
        query: &str,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let uuid = Uuid::new_v4();
        let mut buffer = Uuid::encode_buffer();
        let uuid_as_str: &str = uuid.as_hyphenated().encode_lower(&mut buffer);

        let metadata = params.metadata.unwrap();

        let metadata_str = metadata.to_string();

        let scheduled_at = params.scheduled_at.unwrap();

        let task_type = params.task_type.unwrap();

        let uniq_hash = calculate_hash(&metadata_str);

        let affected_rows = Into::<MySqlQueryResult>::into(
            sqlx::query(query)
                .bind(uuid_as_str)
                .bind(metadata_str)
                .bind(task_type)
                .bind(uniq_hash)
                .bind(scheduled_at)
                .execute(pool)
                .await?,
        )
        .rows_affected();

        if affected_rows != 1 {
            return Err(AsyncQueueError::ResultError {
                expected: 1,
                found: affected_rows,
            });
        }

        let query_params = QueryParams::builder().uuid(&uuid).build();

        let task: Task = <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_id(
            FIND_TASK_BY_ID_QUERY_MYSQL,
            pool,
            query_params,
        )
        .await?;

        Ok(task)
    }

    async fn fail_task(
        query: &str,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now();

        let id = params.task.unwrap().id;

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = id.as_hyphenated().encode_lower(&mut buffer);

        let error_message = params.error_message.unwrap();

        let affected_rows = Into::<MySqlQueryResult>::into(
            sqlx::query(query)
                .bind(<&str>::from(FangTaskState::Failed))
                .bind(error_message)
                .bind(updated_at)
                .bind(&*uuid_as_text)
                .execute(pool)
                .await?,
        )
        .rows_affected();

        if affected_rows != 1 {
            return Err(AsyncQueueError::ResultError {
                expected: 1,
                found: affected_rows,
            });
        }

        let query_params = QueryParams::builder().uuid(&id).build();

        let failed_task: Task = <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_id(
            FIND_TASK_BY_ID_QUERY_MYSQL,
            pool,
            query_params,
        )
        .await?;

        Ok(failed_task)
    }

    async fn retry_task(
        query: &str,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        let now = Utc::now();

        let scheduled_at = now + Duration::seconds(params.backoff_seconds.unwrap() as i64);

        let retries = params.task.unwrap().retries + 1;

        let uuid = params.task.unwrap().id;

        let mut buffer = Uuid::encode_buffer();
        let uuid_as_text = uuid.as_hyphenated().encode_lower(&mut buffer);

        let error = params.error_message.unwrap();

        let affected_rows = Into::<MySqlQueryResult>::into(
            sqlx::query(query)
                .bind(error)
                .bind(retries)
                .bind(scheduled_at)
                .bind(now)
                .bind(&*uuid_as_text)
                .execute(pool)
                .await?,
        )
        .rows_affected();

        if affected_rows != 1 {
            return Err(AsyncQueueError::ResultError {
                expected: 1,
                found: affected_rows,
            });
        }

        let query_params = QueryParams::builder().uuid(&uuid).build();

        let failed_task: Task = <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_id(
            FIND_TASK_BY_ID_QUERY_MYSQL,
            pool,
            query_params,
        )
        .await?;

        Ok(failed_task)
    }

    async fn insert_task_if_not_exists(
        queries: (&str, &str),
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        match <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_uniq_hash(
            queries.0, pool, &params,
        )
        .await
        {
            Some(task) => Ok(task),
            None => {
                <BackendSqlXMySQL as FangQueryable<MySql>>::insert_task_uniq(
                    queries.1, pool, params,
                )
                .await
            }
        }
    }

    async fn find_task_by_id(
        query: &str,
        pool: &Pool<MySql>,
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

    async fn fetch_task_type(
        query: &str,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Task, AsyncQueueError> {
        // Unwraps by QueryParams are safe because the responsibility is of the caller
        // and the caller is the library itself
        let task_type = params.task_type.unwrap();

        let now = Utc::now();

        let task: Task = sqlx::query_as(query)
            .bind(task_type)
            .bind(now)
            .fetch_one(pool)
            .await?;

        Ok(task)
    }

    async fn remove_all_scheduled_tasks(
        query: &str,
        pool: &Pool<MySql>,
    ) -> Result<u64, AsyncQueueError> {
        let now = Utc::now();

        // This converts <DB>QueryResult to AnyQueryResult and then to u64
        // do not delete into() method and do not delete Into<AnyQueryResult> trait bound

        Ok(
            Into::<MySqlQueryResult>::into(sqlx::query(query).bind(now).execute(pool).await?)
                .rows_affected(),
        )
    }
}

impl BackendSqlXMySQL {
    pub(super) async fn execute_query(
        query: SqlXQuery,
        pool: &Pool<MySql>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match query {
            Q::InsertTask => {
                let task = <BackendSqlXMySQL as FangQueryable<MySql>>::insert_task(
                    INSERT_TASK_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::UpdateTaskState => {
                let task = <BackendSqlXMySQL as FangQueryable<MySql>>::update_task_state(
                    UPDATE_TASK_STATE_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }

            Q::FailTask => {
                let task = <BackendSqlXMySQL as FangQueryable<MySql>>::fail_task(
                    FAIL_TASK_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }

            Q::RemoveAllTask => {
                let affected_rows = <BackendSqlXMySQL as FangQueryable<MySql>>::remove_all_task(
                    REMOVE_ALL_TASK_QUERY_MYSQL,
                    pool,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }

            Q::RemoveAllScheduledTask => {
                let affected_rows =
                    <BackendSqlXMySQL as FangQueryable<MySql>>::remove_all_scheduled_tasks(
                        REMOVE_ALL_SCHEDULED_TASK_QUERY_MYSQL,
                        pool,
                    )
                    .await?;

                Ok(Res::Bigint(affected_rows))
            }

            Q::RemoveTask => {
                let affected_rows = <BackendSqlXMySQL as FangQueryable<MySql>>::remove_task(
                    REMOVE_TASK_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskByMetadata => {
                let affected_rows =
                    <BackendSqlXMySQL as FangQueryable<MySql>>::remove_task_by_metadata(
                        REMOVE_TASK_BY_METADATA_QUERY_MYSQL,
                        pool,
                        params,
                    )
                    .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskType => {
                let affected_rows = <BackendSqlXMySQL as FangQueryable<MySql>>::remove_task_type(
                    REMOVE_TASKS_TYPE_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::FetchTaskType => {
                let task = <BackendSqlXMySQL as FangQueryable<MySql>>::fetch_task_type(
                    FETCH_TASK_TYPE_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FindTaskById => {
                let task: Task = <BackendSqlXMySQL as FangQueryable<MySql>>::find_task_by_id(
                    FIND_TASK_BY_ID_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::RetryTask => {
                let task = <BackendSqlXMySQL as FangQueryable<MySql>>::retry_task(
                    RETRY_TASK_QUERY_MYSQL,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::InsertTaskIfNotExists => {
                let task = <BackendSqlXMySQL as FangQueryable<MySql>>::insert_task_if_not_exists(
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
