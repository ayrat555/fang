const INSERT_TASK_QUERY_SQLITE: &str = include_str!("../queries_sqlite/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_SQLITE: &str = include_str!("../queries_sqlite/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_SQLITE: &str =
    include_str!("../queries_sqlite/update_task_state.sql");
const FAIL_TASK_QUERY_SQLITE: &str = include_str!("../queries_sqlite/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_SQLITE: &str = include_str!("../queries_sqlite/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE: &str =
    include_str!("../queries_sqlite/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_SQLITE: &str = include_str!("../queries_sqlite/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_SQLITE: &str =
    include_str!("../queries_sqlite/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_SQLITE: &str =
    include_str!("../queries_sqlite/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_SQLITE: &str = include_str!("../queries_sqlite/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_SQLITE: &str =
    include_str!("../queries_sqlite/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_SQLITE: &str = include_str!("../queries_sqlite/find_task_by_id.sql");
const RETRY_TASK_QUERY_SQLITE: &str = include_str!("../queries_sqlite/retry_task.sql");

#[derive(Debug, Clone)]
pub(super) struct BackendSqlXSQLite {}

use super::FangQueryable;
use super::{QueryParams, Res, SqlXQuery};
use crate::AsyncQueueError;
use crate::FangTaskState;
use crate::Task;
use chrono::{DateTime, Utc};
use sqlx::sqlite::SqliteRow;
use sqlx::FromRow;
use sqlx::Pool;
use sqlx::Row;
use sqlx::Sqlite;
use uuid::Uuid;
use SqlXQuery as Q;

impl<'a> FromRow<'a, SqliteRow> for Task {
    fn from_row(row: &'a SqliteRow) -> Result<Self, sqlx::Error> {
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

        let scheduled_at: i64 = row.get("scheduled_at");

        // This unwrap is safe because we know that the database returns the date in the correct format
        let scheduled_at: DateTime<Utc> = DateTime::from_timestamp(scheduled_at, 0).unwrap();

        let created_at: i64 = row.get("created_at");

        // This unwrap is safe because we know that the database returns the date in the correct format
        let created_at: DateTime<Utc> = DateTime::from_timestamp(created_at, 0).unwrap();

        let updated_at: i64 = row.get("updated_at");

        // This unwrap is safe because we know that the database returns the date in the correct format
        let updated_at: DateTime<Utc> = DateTime::from_timestamp(updated_at, 0).unwrap();

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

impl FangQueryable<Sqlite> for BackendSqlXSQLite {}

impl BackendSqlXSQLite {
    pub(super) async fn execute_query(
        query: SqlXQuery,
        pool: &Pool<Sqlite>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match query {
            Q::InsertTask => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::insert_task(
                    INSERT_TASK_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::UpdateTaskState => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::update_task_state(
                    UPDATE_TASK_STATE_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FailTask => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::fail_task(
                    FAIL_TASK_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::RemoveAllTask => {
                let affected_rows = <BackendSqlXSQLite as FangQueryable<Sqlite>>::remove_all_task(
                    REMOVE_ALL_TASK_QUERY_SQLITE,
                    pool,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveAllScheduledTask => {
                let affected_rows =
                    <BackendSqlXSQLite as FangQueryable<Sqlite>>::remove_all_scheduled_tasks(
                        REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE,
                        pool,
                    )
                    .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTask => {
                let affected_rows = <BackendSqlXSQLite as FangQueryable<Sqlite>>::remove_task(
                    REMOVE_TASK_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskByMetadata => {
                let affected_rows =
                    <BackendSqlXSQLite as FangQueryable<Sqlite>>::remove_task_by_metadata(
                        REMOVE_TASK_BY_METADATA_QUERY_SQLITE,
                        pool,
                        params,
                    )
                    .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskType => {
                let affected_rows = <BackendSqlXSQLite as FangQueryable<Sqlite>>::remove_task_type(
                    REMOVE_TASKS_TYPE_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::FetchTaskType => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::fetch_task_type(
                    FETCH_TASK_TYPE_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FindTaskById => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::find_task_by_id(
                    FIND_TASK_BY_ID_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::RetryTask => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::retry_task(
                    RETRY_TASK_QUERY_SQLITE,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::InsertTaskIfNotExists => {
                let task = <BackendSqlXSQLite as FangQueryable<Sqlite>>::insert_task_if_not_exists(
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

    pub(super) fn _name() -> &'static str {
        "SQLite"
    }
}
