const INSERT_TASK_QUERY_POSTGRES: &str = include_str!("../queries_postgres/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/update_task_state.sql");
const FAIL_TASK_QUERY_POSTGRES: &str = include_str!("../queries_postgres/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_POSTGRES: &str = include_str!("../queries_postgres/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_POSTGRES: &str =
    include_str!("../queries_postgres/find_task_by_id.sql");
const RETRY_TASK_QUERY_POSTGRES: &str = include_str!("../queries_postgres/retry_task.sql");

#[derive(Debug, Clone)]
pub(super) struct BackendSqlXPg {}

use chrono::DateTime;
use chrono::Utc;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use sqlx::Pool;
use sqlx::Postgres;
use sqlx::Row;
use uuid::Uuid;
use SqlXQuery as Q;

use super::FangQueryable;
use super::{QueryParams, Res, SqlXQuery};
use crate::AsyncQueueError;
use crate::FangTaskState;
use crate::Task;

impl<'a> FromRow<'a, PgRow> for Task {
    fn from_row(row: &'a PgRow) -> Result<Self, sqlx::Error> {
        let id: Uuid = row.get("id");

        // -- SELECT metadata->>'type' FROM fang_tasks ;
        let metadata: serde_json::Value = row.get("metadata");

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

impl FangQueryable<Postgres> for BackendSqlXPg {}

impl BackendSqlXPg {
    pub(super) async fn execute_query(
        query: SqlXQuery,
        pool: &Pool<Postgres>,
        params: QueryParams<'_>,
    ) -> Result<Res, AsyncQueueError> {
        match query {
            Q::InsertTask => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::insert_task(
                    INSERT_TASK_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::UpdateTaskState => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::update_task_state(
                    UPDATE_TASK_STATE_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FailTask => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::fail_task(
                    FAIL_TASK_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::RemoveAllTask => {
                let affected_rows = <BackendSqlXPg as FangQueryable<Postgres>>::remove_all_task(
                    REMOVE_ALL_TASK_QUERY_POSTGRES,
                    pool,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveAllScheduledTask => {
                let affected_rows =
                    <BackendSqlXPg as FangQueryable<Postgres>>::remove_all_scheduled_tasks(
                        REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES,
                        pool,
                    )
                    .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTask => {
                let affected_rows = <BackendSqlXPg as FangQueryable<Postgres>>::remove_task(
                    REMOVE_TASK_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskByMetadata => {
                let affected_rows =
                    <BackendSqlXPg as FangQueryable<Postgres>>::remove_task_by_metadata(
                        REMOVE_TASK_BY_METADATA_QUERY_POSTGRES,
                        pool,
                        params,
                    )
                    .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::RemoveTaskType => {
                let affected_rows = <BackendSqlXPg as FangQueryable<Postgres>>::remove_task_type(
                    REMOVE_TASKS_TYPE_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Bigint(affected_rows))
            }
            Q::FetchTaskType => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::fetch_task_type(
                    FETCH_TASK_TYPE_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::FindTaskById => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::find_task_by_id(
                    FIND_TASK_BY_ID_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;
                Ok(Res::Task(task))
            }
            Q::RetryTask => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::retry_task(
                    RETRY_TASK_QUERY_POSTGRES,
                    pool,
                    params,
                )
                .await?;

                Ok(Res::Task(task))
            }
            Q::InsertTaskIfNotExists => {
                let task = <BackendSqlXPg as FangQueryable<Postgres>>::insert_task_if_not_exists(
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

    pub(super) fn _name() -> &'static str {
        "PostgreSQL"
    }
}
