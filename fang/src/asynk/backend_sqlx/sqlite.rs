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

use super::{QueryParams, Res, SqlXQuery};
use crate::AsyncQueueError;
use sqlx::{Any, Pool};
use SqlXQuery as Q;

use super::general_any_impl_fail_task;
use super::general_any_impl_fetch_task_type;
use super::general_any_impl_find_task_by_id;
use super::general_any_impl_insert_task;
use super::general_any_impl_insert_task_if_not_exists;
use super::general_any_impl_remove_all_scheduled_tasks;
use super::general_any_impl_remove_all_task;
use super::general_any_impl_remove_task;
use super::general_any_impl_remove_task_by_metadata;
use super::general_any_impl_remove_task_type;
use super::general_any_impl_retry_task;
use super::general_any_impl_update_task_state;

impl BackendSqlXSQLite {
    pub(super) async fn execute_query(
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

    pub(super) fn _name() -> &'static str {
        "SQLite"
    }
}
