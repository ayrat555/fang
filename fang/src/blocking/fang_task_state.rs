/// Possible states of the task
#[derive(diesel_derive_enum::DbEnum, Debug, Eq, PartialEq, Clone)]
#[ExistingTypePath = "crate::schema::sql_types::FangTaskState"]
pub enum FangTaskState {
    /// The task is ready to be executed
    New,
    /// The task is being executing.
    ///
    /// The task may stay in this state forever
    /// if an unexpected error happened
    InProgress,
    /// The task failed
    Failed,
    /// The task finished successfully
    Finished,
    /// The task is being retried. It means it failed but it's scheduled to be executed again
    Retried,
}
