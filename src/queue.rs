pub trait JobQueue {
    type Job;
    type Error;

    fn push(job: Self::Job) -> Result<(), Self::Error>;
    fn pop(job: Self::Job) -> Result<(), Self::Error>;
}
