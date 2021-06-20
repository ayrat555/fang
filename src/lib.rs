#[macro_use]
extern crate diesel;

// pub trait JobQueue {
//     type Job;
//     type Error;

//     fn push(job: Self::Job) -> Result<(), Self::Error> {

//     }
//     fn pop(job: Self::Job) -> Result<(), Self::Error>;
// }

// pub trait Storage {
//     fn save() ->
// }

pub mod executor;
pub mod postgres;
pub mod scheduler;
mod schema;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
