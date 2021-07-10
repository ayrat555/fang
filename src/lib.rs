macro_rules! reexport_proc_macro {
    ($crate_name:ident) => {
        #[doc(hidden)]
        pub use self::$crate_name::*;
        #[doc(hidden)]
        pub use $crate_name;
    };
}

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate log;

mod schema;

pub mod executor;
pub mod postgres;
pub mod worker_pool;

pub use executor::*;
pub use postgres::*;
pub use worker_pool::*;

reexport_proc_macro!(typetag);

#[doc(hidden)]
pub use typetag::serde as fang_typetag;

#[doc(hidden)]
pub use serde::{Deserialize, Serialize};
