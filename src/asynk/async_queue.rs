use bb8_postgres::tokio_postgres::Client;

pub struct AsyncQueue {
    pg_client: Client,
}

impl AsyncQueue {
    pub fn new(pg_client: Client) -> Self {
        Self { pg_client }
    }
}
