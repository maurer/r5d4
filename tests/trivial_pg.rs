extern crate r5d4;
extern crate r5d4_postgres;
extern crate tokio_postgres;
extern crate tokio_core;
extern crate futures;
use futures::Future;
use tokio_core::reactor::Core;

use r5d4_postgres::PostgresConnectionManager;
use r5d4::Pool;

fn main() {
    let uri = "/tmp";
    let mut core = Core::new().unwrap();
    let manager = PostgresConnectionManager::new(uri, r5d4_postgres::TlsMode::None).unwrap();
    let pool = Pool::new(r5d4::Config::default(), manager, core.handle());
    let trivial_future = pool.with_connection(|conn| {
        conn.batch_execute("SELECT 1")
            .map(|x| ((), x))
            .map_err(|x| (r5d4_postgres::Error::Other(x), None))
    });
}
