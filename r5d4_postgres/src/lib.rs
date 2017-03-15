#![warn(missing_docs)]
extern crate r5d4;
extern crate tokio_postgres;
extern crate tokio_core;
extern crate futures;

use futures::Future;
use std::error;
use std::error::Error as _StdError;
use std::fmt;
use tokio_postgres::params::{ConnectParams, IntoConnectParams};
use tokio_postgres::tls::Handshake;
use tokio_core::reactor::Handle;

/// A unified enum of errors returned by postgres::Connection
#[derive(Debug)]
pub enum Error {
    /// A postgres::error::ConnectError
    Connect(tokio_postgres::error::ConnectError),
    /// An postgres::error::Error
    Other(tokio_postgres::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}: {}", self.description(), self.cause().unwrap())
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Connect(_) => "Error opening a connection",
            Error::Other(_) => "Error communicating with server",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Connect(ref err) => Some(err as &error::Error),
            Error::Other(ref err) => Some(err as &error::Error),
        }
    }
}

pub struct PostgresConnectionManager {
    params: ConnectParams,
}

impl PostgresConnectionManager {
    /// Creates a new `PostgresConnectionManager`.
    ///
    /// See `postgres::Connection::connect` for a description of the parameter
    /// types.
    pub fn new<T>(params: T)
                  -> Result<PostgresConnectionManager, tokio_postgres::error::ConnectError>
        where T: IntoConnectParams
    {
        let params = match params.into_connect_params() {
            Ok(params) => params,
            Err(err) => return Err(tokio_postgres::error::ConnectError::ConnectParams(err)),
        };

        Ok(PostgresConnectionManager { params: params })
    }
}

impl r5d4::ManageConnection for PostgresConnectionManager {
    type Connection = tokio_postgres::Connection;
    type Error = Error;

    fn connect(&self, handle: &Handle) -> r5d4::BoxFuture<tokio_postgres::Connection, Error> {
        tokio_postgres::Connection::connect(self.params.clone(),
                                            tokio_postgres::TlsMode::None,
                                            handle)
            .map_err(Error::Connect)
            .boxed()
    }

    fn is_valid(&self,
                conn: tokio_postgres::Connection)
                -> r5d4::BoxFuture<tokio_postgres::Connection, Error> {
        conn.batch_execute("").map_err(Error::Other).boxed()
    }
}
