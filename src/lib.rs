extern crate log;
extern crate tokio_core;
extern crate futures;

use std::collections::vec_deque::VecDeque;
use futures::{Future, Async, Poll, done};
use futures::task::{Task, park};

use tokio_core::reactor::Handle;

use std::rc::Rc;
use std::cell::RefCell;

// We're not using futures::BoxFuture here because I don't want to mandate Send
// Since both r5d4::BoxFuture and futures::BoxFuture are type aliases, futures::BoxFuture should be
// compatible with us, while not requiring Send when unnecessary.
pub type BoxFuture<T, E> = Box<Future<Item = T, Error = E>>;

pub struct Config {
    pool_size: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config { pool_size: 10 }
    }
}

pub trait ManageConnection: 'static {
    type Connection;
    type Error;

    fn connect(&self, &Handle) -> BoxFuture<Self::Connection, Self::Error>;
    fn is_valid(&self, conn: Self::Connection) -> BoxFuture<Self::Connection, Self::Error>;
}

pub struct InnerPool<M: ManageConnection> {
    config: Config,
    manager: M,
    conns: VecDeque<M::Connection>,
    blocked: VecDeque<Task>,
    lent: usize,
    handle: Handle,
}

pub struct Pool<M: ManageConnection>(Rc<RefCell<InnerPool<M>>>);

impl<M: ManageConnection> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}

impl<M: ManageConnection> Pool<M> {
    fn pop_conn(&self) -> Option<M::Connection> {
        self.0.borrow_mut().conns.pop_front()
    }
    fn live_conns(&self) -> usize {
        self.0.borrow().conns.len() + self.0.borrow().lent
    }
    fn pool_size(&self) -> usize {
        self.0.borrow().config.pool_size
    }
    fn block(&self, task: Task) {
        self.0.borrow_mut().blocked.push_back(task);
    }
    fn return_conn(&self, conn: M::Connection) {
        self.0.borrow_mut().conns.push_back(conn);
        self.0.borrow_mut().lent -= 1;
        // We've gotten a connection back, so someone blocked on us may want to wake up
        match self.0.borrow_mut().blocked.pop_front() {
            Some(task) => task.unpark(),
            None => (),
        }
    }
    fn connect(&self) -> BoxFuture<M::Connection, M::Error> {
        let handle = self.0.borrow().handle.clone();
        let conn_fut = self.0.borrow().manager.connect(&handle);
        let pool = self.clone();
        Box::new(conn_fut.and_then(move |conn| {
            pool.0.borrow_mut().lent += 1;
            done(Ok(conn))
        }))
    }
    fn get(&self) -> ConnFuture<M> {
        ConnFuture { pool: (*self).clone() }
    }
    // We lost a connection
    fn apologize(&self) {
        self.0.borrow_mut().lent -= 1;
    }
    pub fn with_connection<F, U, T, E>(&self, f: F) -> BoxFuture<T, E>
        where F: FnOnce(M::Connection) -> U + 'static,
              U: Future<Item = (T, M::Connection), Error = (E, Option<M::Connection>)> + 'static,
              E: From<M::Error> + 'static,
              T: 'static
    {
        let pool: Pool<M> = self.clone();
        Box::new(self.get().map_err(|e| E::from(e)).and_then(move |conn| {
            f(conn).then(move |res| match res {
                Ok((v, c)) => {
                    pool.return_conn(c);
                    done(Ok(v))
                }
                Err((e, mc)) => {
                    match mc {
                        Some(c) => pool.return_conn(c),
                        None => pool.apologize(),
                    }
                    done(Err(e))
                }
            })
        }))

    }
    pub fn new(config: Config, manager: M, handle: Handle) -> Pool<M> {
        Pool(Rc::new(RefCell::new(InnerPool {
            config: config,
            manager: manager,
            conns: VecDeque::new(),
            blocked: VecDeque::new(),
            lent: 0,
            handle: handle,
        })))
    }
}

struct ConnFuture<M: ManageConnection> {
    pool: Pool<M>,
}

impl<M: ManageConnection> Future for ConnFuture<M> {
    type Item = M::Connection;
    type Error = M::Error;
    fn poll(&mut self) -> Poll<M::Connection, M::Error> {
        match self.pool.pop_conn() {
            Some(conn) => Ok(Async::Ready(conn)),
            None => {
                if self.pool.live_conns() >= self.pool.pool_size() {
                    // We're over our limit and need to block
                    self.pool.block(park());
                    Ok(Async::NotReady)
                } else {
                    // TODO unsure of legitimacy here
                    self.pool.connect().poll()
                }
            }
        }
    }
}
