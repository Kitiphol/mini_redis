//! One database (namespace): the keyspace plus the clients blocked in
//! BLPOP/BRPOP.
//!
//! * `mod.rs`    — [`Db`]: the public API, and the *only* place the lock is taken
//! * [`ops`]     — every data command (the command table and its executors)
//! * [`state`]   — the data itself, expiry, and serving blocked clients
//! * [`waiters`] — the registry of blocked clients (FIFO per key)
//! * [`value`]   — stored value types and list helpers
//! * [`glob`]    — pattern matching for `KEYS`
//!
//! # The linearizability argument in one paragraph
//!
//! Everything about one database lives behind **one** `Mutex`. Every command
//! — and every `MULTI`/`EXEC` transaction and every Lua script, as a whole —
//! does all of its reads and writes inside a single critical section
//! ([`Db::critical`]), and the connection only replies *after* that section
//! has ended. So each one has a linearization point (LP) — any instant while
//! it holds the lock — that lies between request and response, and no other
//! command can observe an intermediate state. The blocking commands keep this
//! property because pushes *hand values to waiters inside the pushing
//! critical section* (`State::serve_ready`): the waiter's LP is placed
//! immediately after the push's LP, with nothing in between. See README.md.
//!
//! This module knows nothing about sockets, so all of it is unit-testable.

pub mod glob;
pub mod ops;
mod state;
mod value;
mod waiters;

#[cfg(test)]
mod tests;

pub use ops::{DataCmd, Op};
pub use value::End;

use crate::resp::Frame;
use bytes::Bytes;
use state::State;
use std::sync::{Mutex, MutexGuard};
use tokio::sync::oneshot;

/// The data error the blocking path needs: a list command on another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DbError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}

/// `(key, value)` returned by a successful pop.
pub type Popped = (Bytes, Bytes);

/// What a blocked client eventually receives.
pub type Delivery = Result<Popped, DbError>;

/// Result of BLPOP/BRPOP's first, non-blocking attempt.
pub enum PopOutcome {
    /// Some key had data: popped immediately, nothing to wait for.
    Ready(Popped),
    /// All keys were empty: the client is now registered as a waiter.
    Blocked(WaitTicket),
}

/// Handle of a registered waiter. `rx` receives exactly one delivery,
/// unless the waiter is cancelled first.
pub struct WaitTicket {
    pub id: u64,
    pub rx: oneshot::Receiver<Delivery>,
}

/// Result of trying to stop waiting (timeout or disconnect).
#[derive(Debug, PartialEq, Eq)]
pub enum Cancel {
    /// The waiter was still registered and is now removed: it was never
    /// served, so nothing was popped on its behalf.
    Cancelled,
    /// A push already served it; the delivery is sitting in its receiver.
    AlreadyServed,
}

/// Access to a locked database during one critical section. Data commands
/// run through [`Txn::run`]; a transaction or script calls it many times.
pub struct Txn<'a> {
    state: &'a mut State,
}

impl Txn<'_> {
    /// Execute one data command.
    pub fn run(&mut self, cmd: &DataCmd) -> Frame {
        ops::execute(self.state, cmd)
    }
}

/// One database. Every public method is exactly one critical section.
#[derive(Default)]
pub struct Db {
    state: Mutex<State>,
}

impl Db {
    pub fn new() -> Db {
        Db::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A poisoned mutex means a critical section panicked half-way. The
        // build aborts on panic, so this is unreachable; the spec's rule is
        // "if the service crashes, it stops and gives up".
        let mut st = self
            .state
            .lock()
            .expect("database mutex poisoned: a critical section panicked");
        st.begin();
        st
    }

    /// Run `f` as ONE critical section, then serve clients blocked on any
    /// list that received elements. Single commands, `MULTI`/`EXEC` and Lua
    /// scripts all go through here, which is what makes each of them atomic.
    pub fn critical<R>(&self, f: impl FnOnce(&mut Txn<'_>) -> R) -> R {
        let mut st = self.lock();
        let result = f(&mut Txn { state: &mut st });

        #[cfg(not(feature = "bug-split-serve"))]
        st.serve_ready();

        #[cfg(feature = "bug-split-serve")]
        {
            // PLANTED BUG (tests must catch it): release the lock between the
            // push and serving. A newcomer's BLPOP can now take the element
            // ahead of a client that has been waiting longer.
            drop(st);
            std::thread::sleep(std::time::Duration::from_millis(2));
            self.lock().serve_ready();
        }
        result
    }

    /// Execute one data command in its own critical section.
    pub fn execute(&self, cmd: &DataCmd) -> Frame {
        self.critical(|t| t.run(cmd))
    }

    /// `GET key`
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DbError> {
        self.lock().string(key).map(|v| v.cloned())
    }

    /// `SET key value` — overwrites a value of any type and clears its TTL.
    pub fn set(&self, key: Bytes, value: Bytes) {
        self.lock().insert(key, value::Value::Str(value));
    }

    /// `EXISTS key [key ...]` — a key named twice counts twice.
    pub fn exists(&self, keys: &[Bytes]) -> i64 {
        let mut st = self.lock();
        keys.iter().filter(|k| st.contains(k)).count() as i64
    }

    /// `LPUSH`/`RPUSH key value [value ...]`. Returns the list length right
    /// after the push and *before* blocked clients are served (as Redis does).
    pub fn push(&self, key: Bytes, values: Vec<Bytes>, end: End) -> Result<usize, DbError> {
        self.critical(|t| t.state.push(&key, values, end))
    }

    /// First phase of `BLPOP`/`BRPOP`. In one critical section: scan keys left
    /// to right and pop from the first non-empty list; if all are empty,
    /// register as a waiter on every key.
    pub fn pop_or_register(&self, keys: &[Bytes], end: End) -> Result<PopOutcome, DbError> {
        let mut st = self.lock();
        if let Some(popped) = st.try_pop(keys, end)? {
            return Ok(PopOutcome::Ready(popped));
        }

        #[cfg(feature = "bug-split-register")]
        {
            // PLANTED BUG (tests must catch it): check and register in two
            // separate critical sections. A push in the gap is missed, and the
            // client sleeps on a non-empty list ("lost wake-up").
            drop(st);
            std::thread::sleep(std::time::Duration::from_millis(2));
            st = self.lock();
        }

        Ok(PopOutcome::Blocked(st.register(keys, end)))
    }

    /// Stop waiting. Decided under the lock, so it races safely with a push:
    /// exactly one of "served" and "cancelled" happens.
    pub fn cancel(&self, id: u64) -> Cancel {
        self.critical(|t| {
            if t.state.cancel(id) {
                Cancel::Cancelled
            } else {
                Cancel::AlreadyServed
            }
        })
    }

    /// Number of clients currently blocked on this database.
    pub fn waiter_count(&self) -> usize {
        self.lock().waiter_count()
    }

    /// Delete up to `max` expired keys (background active expiry).
    pub fn sweep_expired(&self, max: usize) -> usize {
        self.lock().sweep_expired(max)
    }

    /// Keys physically stored, including expired ones not yet removed
    /// (used by tests to watch the background sweeper).
    pub fn stored_keys(&self) -> usize {
        self.lock().stored_keys()
    }

    /// Check the structural invariants (used by tests).
    pub fn check_invariants(&self) -> Result<(), String> {
        self.lock().check_invariants()
    }
}

/// `FLUSHALL`: empty every database in ONE step. The locks are taken in
/// index order (the only place more than one is held, so no deadlock), all
/// databases are cleared, then all locks are released.
pub fn flush_all(dbs: &[Db]) {
    let mut guards: Vec<MutexGuard<'_, State>> = dbs.iter().map(Db::lock).collect();
    for st in &mut guards {
        st.flush();
    }
}
