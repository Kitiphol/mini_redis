//! Per-connection session state: the selected database and the course rule
//! that SELECT is only allowed at the start of a session, the client's name,
//! and an open `MULTI` transaction.
//!
//! **SELECT is optional.** A client that never sends it uses database 0.
//! When it is sent, it must come first: the session "starts" with the first
//! command that is actually executed. Two kinds of requests don't count:
//!
//! * rejected requests (unknown command, wrong arity, ...) have no effect;
//! * connection-setup commands (`CLIENT SETNAME/SETINFO`, `COMMAND`) only
//!   describe the connection, and client libraries send them while
//!   connecting, sometimes before their SELECT (redis-py does).

use super::transaction::Transaction;
use crate::command::Command;
use crate::error;
use crate::resp::Frame;
use bytes::Bytes;

#[derive(Debug, Default)]
pub(super) struct Session {
    db: usize,
    started: bool,
    name: Option<Bytes>,
    multi: Option<Transaction>,
}

impl Session {
    /// The currently selected database (default 0).
    pub fn db(&self) -> usize {
        self.db
    }

    /// Mark that a command is about to execute. Returns `true` if it is the
    /// first one on this connection (so SELECT is still allowed).
    pub fn begin_command(&mut self) -> bool {
        !std::mem::replace(&mut self.started, true)
    }

    /// `SELECT index`: allowed only if this is the session's first command.
    pub fn select(&mut self, index: usize, first: bool) -> Frame {
        if first {
            self.db = index;
            Frame::ok()
        } else {
            error::select_not_first()
        }
    }

    pub fn set_name(&mut self, name: Bytes) {
        self.name = Some(name);
    }

    pub fn name(&self) -> Option<&Bytes> {
        self.name.as_ref()
    }

    // ---- MULTI -----------------------------------------------------------

    pub fn in_multi(&self) -> bool {
        self.multi.is_some()
    }

    /// `MULTI`
    pub fn start_multi(&mut self) -> Frame {
        if self.multi.is_some() {
            return error::nested_multi();
        }
        self.multi = Some(Transaction::default());
        Frame::ok()
    }

    /// Queue a command inside MULTI; replies `+QUEUED` or an error.
    pub fn queue(&mut self, cmd: Command) -> Frame {
        match self.multi.as_mut() {
            Some(tx) => tx.queue(cmd),
            None => error::exec_without_multi(),
        }
    }

    /// A request inside MULTI was rejected: EXEC will refuse to run.
    pub fn reject_in_multi(&mut self) {
        if let Some(tx) = self.multi.as_mut() {
            tx.fail();
        }
    }

    /// End MULTI (EXEC or DISCARD), returning the transaction.
    pub fn take_multi(&mut self) -> Option<Transaction> {
        self.multi.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_works_only_first() {
        let mut s = Session::default();
        assert_eq!(s.db(), 0);
        let first = s.begin_command();
        assert_eq!(s.select(3, first), Frame::ok());
        assert_eq!(s.db(), 3);
        let first = s.begin_command();
        assert_eq!(s.select(5, first), error::select_not_first());
        assert_eq!(s.db(), 3);
    }

    #[test]
    fn any_executed_command_closes_the_window() {
        let mut s = Session::default();
        assert!(s.begin_command()); // e.g. PING
        assert!(!s.begin_command()); // a later SELECT is refused
    }

    #[test]
    fn multi_state() {
        let mut s = Session::default();
        assert!(!s.in_multi());
        assert_eq!(s.start_multi(), Frame::ok());
        assert_eq!(s.start_multi(), error::nested_multi());
        assert!(s.in_multi());
        assert!(s.take_multi().is_some());
        assert!(!s.in_multi());
    }
}
