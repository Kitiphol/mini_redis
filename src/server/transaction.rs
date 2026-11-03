//! `MULTI` / `EXEC` / `DISCARD`.
//!
//! Between MULTI and EXEC, commands are checked (name, argument count) and
//! queued, each answered with `+QUEUED`. EXEC then runs the whole queue in
//! ONE critical section of the selected database, so no other client's
//! command can run in between: the transaction has a single linearization
//! point. Blocked BLPOP clients are served after the whole transaction.
//!
//! As in Redis: if any request was rejected while queueing, EXEC refuses to
//! run anything (`EXECABORT`); an error *while running* (say INCR on a
//! non-number) is reported in that command's slot and the others still run
//! (no rollback); BLPOP/BRPOP inside a transaction never block.

use crate::command::{Command, ScriptCall};
use crate::db::{DataCmd, Db, Op};
use crate::error;
use crate::resp::Frame;
use crate::scripting::{self, ScriptCache};
use bytes::Bytes;

/// One queued command.
#[derive(Debug, Clone)]
enum Queued {
    Data(DataCmd),
    Eval(ScriptCall),
}

/// A queued command made ready to run inside the atomic step.
enum Step {
    Data(DataCmd),
    Script(scripting::Resolved),
    /// EVALSHA of a script that isn't cached: its slot gets this error.
    Failed(Frame),
}

#[derive(Debug, Default)]
pub(super) struct Transaction {
    queued: Vec<Queued>,
    failed: bool,
}

impl Transaction {
    /// Queue `cmd`, or reject it (which makes EXEC fail).
    pub fn queue(&mut self, cmd: Command) -> Frame {
        let queued = match cmd {
            Command::Data(c) => Queued::Data(c),
            Command::Ping(msg) => Queued::Data(DataCmd {
                op: Op::Ping,
                args: msg.into_iter().collect::<Vec<Bytes>>(),
            }),
            Command::Eval(call) => Queued::Eval(call),
            other => {
                self.failed = true;
                return error::not_in_multi(other.name());
            }
        };
        self.queued.push(queued);
        Frame::Simple("QUEUED".into())
    }

    pub fn fail(&mut self) {
        self.failed = true;
    }

    /// EXEC: run everything as one atomic step.
    pub fn exec(self, db: &Db, scripts: &ScriptCache) -> Frame {
        if self.failed {
            return error::exec_abort();
        }
        // Look scripts up in the cache now (EVALSHA of an unknown script is
        // an error in its own slot, as in Redis); the step itself only needs
        // the sources.
        let steps: Vec<Step> = self
            .queued
            .into_iter()
            .map(|q| match q {
                Queued::Data(c) => Step::Data(c),
                Queued::Eval(call) => match scripting::resolve(scripts, call) {
                    Ok(s) => Step::Script(s),
                    Err(e) => Step::Failed(e),
                },
            })
            .collect();
        let replies = db.critical(|t| {
            steps
                .iter()
                .map(|step| match step {
                    Step::Data(c) => t.run(c),
                    Step::Script(s) => scripting::run(t, s),
                    Step::Failed(e) => e.clone(),
                })
                .collect()
        });
        Frame::Array(replies)
    }
}
