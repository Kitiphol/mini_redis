//! Running one command for a connection.
//!
//! Every data command is one `Db` call (one critical section); a transaction
//! or a script is also one critical section. The reply is written by the
//! caller, after the lock has been released.

use super::connection::{Connection, Outcome};
use super::reply;
use crate::command::{ClientCmd, Command, args};
use crate::db::{DataCmd, End, Op, PopOutcome};
use crate::error;
use crate::resp::Frame;
use crate::scripting;
use std::io;

impl Connection {
    pub(super) async fn execute(&mut self, cmd: Command) -> io::Result<Outcome> {
        // Connection-setup commands don't start the session (see session.rs).
        let first = !cmd.is_connection_setup() && self.session.begin_command();
        let server = self.server.clone();
        let db = server.db(self.session.db());

        let frame = match cmd {
            // PING touches no data: exempt from linearizability.
            Command::Ping(None) => Frame::Simple("PONG".into()),
            Command::Ping(Some(msg)) => Frame::Bulk(msg),

            Command::Select(index) => self.session.select(index, first),

            Command::Data(c) if matches!(c.op, Op::BLPop | Op::BRPop) => {
                return Ok(self.blocking_pop(c).await);
            }

            #[cfg(feature = "bug-reply-before-apply")]
            Command::Data(c) if c.op == Op::Set => {
                // PLANTED BUG (tests must catch it): acknowledge first, apply
                // later. A client can see OK, and a GET issued afterwards by
                // anyone can still miss the value: not linearizable.
                self.queue_reply(&Frame::ok());
                self.flush().await?;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                db.execute(&c);
                return Ok(Outcome::AlreadySent);
            }

            Command::Data(c) => db.execute(&c),

            Command::Multi => self.session.start_multi(),
            Command::Exec => match self.session.take_multi() {
                Some(tx) => tx.exec(db, server.scripts()),
                None => error::exec_without_multi(),
            },
            Command::Discard => match self.session.take_multi() {
                Some(_) => Frame::ok(),
                None => error::discard_without_multi(),
            },

            Command::Eval(call) => scripting::eval(db, server.scripts(), call),
            Command::ScriptLoad(src) => Frame::Bulk(server.scripts().load(&src).into()),
            Command::ScriptExists(shas) => Frame::Array(
                shas.iter()
                    .map(|s| Frame::Integer(server.scripts().exists(s) as i64))
                    .collect(),
            ),
            Command::ScriptFlush => {
                server.scripts().flush();
                Frame::ok()
            }

            Command::FlushAll => {
                server.flush_all();
                Frame::ok()
            }

            Command::Client(ClientCmd::SetName(name)) => {
                self.session.set_name(name);
                Frame::ok()
            }
            Command::Client(ClientCmd::GetName) => match self.session.name() {
                Some(n) => Frame::Bulk(n.clone()),
                None => Frame::Null,
            },
            Command::Client(ClientCmd::SetInfo) => Frame::ok(),
            Command::Client(ClientCmd::Id) => Frame::Integer(self.id as i64),
            // Introspection isn't implemented; an empty answer keeps
            // redis-cli happy (it only loses its typing hints).
            Command::CommandInfo => Frame::Array(vec![]),
            Command::Quit => return Ok(Outcome::Close(Frame::ok())),
        };
        Ok(Outcome::Reply(frame))
    }

    /// BLPOP/BRPOP outside a transaction: pop now, or block until served,
    /// timed out, or the client leaves.
    async fn blocking_pop(&mut self, c: DataCmd) -> Outcome {
        let end = if c.op == Op::BLPop {
            End::Left
        } else {
            End::Right
        };
        let (keys, raw_timeout) = c.args.split_at(c.args.len() - 1);
        // Already validated when the command was parsed.
        let timeout = match args::timeout(&raw_timeout[0]) {
            Ok(t) => t,
            Err(e) => return Outcome::Reply(e),
        };
        let server = self.server.clone();
        let db = server.db(self.session.db());
        match db.pop_or_register(keys, end) {
            Err(e) => Outcome::Reply(error::db(e)),
            Ok(PopOutcome::Ready(popped)) => Outcome::Reply(reply::popped(popped)),
            Ok(PopOutcome::Blocked(ticket)) => {
                // Replies to earlier pipelined commands must reach the client
                // before we start waiting, possibly for a long time.
                if self.flush().await.is_err() {
                    db.cancel(ticket.id);
                    return Outcome::Disconnected;
                }
                self.wait(ticket, timeout).await
            }
        }
    }
}
