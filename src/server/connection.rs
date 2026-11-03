//! One task per client connection: read bytes, parse a request, dispatch it,
//! reply — strictly in order, so pipelined requests also work.

use super::Server;
use super::session::Session;
use crate::command::{self, Command, Parsed};
use crate::db::{DataCmd, Op};
use crate::error;
use crate::resp::{self, Frame};
use bytes::{Buf, BytesMut};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// Bytes a client may send while it is blocked (or before a frame is
/// complete) before we give up on it. Protects memory from a bad client.
pub(super) const MAX_BUFFERED: usize = 64 * 1024 * 1024;

pub(super) struct Connection {
    pub(super) stream: TcpStream,
    pub(super) server: Arc<Server>,
    /// Bytes read from the socket but not yet parsed into a request.
    pub(super) buf: BytesMut,
    /// Reused buffer for encoding replies.
    out: BytesMut,
    pub(super) session: Session,
    /// Connection number (for `CLIENT ID` and logs).
    pub(super) id: u64,
}

/// What executing a command produced.
pub(super) enum Outcome {
    Reply(Frame),
    /// The reply was already written (only used by a planted-bug build).
    #[allow(dead_code)]
    AlreadySent,
    /// The client went away while blocked; stop serving it.
    Disconnected,
    /// Send this reply, then close the connection (QUIT).
    Close(Frame),
}

impl Connection {
    pub(super) fn new(stream: TcpStream, server: Arc<Server>, id: u64) -> Connection {
        Connection {
            stream,
            server,
            buf: BytesMut::with_capacity(4096),
            out: BytesMut::with_capacity(4096),
            session: Session::default(),
            id,
        }
    }

    pub(super) async fn run(mut self) -> io::Result<()> {
        // A request already taken out of the buffer that ended a batch; it is
        // handled next.
        let mut pending: Option<Next> = None;
        loop {
            // 1. Get the next request, reading more bytes only when the buffer
            //    holds no complete request. Before waiting for the client,
            //    send every reply queued so far, in one write.
            let next = match pending.take() {
                Some(n) => n,
                None => self.next_request(),
            };
            let parsed = match next {
                Next::Request(p) => p,
                Next::NeedMore => {
                    self.flush().await?;
                    if self.buf.len() > MAX_BUFFERED {
                        self.queue_reply(&error::request_too_large());
                        return self.flush().await;
                    }
                    if self.stream.read_buf(&mut self.buf).await? == 0 {
                        return Ok(()); // client closed the connection
                    }
                    continue;
                }
                Next::Broken(err) => {
                    // The stream is out of sync: we cannot find the next frame
                    // boundary safely. Reply, then close (like Redis).
                    self.queue_reply(&err);
                    return self.flush().await;
                }
            };

            // 2. A run of plain data commands that are already in the buffer
            //    (a pipelining client) is executed as ONE atomic step: one lock
            //    hold, or one job for the database task.
            if let Parsed::Command(Command::Data(c)) = &parsed
                && self.batchable(c)
            {
                let Parsed::Command(Command::Data(first)) = parsed else {
                    unreachable!("checked just above")
                };
                let mut batch = vec![first];
                while batch.len() < MAX_BATCH {
                    match self.next_request() {
                        Next::Request(Parsed::Command(Command::Data(c))) if self.batchable(&c) => {
                            batch.push(c)
                        }
                        Next::NeedMore => break,
                        other => {
                            pending = Some(other);
                            break;
                        }
                    }
                }
                self.run_batch(batch);
                continue;
            }

            // 3. Everything else, one at a time.
            let outcome = match parsed {
                Parsed::Empty => continue,
                Parsed::Error(err) => {
                    // Rejected requests have no effect and do not start the
                    // session (see `session.rs`). Inside MULTI, they make the
                    // whole transaction fail at EXEC, as in Redis.
                    debug!(reply = ?err, "rejected request");
                    self.session.reject_in_multi();
                    Outcome::Reply(err)
                }
                Parsed::Command(cmd) if self.session.in_multi() && !cmd.controls_multi() => {
                    debug!(cmd = cmd.name(), "queued");
                    Outcome::Reply(self.session.queue(cmd))
                }
                Parsed::Command(cmd) => {
                    debug!(cmd = cmd.name(), db = self.session.db(), "command");
                    self.execute(cmd).await?
                }
            };

            // 4. Queue the reply. It is always produced after the command's
            //    atomic step ended, and sent at the next flush.
            match outcome {
                Outcome::Reply(frame) => self.queue_reply(&frame),
                Outcome::AlreadySent => {}
                Outcome::Disconnected => return Ok(()),
                Outcome::Close(frame) => {
                    self.queue_reply(&frame);
                    return self.flush().await;
                }
            }
        }
    }

    /// Take the next complete request out of the buffer, without reading.
    fn next_request(&mut self) -> Next {
        match resp::parse_request(&self.buf) {
            Ok(Some((frame, n))) => {
                self.buf.advance(n);
                Next::Request(command::parse(frame))
            }
            Ok(None) => Next::NeedMore,
            Err(e) => Next::Broken(error::protocol(e)),
        }
    }

    /// May this command join a batch? Plain data commands outside MULTI;
    /// not the blocking pops (they may wait).
    fn batchable(&self, c: &DataCmd) -> bool {
        // The planted bug replies before applying SET; keep it on its own path.
        if cfg!(feature = "bug-reply-before-apply") {
            return false;
        }
        !self.session.in_multi() && !matches!(c.op, Op::BLPop | Op::BRPop)
    }

    /// Run several data commands as one atomic step and queue their replies.
    ///
    /// Linearizable: every command in the batch was already received (its
    /// request has started) and none has been answered yet, so placing all of
    /// them, in order, inside one step lies within every one's interval.
    fn run_batch(&mut self, batch: Vec<DataCmd>) {
        for _ in &batch {
            self.session.begin_command(); // any executed command ends the SELECT window
        }
        debug!(commands = batch.len(), db = self.session.db(), "batch");
        let server = self.server.clone();
        let db = server.db(self.session.db());
        let replies: Vec<Frame> = db.critical(|t| batch.iter().map(|c| t.run(c)).collect());
        for r in &replies {
            self.queue_reply(r);
        }
    }

    /// Encode a reply into the output buffer (sent at the next flush).
    pub(super) fn queue_reply(&mut self, frame: &Frame) {
        resp::encode(frame, &mut self.out);
    }

    /// Send every queued reply in one write.
    pub(super) async fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            self.stream.write_all(&self.out).await?;
            self.out.clear();
        }
        Ok(())
    }
}

/// What the input buffer holds next.
enum Next {
    Request(Parsed),
    /// No complete request yet: read more bytes.
    NeedMore,
    /// Malformed bytes: reply with this error and close.
    Broken(Frame),
}

/// At most this many commands per batch, so one pipelining client can't hold
/// a database for long.
const MAX_BATCH: usize = 512;
