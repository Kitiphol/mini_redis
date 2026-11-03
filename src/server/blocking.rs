//! Waiting for a blocked BLPOP/BRPOP to end.
//!
//! A wait ends in exactly one of three ways, and every decision is taken
//! under the database lock, so exactly one of them happens:
//!
//! * **served** — a push handed us a value (already in `rx`);
//! * **timed out** — `cancel` removed us before any push served us;
//! * **client gone** — `cancel` removed us, or, if a push won the race, the
//!   value was already ours and is lost with the client (as in Redis).
//!
//! No lock and no thread is held while waiting.

use super::connection::{Connection, MAX_BUFFERED, Outcome};
use super::reply;
use crate::db::{Cancel, WaitTicket};
use crate::resp::Frame;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use tracing::warn;

impl Connection {
    pub(super) async fn wait(&mut self, ticket: WaitTicket, timeout: Option<Duration>) -> Outcome {
        let WaitTicket { id, mut rx } = ticket;
        let server = self.server.clone();
        let db = server.db(self.session.db());
        let deadline = timeout.and_then(|d| Instant::now().checked_add(d));
        let sleep = async move {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                biased;

                // Served: a push popped a value for us inside its critical section.
                delivered = &mut rx => {
                    return Outcome::Reply(match delivered {
                        Ok(d) => reply::delivery(d),
                        // Sender dropped without sending: cannot happen (only
                        // `cancel` drops it, and only this task cancels).
                        Err(_) => Frame::NullArray,
                    });
                }

                // Timed out.
                () = &mut sleep => {
                    return Outcome::Reply(match db.cancel(id) {
                        // Never served. Linearization point: our registration,
                        // when all keys were empty, so `nil` is correct.
                        Cancel::Cancelled => Frame::NullArray,
                        // A push won the race just before the deadline: the
                        // value is already in our receiver. Return it.
                        Cancel::AlreadyServed => match rx.try_recv() {
                            Ok(d) => reply::delivery(d),
                            Err(_) => Frame::NullArray,
                        },
                    });
                }

                // The client sent more bytes, or went away.
                read = self.stream.read_buf(&mut self.buf) => {
                    // Bytes from a pipelining client stay buffered for later.
                    if matches!(read, Ok(n) if n > 0) && self.buf.len() <= MAX_BUFFERED {
                        continue;
                    }
                    if db.cancel(id) == Cancel::AlreadyServed
                        && let Ok(Ok((key, _))) = rx.try_recv()
                    {
                        server.record_lost_delivery();
                        warn!(
                            key = %key.escape_ascii(),
                            "blocked client disconnected after being served; value dropped"
                        );
                    }
                    return Outcome::Disconnected;
                }
            }
        }
    }
}
