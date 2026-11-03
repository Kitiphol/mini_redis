//! The TCP server.
//!
//! * `mod.rs`         — [`Server`] (the 16 shared databases) and the accept loop
//! * [`connection`]   — one task per client: read, parse, dispatch, reply
//! * [`dispatch`]     — run one command against the selected database
//! * [`blocking`]     — wait for a blocked BLPOP/BRPOP to be served
//! * [`session`]      — per-connection state: selected db, SELECT rule, name
//! * [`transaction`]  — MULTI/EXEC: queueing and running a transaction
//! * [`reply`]        — building reply frames from results

mod blocking;
mod connection;
mod dispatch;
mod reply;
mod session;
mod transaction;

use crate::command::NUM_DBS;
use crate::db::{self, Db};
use crate::scripting::ScriptCache;
use connection::Connection;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{Instrument, debug, info, info_span, warn};

/// Shared state of the whole service: 16 independent databases and the
/// Lua script cache.
pub struct Server {
    dbs: Vec<Db>,
    scripts: ScriptCache,
    lost_deliveries: AtomicU64,
    next_conn_id: AtomicU64,
}

impl Server {
    /// A server with 16 empty databases, each behind its own mutex.
    pub fn new() -> Arc<Server> {
        Arc::new(Server {
            dbs: (0..NUM_DBS).map(|_| Db::new()).collect(),
            scripts: ScriptCache::default(),
            lost_deliveries: AtomicU64::new(0),
            next_conn_id: AtomicU64::new(1),
        })
    }

    /// Database `i` (0..16).
    pub fn db(&self, i: usize) -> &Db {
        &self.dbs[i]
    }

    /// The Lua script cache (shared by all databases, as in Redis).
    pub fn scripts(&self) -> &ScriptCache {
        &self.scripts
    }

    /// `FLUSHALL`: empty every database in one atomic step.
    pub fn flush_all(&self) {
        db::flush_all(&self.dbs);
    }

    /// Values that were handed to a blocked client which disconnected before
    /// the reply could be written (they are gone, exactly as in Redis).
    pub fn lost_deliveries(&self) -> u64 {
        self.lost_deliveries.load(Ordering::SeqCst)
    }

    fn record_lost_delivery(&self) {
        self.lost_deliveries.fetch_add(1, Ordering::SeqCst);
    }

    /// Total clients currently blocked in BLPOP/BRPOP, across all databases.
    pub fn total_waiters(&self) -> usize {
        self.dbs.iter().map(Db::waiter_count).sum()
    }

    /// Check every database's invariants (used by tests).
    pub fn check_invariants(&self) -> Result<(), String> {
        for (i, db) in self.dbs.iter().enumerate() {
            db.check_invariants().map_err(|e| format!("db {i}: {e}"))?;
        }
        Ok(())
    }
}

/// Accept connections forever, one tokio task per connection.
pub async fn serve(listener: TcpListener, server: Arc<Server>) -> io::Result<()> {
    info!(addr = %listener.local_addr()?, "micro-redis listening");
    tokio::spawn(expire_keys(Arc::downgrade(&server)));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                // e.g. too many open files: keep serving existing clients.
                warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let server = Arc::clone(&server);
        let conn_id = server.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let span = info_span!("conn", id = conn_id, %peer);
        tokio::spawn(
            async move {
                debug!("connected");
                let _ = stream.set_nodelay(true);
                match Connection::new(stream, server, conn_id).run().await {
                    Ok(()) => debug!("disconnected"),
                    Err(e) => debug!(error = %e, "connection ended with error"),
                }
            }
            .instrument(span),
        );
    }
}

/// Background "active expiry": every 100 ms, delete up to 200 expired keys per
/// database, so keys with a TTL that nobody reads again don't stay in memory.
/// (Reads already ignore expired keys; this only frees memory.) Stops when
/// the server is dropped.
async fn expire_keys(server: std::sync::Weak<Server>) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tick.tick().await;
        let Some(server) = server.upgrade() else {
            return;
        };
        for db in &server.dbs {
            db.sweep_expired(200);
        }
    }
}
