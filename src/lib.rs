//! micro-redis — a small, linearizable, multithreaded Redis-compatible server.
//!
//! A request flows through the modules in this order:
//!
//! ```text
//!  bytes ──► resp ──► command ──► server ──► db
//!  (TCP)    (frame)   (validated)  (dispatch) (one locked critical section)
//!                                     │
//!  bytes ◄── resp::encode ◄───────────┘ reply, sent after the lock is released
//! ```
//!
//! * [`resp`]    — RESP2 parser and encoder (written from scratch)
//! * [`command`] — request validation into a typed [`command::Command`]
//! * [`server`]  — accept loop, connections, dispatch, blocking waits, SELECT rule
//! * [`db`]      — one database: keyspace, every data command, blocked clients
//! * [`scripting`] — Lua scripts (EVAL, EVALSHA, SCRIPT)
//! * [`error`]   — every error reply text in one place
//! * [`config`]  — command-line flags for the server binary
//! * [`client`]  — a small async RESP2 client (tests and `micro-redis-cli`)

pub mod client;
pub mod command;
pub mod config;
pub mod db;
pub mod error;
pub mod resp;
pub mod scripting;
pub mod server;

pub use server::{Server, serve};

/// Bind `addr` and serve until the process exits.
pub async fn run(addr: &str) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve(listener, Server::new()).await
}
