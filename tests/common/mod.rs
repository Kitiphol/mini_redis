//! Shared helpers for integration tests.
#![allow(dead_code)]

use micro_redis::Server;
use micro_redis::client::{Client, ClientError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};

/// Start a fresh server on a random free port, inside this test's runtime.
pub async fn start() -> (SocketAddr, Arc<Server>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = Server::new();
    let s = Arc::clone(&server);
    tokio::spawn(async move {
        let _ = micro_redis::serve(listener, s).await;
    });
    (addr, server)
}

/// How long a test keeps retrying a connection the OS turned away.
const CONNECT_PATIENCE: Duration = Duration::from_secs(60);

/// Errors that mean "the operating system turned this connection away, try
/// again", not "the server is broken":
/// * reset / refused / aborted: the queue of not-yet-accepted connections
///   overflowed. macOS caps it at 128 (`kern.ipc.somaxconn`). It may accept
///   the TCP handshake and reset the connection only at the first command.
/// * address not available: the OS ran out of local ports for new
///   connections (see [`raw`]).
fn turned_away(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionReset
            | ConnectionRefused
            | ConnectionAborted
            | BrokenPipe
            | UnexpectedEof
            | AddrNotAvailable
            | AddrInUse
    )
}

/// Open a plain TCP connection to the test server, retrying while the OS
/// turns it away.
///
/// The socket is set to close with a reset instead of the usual goodbye.
/// Normally the side that closes first keeps the local port reserved for
/// about 30 s (macOS) to 60 s (Linux) in `TIME_WAIT`. The stress tests open
/// hundreds of thousands of connections, which would use up all ~16,000 of
/// macOS's local ports and make every later connection fail with "Can't
/// assign requested address". The server treats a reset like any other
/// disconnect.
pub async fn raw(addr: SocketAddr) -> TcpStream {
    let start = Instant::now();
    loop {
        match TcpStream::connect(addr).await {
            Ok(s) => {
                // macOS rejects this with "Invalid argument" if the OS has
                // already reset the connection (accept queue overflow), so a
                // failure here means "turned away" too: drop it and retry.
                if socket2::SockRef::from(&s)
                    .set_linger(Some(Duration::ZERO))
                    .is_ok()
                {
                    return s;
                }
                if start.elapsed() >= CONNECT_PATIENCE {
                    panic!("connect: every attempt was reset for {CONNECT_PATIENCE:?}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) if turned_away(&e) && start.elapsed() < CONNECT_PATIENCE => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("connect: {e:?}"),
        }
    }
}

/// Connect a client, retrying like a real client library would, and make
/// sure the server really accepted it: one round trip (`CLIENT GETNAME`,
/// which doesn't count as a command for the SELECT rule) before returning.
pub async fn client(addr: SocketAddr) -> Client {
    let start = Instant::now();
    loop {
        let mut c = Client::from_stream(raw(addr).await).expect("client");
        match c.cmd(&["CLIENT", "GETNAME"]).await {
            Ok(_) => return c,
            Err(ClientError::Io(e)) if turned_away(&e) && start.elapsed() < CONNECT_PATIENCE => {}
            Err(ClientError::Closed) if start.elapsed() < CONNECT_PATIENCE => {}
            Err(e) => panic!("connect: {e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Poll until `cond` holds (or panic after 5 s). Used to make sure a client
/// is really blocked before the test continues.
pub async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Small deterministic PRNG (xorshift64*), so tests need no extra crates.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

/// Number of iterations for heavy tests; override with STRESS_ITERS.
pub fn iters(default: usize) -> usize {
    std::env::var("STRESS_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
