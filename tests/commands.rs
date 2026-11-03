//! Behaviour of each command, through real TCP connections.

mod common;

use bytes::Bytes;
use micro_redis::resp::Frame;
use std::time::{Duration, Instant};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

#[tokio::test]
async fn set_get_overwrite() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    assert_eq!(c.get("k").await.unwrap(), None);
    c.set("k", "1").await.unwrap();
    c.set("k", "2").await.unwrap();
    assert_eq!(c.get("k").await.unwrap(), Some(b("2")));
}

#[tokio::test]
async fn set_replaces_a_list() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    c.rpush("k", &["a", "b"]).await.unwrap();
    c.set("k", "str").await.unwrap();
    assert_eq!(c.get("k").await.unwrap(), Some(b("str")));
    // It is a string now, so list commands on it are type errors.
    assert!(matches!(
        c.cmd(&["RPUSH", "k", "x"]).await.unwrap(),
        Frame::Error(e) if e.starts_with("WRONGTYPE")
    ));
}

#[tokio::test]
async fn exists_counts_repeats() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    c.set("a", "1").await.unwrap();
    c.rpush("l", &["x"]).await.unwrap();
    assert_eq!(c.exists(&["a", "a", "l", "nope"]).await.unwrap(), 3);
}

#[tokio::test]
async fn ping_with_message() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    assert_eq!(
        c.cmd(&["PING", "hello"]).await.unwrap(),
        Frame::Bulk(b("hello"))
    );
}

#[tokio::test]
async fn push_returns_length_and_lpush_reverses() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    assert_eq!(c.lpush("l", &["a", "b", "c"]).await.unwrap(), 3);
    assert_eq!(c.rpush("l", &["d"]).await.unwrap(), 4);
    // list: c b a d
    let mut got = vec![];
    while let Some((_, v)) = c.blpop(&["l"], 0.01).await.unwrap() {
        got.push(v);
    }
    assert_eq!(got, vec![b("c"), b("b"), b("a"), b("d")]);
    assert_eq!(
        c.exists(&["l"]).await.unwrap(),
        0,
        "emptied list is deleted"
    );
}

#[tokio::test]
async fn brpop_pops_from_the_tail() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    c.rpush("l", &["a", "b", "c"]).await.unwrap();
    assert_eq!(c.brpop(&["l"], 0.0).await.unwrap(), Some((b("l"), b("c"))));
}

#[tokio::test]
async fn blpop_takes_first_non_empty_key_without_blocking() {
    let (addr, server) = common::start().await;
    let mut c = common::client(addr).await;
    c.rpush("k2", &["x"]).await.unwrap();
    c.rpush("k4", &["y"]).await.unwrap();
    assert_eq!(
        c.blpop(&["k1", "k2", "k3", "k4"], 0.0).await.unwrap(),
        Some((b("k2"), b("x")))
    );
    assert_eq!(server.total_waiters(), 0);
}

#[tokio::test]
async fn blpop_times_out_with_nil_after_the_timeout() {
    let (addr, server) = common::start().await;
    let mut c = common::client(addr).await;
    let t0 = Instant::now();
    assert_eq!(c.blpop(&["empty"], 0.2).await.unwrap(), None);
    let took = t0.elapsed();
    assert!(
        took >= Duration::from_millis(200),
        "returned early: {took:?}"
    );
    assert!(
        took < Duration::from_millis(1000),
        "returned late: {took:?}"
    );
    assert_eq!(server.total_waiters(), 0, "timed-out waiter removed");
    server.check_invariants().unwrap();
}

#[tokio::test]
async fn select_isolates_databases() {
    let (addr, _) = common::start().await;
    let mut d0 = common::client(addr).await;
    let mut d3 = common::client(addr).await;
    d3.select(3).await.unwrap();
    d3.set("color", "blue").await.unwrap();
    d0.set("color", "red").await.unwrap();
    assert_eq!(d3.get("color").await.unwrap(), Some(b("blue")));
    assert_eq!(d0.get("color").await.unwrap(), Some(b("red")));

    let mut d15 = common::client(addr).await;
    d15.select(15).await.unwrap();
    assert_eq!(d15.get("color").await.unwrap(), None);

    let mut d0_explicit = common::client(addr).await;
    d0_explicit.select(0).await.unwrap();
    assert_eq!(d0_explicit.get("color").await.unwrap(), Some(b("red")));
}

#[tokio::test]
async fn select_only_as_first_command() {
    let (addr, _) = common::start().await;
    let late = Frame::Error("ERR SELECT is only allowed as the first command".into());

    let mut c = common::client(addr).await;
    c.ping().await.unwrap();
    assert_eq!(c.cmd(&["SELECT", "1"]).await.unwrap(), late);

    let mut c = common::client(addr).await;
    c.select(1).await.unwrap();
    assert_eq!(c.cmd(&["SELECT", "2"]).await.unwrap(), late);

    // Rejected requests have no effect, so they do not start the session:
    // and neither do connection-setup commands (CLIENT SETINFO, COMMAND):
    // this is what client libraries send while connecting.
    let mut c = common::client(addr).await;
    assert_eq!(
        c.cmd(&["CLIENT", "SETINFO", "LIB-NAME", "x"])
            .await
            .unwrap(),
        Frame::ok()
    );
    assert_eq!(
        c.cmd(&["CLIENT", "SETNAME", "worker"]).await.unwrap(),
        Frame::ok()
    );
    assert!(matches!(
        c.cmd(&["COMMAND", "DOCS"]).await.unwrap(),
        Frame::Array(_)
    ));
    assert!(matches!(c.cmd(&["GET"]).await.unwrap(), Frame::Error(_)));
    assert!(matches!(
        c.cmd(&["SELECT", "99"]).await.unwrap(),
        Frame::Error(_)
    ));
    c.select(2).await.unwrap();
    c.set("lib", "db2").await.unwrap();
    assert_eq!(c.cmd(&["SELECT", "3"]).await.unwrap(), late);
    let mut d2 = common::client(addr).await;
    d2.select(2).await.unwrap();
    assert_eq!(d2.get("lib").await.unwrap(), Some(b("db2")));

    // Any executed command, even PING, ends the window.
    let mut c = common::client(addr).await;
    c.ping().await.unwrap();
    assert_eq!(c.cmd(&["SELECT", "2"]).await.unwrap(), late);

    // A late SELECT does not change the database.
    let mut c = common::client(addr).await;
    c.set("where", "db0").await.unwrap();
    assert_eq!(c.cmd(&["SELECT", "5"]).await.unwrap(), late);
    assert_eq!(c.get("where").await.unwrap(), Some(b("db0")));
}

#[tokio::test]
async fn blocked_pop_on_one_db_ignores_pushes_on_another() {
    let (addr, server) = common::start().await;
    let mut w = common::client(addr).await;
    w.select(1).await.unwrap();
    let waiter = tokio::spawn(async move { w.blpop(&["q"], 0.3).await.unwrap() });
    common::wait_until("blocked", || server.db(1).waiter_count() == 1).await;
    let mut p = common::client(addr).await;
    p.rpush("q", &["db0-value"]).await.unwrap();
    assert_eq!(waiter.await.unwrap(), None);
    assert_eq!(p.exists(&["q"]).await.unwrap(), 1);
}

#[tokio::test]
async fn many_connections_at_once() {
    let (addr, server) = common::start().await;
    let mut tasks = Vec::new();
    for i in 0..300 {
        tasks.push(tokio::spawn(async move {
            let mut c = common::client(addr).await;
            let key = format!("k{i}");
            c.set(&key, &key).await.unwrap();
            assert_eq!(c.get(&key).await.unwrap(), Some(b(&key)));
            c.rpush("shared", &[key.as_str()]).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut c = common::client(addr).await;
    let mut n = 0;
    while c.blpop(&["shared"], 0.01).await.unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 300);
    server.check_invariants().unwrap();
}

// ---- the binaries ----------------------------------------------------------

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn server_binary_honours_port_flag_and_cli_binary_talks_to_it() {
    let port = free_port();
    let _server = Child(
        std::process::Command::new(env!("CARGO_BIN_EXE_micro-redis"))
            .args(["--bind", "127.0.0.1", "--port", &port.to_string()])
            .env("RUST_LOG", "warn")
            .spawn()
            .unwrap(),
    );
    let addr = format!("127.0.0.1:{port}");
    let start = Instant::now();
    let mut c = loop {
        match micro_redis::client::Client::connect(&addr).await {
            Ok(c) => break c,
            Err(_) if start.elapsed() < Duration::from_secs(10) => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(e) => panic!("server did not start: {e}"),
        }
    };
    assert_eq!(c.ping().await.unwrap(), "PONG");

    let cli = |args: &[&str]| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_micro-redis-cli"))
            .args(["-p", &port.to_string()])
            .args(args)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    assert_eq!(cli(&["-n", "4", "SET", "x", "hello world"]), "OK");
    assert_eq!(cli(&["-n", "4", "GET", "x"]), "\"hello world\"");
    assert_eq!(cli(&["GET", "x"]), "(nil)");
    assert_eq!(cli(&["RPUSH", "q", "a", "b"]), "(integer) 2");
    assert_eq!(cli(&["BLPOP", "q", "0"]), "1) \"q\"\n2) \"a\"");
}

#[test]
fn server_binary_rejects_bad_flags() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_micro-redis"))
        .args(["--port", "notaport"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let help = std::process::Command::new(env!("CARGO_BIN_EXE_micro-redis"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--port"));
}

// ---- reachable from other machines -------------------------------------------

/// This machine's own network address (not 127.0.0.1), or `None` if it has
/// no network. "Connecting" a UDP socket sends nothing; it only asks the OS
/// which local address it would use to reach the outside world.
fn network_address() -> Option<std::net::IpAddr> {
    let s = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("192.0.2.1:9").ok()?;
    let ip = s.local_addr().ok()?.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

/// Start the real server binary with `args` and wait until `addr` answers.
async fn start_binary(args: &[&str], addr: &str) -> (Child, micro_redis::client::Client) {
    let child = Child(
        std::process::Command::new(env!("CARGO_BIN_EXE_micro-redis"))
            .args(args)
            .env("RUST_LOG", "warn")
            .spawn()
            .unwrap(),
    );
    let start = Instant::now();
    loop {
        match micro_redis::client::Client::connect(addr).await {
            Ok(c) => return (child, c),
            Err(_) if start.elapsed() < Duration::from_secs(10) => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(e) => panic!("server did not start on {addr}: {e}"),
        }
    }
}

/// With default flags the server accepts connections arriving on the
/// machine's network address, which is how another machine reaches it. A
/// client there shares databases and blocking queues with a local client.
/// With `--bind 127.0.0.1` the same address is refused.
#[tokio::test]
async fn default_bind_accepts_connections_from_the_network() {
    let Some(ip) = network_address() else {
        eprintln!("SKIPPED: this machine has no network address");
        return;
    };
    let port = free_port();
    let remote = std::net::SocketAddr::new(ip, port).to_string();
    let local = format!("127.0.0.1:{port}");

    // Default flags: no --bind.
    let (server, mut net) = start_binary(&["--port", &port.to_string()], &remote).await;
    // A network client blocks on database 4; a local client's push wakes it.
    // (SELECT first: after any other command the spec rejects it.)
    net.select(4).await.unwrap();
    assert_eq!(net.ping().await.unwrap(), "PONG");
    let waiter = tokio::spawn(async move { net.blpop(&["jobs"], 5.0).await.unwrap() });
    let mut lo = micro_redis::client::Client::connect(&local).await.unwrap();
    lo.select(4).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    lo.rpush("jobs", &["hello"]).await.unwrap();
    assert_eq!(waiter.await.unwrap(), Some((b("jobs"), b("hello"))));
    drop(server);

    // --bind 127.0.0.1: only this machine may connect.
    let port = free_port();
    let local = format!("127.0.0.1:{port}");
    let (_server, _c) = start_binary(
        &["--bind", "127.0.0.1", "--port", &port.to_string()],
        &local,
    )
    .await;
    let remote = std::net::SocketAddr::new(ip, port);
    assert!(
        tokio::net::TcpStream::connect(remote).await.is_err(),
        "--bind 127.0.0.1 must refuse connections on {remote}"
    );
}
