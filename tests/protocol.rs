//! Wire-level tests: exact bytes in, exact bytes out.

mod common;

use micro_redis::resp::Frame;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Send raw bytes, read until `expect.len()` bytes arrived, compare exactly.
async fn exchange(stream: &mut TcpStream, send: &[u8], expect: &[u8]) {
    stream.write_all(send).await.unwrap();
    let mut got = vec![0u8; expect.len()];
    tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut got))
        .await
        .expect("reply in time")
        .expect("read");
    assert_eq!(
        got.escape_ascii().to_string(),
        expect.escape_ascii().to_string(),
        "reply to {:?}",
        send.escape_ascii().to_string()
    );
}

async fn raw(addr: std::net::SocketAddr) -> TcpStream {
    common::raw(addr).await
}

async fn is_closed(stream: &mut TcpStream) -> bool {
    let mut b = [0u8; 64];
    matches!(
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut b)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test]
async fn ping_byte_by_byte() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    for &b in b"*1\r\n$4\r\nPING\r\n" {
        s.write_all(&[b]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    exchange(&mut s, b"", b"+PONG\r\n").await;
    exchange(&mut s, b"*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n", b"$2\r\nhi\r\n").await;
}

#[tokio::test]
async fn binary_safe_values() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    exchange(
        &mut s,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\na\r\n\0b\r\n",
        b"+OK\r\n",
    )
    .await;
    exchange(
        &mut s,
        b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
        b"$5\r\na\r\n\0b\r\n",
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$3\r\nSET\r\n$1\r\ne\r\n$0\r\n\r\n",
        b"+OK\r\n",
    )
    .await;
    exchange(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\ne\r\n", b"$0\r\n\r\n").await;
    exchange(&mut s, b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n", b"$-1\r\n").await;
}

#[tokio::test]
async fn every_reply_type_is_exact() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nRPUSH\r\n$1\r\nq\r\n$1\r\nx\r\n",
        b":1\r\n",
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n",
        b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n",
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nBRPOP\r\n$1\r\nq\r\n$4\r\n0.05\r\n",
        b"*-1\r\n",
    )
    .await;
    exchange(&mut s, b"*2\r\n$6\r\nEXISTS\r\n$1\r\nq\r\n", b":0\r\n").await;
}

#[tokio::test]
async fn inline_commands_work_like_redis() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    exchange(&mut s, b"PING\r\n", b"+PONG\r\n").await;
    exchange(&mut s, b"SET greeting \"hello world\"\r\n", b"+OK\r\n").await;
    exchange(&mut s, b"GET greeting\n", b"$11\r\nhello world\r\n").await;
}

#[tokio::test]
async fn pipelined_requests_are_answered_in_order() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    exchange(
        &mut s,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$4\r\nPING\r\n",
        b"+OK\r\n$1\r\n1\r\n+PONG\r\n",
    )
    .await;
}

#[tokio::test]
async fn commands_after_a_blocking_pop_wait_their_turn() {
    let (addr, server) = common::start().await;
    let mut s = raw(addr).await;
    // BLPOP blocks; the pipelined GET must be answered only afterwards.
    s.write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n*2\r\n$3\r\nGET\r\n$1\r\nz\r\n")
        .await
        .unwrap();
    common::wait_until("client blocked", || server.total_waiters() == 1).await;
    let mut other = common::client(addr).await;
    other.rpush("q", &["v"]).await.unwrap();
    exchange(&mut s, b"", b"*2\r\n$1\r\nq\r\n$1\r\nv\r\n$-1\r\n").await;
}

#[tokio::test]
async fn errors_keep_the_connection_usable() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    let cases: &[(&[u8], &[u8])] = &[
        (b"*2\r\n$3\r\nFOO\r\n$1\r\nx\r\n", b"-ERR unknown command 'FOO', with args beginning with: 'x' \r\n"),
        (b"*1\r\n$3\r\nGET\r\n", b"-ERR wrong number of arguments for 'get' command\r\n"),
        (b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n", b"-ERR wrong number of arguments for 'set' command\r\n"),
        (b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nZZ\r\n$1\r\n9\r\n", b"-ERR syntax error\r\n"),
        (b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$2\r\n-1\r\n", b"-ERR timeout is negative\r\n"),
        (b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$3\r\nabc\r\n", b"-ERR timeout is not a float or out of range\r\n"),
        (
            b"*8\r\n$5\r\nBRPOP\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n$1\r\n0\r\n",
            b"-ERR BRPOP accepts at most 5 keys\r\n",
        ),
        (b"*0\r\n*1\r\n$4\r\nPING\r\n", b"+PONG\r\n"), // empty request is ignored
    ];
    for (send, expect) in cases {
        exchange(&mut s, send, expect).await;
        exchange(&mut s, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
    }
}

#[tokio::test]
async fn wrongtype_errors() {
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    let wrongtype: &[u8] =
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    exchange(
        &mut s,
        b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$1\r\nv\r\n",
        b"+OK\r\n",
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nLPUSH\r\n$1\r\ns\r\n$1\r\nx\r\n",
        wrongtype,
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nRPUSH\r\n$1\r\ns\r\n$1\r\nx\r\n",
        wrongtype,
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nBLPOP\r\n$1\r\ns\r\n$1\r\n0\r\n",
        wrongtype,
    )
    .await;
    exchange(
        &mut s,
        b"*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\nx\r\n",
        b":1\r\n",
    )
    .await;
    exchange(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nl\r\n", wrongtype).await;
}

#[tokio::test]
async fn select_errors() {
    let (addr, _) = common::start().await;
    for (send, expect) in [
        (
            &b"*2\r\n$6\r\nSELECT\r\n$2\r\n16\r\n"[..],
            &b"-ERR DB index is out of range\r\n"[..],
        ),
        (
            b"*2\r\n$6\r\nSELECT\r\n$2\r\n-1\r\n",
            b"-ERR DB index is out of range\r\n",
        ),
        (
            b"*2\r\n$6\r\nSELECT\r\n$1\r\nx\r\n",
            b"-ERR value is not an integer or out of range\r\n",
        ),
    ] {
        let mut s = raw(addr).await;
        exchange(&mut s, send, expect).await;
    }
    let mut s = raw(addr).await;
    exchange(&mut s, b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n", b"+OK\r\n").await;
    exchange(
        &mut s,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n",
        b"-ERR SELECT is only allowed as the first command\r\n",
    )
    .await;
}

#[tokio::test]
async fn malformed_resp_gets_an_error_then_close() {
    let (addr, _) = common::start().await;
    for bad in [
        &b"*1\r\n?x\r\n"[..],
        b"*1\r\n$99999999999\r\n",
        b"*1\r\n$-7\r\n",
        b"*x\r\n",
        b"*1\r\n$3\r\nabcXY",
        b"SET k \"unbalanced\r\n",
    ] {
        let mut s = raw(addr).await;
        s.write_all(bad).await.unwrap();
        let mut c = common::client(addr).await;
        // A separate client keeps working.
        assert_eq!(c.ping().await.unwrap(), "PONG");
        let mut buf = vec![0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let reply = String::from_utf8_lossy(&buf[..n]).to_string();
        assert!(
            reply.starts_with("-ERR Protocol error"),
            "{bad:?} -> {reply}"
        );
        assert!(
            is_closed(&mut s).await,
            "connection closed after protocol error"
        );
    }
}

#[tokio::test]
async fn oversized_frame_is_never_allocated() {
    // Announce ~500 MB, send a few bytes, then hang up: no crash, no OOM.
    let (addr, _) = common::start().await;
    let mut s = raw(addr).await;
    s.write_all(b"*2\r\n$3\r\nGET\r\n$500000000\r\nabc")
        .await
        .unwrap();
    drop(s);
    let mut c = common::client(addr).await;
    assert_eq!(c.ping().await.unwrap(), "PONG");
}

#[tokio::test]
async fn client_parses_every_reply() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    assert_eq!(
        c.cmd(&["PING"]).await.unwrap(),
        Frame::Simple("PONG".into())
    );
    assert_eq!(c.cmd(&["EXISTS", "x"]).await.unwrap(), Frame::Integer(0));
    assert_eq!(c.cmd(&["GET", "x"]).await.unwrap(), Frame::Null);
    assert_eq!(
        c.cmd(&["BLPOP", "x", "0.01"]).await.unwrap(),
        Frame::NullArray
    );
    assert!(matches!(c.cmd(&["NOPE"]).await.unwrap(), Frame::Error(_)));
}
