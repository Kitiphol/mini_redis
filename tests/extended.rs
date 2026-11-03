//! The commands added for the distributed crawler (Mini 1 extra credit):
//! MULTI/EXEC, Lua scripting, expiry, sets, hashes, FLUSHALL — through TCP.

mod common;

use bytes::Bytes;
use micro_redis::resp::Frame;
use std::time::Duration;

fn bulk(s: &str) -> Frame {
    Frame::Bulk(Bytes::copy_from_slice(s.as_bytes()))
}
fn int(n: i64) -> Frame {
    Frame::Integer(n)
}
fn simple(s: &str) -> Frame {
    Frame::Simple(s.into())
}

#[tokio::test]
async fn database_0_is_the_default_without_select() {
    let (addr, server) = common::start().await;
    let mut c = common::client(addr).await;
    c.set("k", "v").await.unwrap();
    let mut d0 = common::client(addr).await;
    d0.select(0).await.unwrap();
    assert_eq!(d0.get("k").await.unwrap(), Some(Bytes::from_static(b"v")));
    assert_eq!(server.db(0).stored_keys(), 1);
    // ...and SELECT is then too late.
    assert!(matches!(
        c.cmd(&["SELECT", "1"]).await.unwrap(),
        Frame::Error(_)
    ));
}

#[tokio::test]
async fn multi_exec_runs_all_or_nothing() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    assert_eq!(c.cmd(&["MULTI"]).await.unwrap(), Frame::ok());
    assert_eq!(
        c.cmd(&["INCRBY", "n", "5"]).await.unwrap(),
        simple("QUEUED")
    );
    assert_eq!(
        c.cmd(&["RPUSH", "l", "a", "b"]).await.unwrap(),
        simple("QUEUED")
    );
    assert_eq!(
        c.cmd(&["HSET", "h", "f", "v"]).await.unwrap(),
        simple("QUEUED")
    );
    assert_eq!(
        c.cmd(&["EXEC"]).await.unwrap(),
        Frame::Array(vec![int(5), int(2), int(1)])
    );

    // A command rejected while queueing aborts the whole transaction.
    c.cmd(&["MULTI"]).await.unwrap();
    c.cmd(&["INCR", "n"]).await.unwrap();
    assert!(matches!(c.cmd(&["GET"]).await.unwrap(), Frame::Error(_)));
    assert!(
        matches!(c.cmd(&["EXEC"]).await.unwrap(), Frame::Error(e) if e.starts_with("EXECABORT"))
    );
    assert_eq!(c.cmd(&["GET", "n"]).await.unwrap(), bulk("5"));

    // DISCARD drops the queue.
    c.cmd(&["MULTI"]).await.unwrap();
    c.cmd(&["SET", "n", "0"]).await.unwrap();
    assert_eq!(c.cmd(&["DISCARD"]).await.unwrap(), Frame::ok());
    assert_eq!(c.cmd(&["GET", "n"]).await.unwrap(), bulk("5"));
    assert!(matches!(c.cmd(&["EXEC"]).await.unwrap(), Frame::Error(_)));
}

#[tokio::test]
async fn transactions_are_atomic_under_concurrency() {
    // Each transaction moves one unit between two counters; a reader must
    // never see the sum differ from 0.
    let (addr, _) = common::start().await;
    let mut writers = Vec::new();
    for _ in 0..8 {
        writers.push(tokio::spawn(async move {
            let mut c = common::client(addr).await;
            for _ in 0..300 {
                c.cmd(&["MULTI"]).await.unwrap();
                c.cmd(&["INCR", "a"]).await.unwrap();
                c.cmd(&["DECR", "b"]).await.unwrap();
                c.cmd(&["EXEC"]).await.unwrap();
            }
        }));
    }
    let mut r = common::client(addr).await;
    for _ in 0..500 {
        let Frame::Array(v) = r.cmd(&["MGET", "a", "b"]).await.unwrap() else {
            panic!()
        };
        let n = |f: &Frame| match f {
            Frame::Bulk(b) => std::str::from_utf8(b).unwrap().parse::<i64>().unwrap(),
            _ => 0,
        };
        assert_eq!(n(&v[0]) + n(&v[1]), 0, "saw a half-applied transaction");
    }
    for w in writers {
        w.await.unwrap();
    }
    assert_eq!(r.cmd(&["GET", "a"]).await.unwrap(), bulk("2400"));
}

#[tokio::test]
async fn eval_evalsha_and_script_cache() {
    let (addr, _) = common::start().await;
    let mut c = common::client(addr).await;
    let src = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    assert_eq!(
        c.cmd(&["EVAL", src, "1", "k", "hi"]).await.unwrap(),
        bulk("hi")
    );

    let Frame::Bulk(sha) = c
        .cmd(&["SCRIPT", "LOAD", "return {1, 'two', ARGV[1]}"])
        .await
        .unwrap()
    else {
        panic!()
    };
    let sha = std::str::from_utf8(&sha).unwrap().to_string();
    assert_eq!(
        c.cmd(&["EVALSHA", &sha, "0", "x"]).await.unwrap(),
        Frame::Array(vec![int(1), bulk("two"), bulk("x")])
    );
    assert_eq!(
        c.cmd(&["SCRIPT", "EXISTS", &sha, "0000"]).await.unwrap(),
        Frame::Array(vec![int(1), int(0)])
    );
    assert_eq!(c.cmd(&["SCRIPT", "FLUSH"]).await.unwrap(), Frame::ok());
    assert!(matches!(
        c.cmd(&["EVALSHA", &sha, "0"]).await.unwrap(),
        Frame::Error(e) if e.starts_with("NOSCRIPT")
    ));

    // Errors carry Redis' script location suffix.
    let Frame::Error(e) = c
        .cmd(&["EVAL", "return redis.call('INCR', KEYS[1])", "1", "k"])
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(e.starts_with("ERR value is not an integer"), "{e}");
    assert!(
        e.contains("script: ") && e.ends_with("on @user_script:1."),
        "{e}"
    );

    // Blocking commands are refused inside scripts.
    assert!(matches!(
        c.cmd(&["EVAL", "return redis.call('BLPOP', 'q', 0)", "0"])
            .await
            .unwrap(),
        Frame::Error(_)
    ));
}

#[tokio::test]
async fn scripts_are_atomic_no_lost_updates() {
    // Read-modify-write in Lua (GET, then SET +1) from many clients at once.
    // If scripts interleaved, increments would be lost.
    let (addr, _) = common::start().await;
    let script = "local v = tonumber(redis.call('GET', KEYS[1]) or '0'); \
                  redis.call('SET', KEYS[1], v + 1); return v + 1";
    let mut tasks = Vec::new();
    for _ in 0..16 {
        tasks.push(tokio::spawn(async move {
            let mut c = common::client(addr).await;
            for _ in 0..200 {
                assert!(matches!(
                    c.cmd(&["EVAL", script, "1", "counter"]).await.unwrap(),
                    Frame::Integer(_)
                ));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut c = common::client(addr).await;
    assert_eq!(c.cmd(&["GET", "counter"]).await.unwrap(), bulk("3200"));
}

#[tokio::test]
async fn crawler_style_claim_hands_each_url_out_once() {
    // The crawler's pattern: a script pops a URL from the frontier and
    // records it as in-flight, and finishing moves it to a "done" set.
    let (addr, server) = common::start().await;
    let mut c = common::client(addr).await;
    for i in 0..500 {
        c.cmd(&["RPUSH", "frontier", &format!("u{i}")])
            .await
            .unwrap();
    }
    let take = "local u = redis.call('LPOP', KEYS[1]); \
                if not u then return false end; \
                redis.call('HSET', KEYS[2], u, ARGV[1]); return u";
    let mut tasks = Vec::new();
    for w in 0..10 {
        tasks.push(tokio::spawn(async move {
            let mut c = common::client(addr).await;
            let mut mine = Vec::new();
            loop {
                match c
                    .cmd(&["EVAL", take, "2", "frontier", "inflight", &format!("w{w}")])
                    .await
                    .unwrap()
                {
                    Frame::Bulk(u) => {
                        let reply = c
                            .cmd(&["SADD", "done", std::str::from_utf8(&u).unwrap()])
                            .await
                            .unwrap();
                        assert_eq!(reply, int(1), "URL handed out twice");
                        mine.push(u);
                    }
                    Frame::Null => return mine.len(),
                    other => panic!("{other:?}"),
                }
            }
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await.unwrap();
    }
    assert_eq!(total, 500);
    assert_eq!(c.cmd(&["SCARD", "done"]).await.unwrap(), int(500));
    assert_eq!(c.cmd(&["HLEN", "inflight"]).await.unwrap(), int(500));
    server.check_invariants().unwrap();
}

#[tokio::test]
async fn keys_expire() {
    let (addr, server) = common::start().await;
    let mut c = common::client(addr).await;
    c.cmd(&["SETEX", "s", "100", "v"]).await.unwrap();
    c.cmd(&["SET", "p", "v", "PX", "50"]).await.unwrap();
    c.cmd(&["RPUSH", "l", "x"]).await.unwrap();
    c.cmd(&["PEXPIRE", "l", "50"]).await.unwrap();
    assert!(matches!(
        c.cmd(&["TTL", "s"]).await.unwrap(),
        Frame::Integer(99 | 100)
    ));
    tokio::time::sleep(Duration::from_millis(80)).await;
    // Lazy expiry on access...
    assert_eq!(c.cmd(&["GET", "p"]).await.unwrap(), Frame::Null);
    assert_eq!(c.cmd(&["TTL", "p"]).await.unwrap(), int(-2));
    // ...and the background sweeper removes keys nobody touches.
    // "l" was never touched after expiring.
    common::wait_until("sweeper", || server.db(0).stored_keys() == 1).await;
    assert_eq!(
        c.cmd(&["KEYS", "*"]).await.unwrap(),
        Frame::Array(vec![bulk("s")])
    );
    assert_eq!(c.cmd(&["PERSIST", "s"]).await.unwrap(), int(1));
    assert_eq!(c.cmd(&["TTL", "s"]).await.unwrap(), int(-1));
}

#[tokio::test]
async fn flushall_clears_every_database() {
    let (addr, _) = common::start().await;
    let mut d0 = common::client(addr).await;
    let mut d7 = common::client(addr).await;
    d7.select(7).await.unwrap();
    d0.set("a", "1").await.unwrap();
    d7.set("b", "2").await.unwrap();
    assert_eq!(d0.cmd(&["FLUSHALL"]).await.unwrap(), Frame::ok());
    assert_eq!(d0.cmd(&["DBSIZE"]).await.unwrap(), int(0));
    assert_eq!(d7.cmd(&["DBSIZE"]).await.unwrap(), int(0));
}

#[tokio::test]
async fn pipelined_mixed_commands_answer_in_order() {
    // One big pipeline: plain data commands (batched together), interrupted by
    // a rejected command, a transaction, a script and a non-blocking BLPOP.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (addr, server) = common::start().await;
    let mut s = common::raw(addr).await;
    let cmd = |args: &[&str]| {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out += &format!("${}\r\n{a}\r\n", a.len());
        }
        out
    };
    let mut req = String::new();
    let mut want = String::new();
    for i in 1..=300 {
        req += &cmd(&["INCR", "n"]);
        want += &format!(":{i}\r\n");
        if i % 100 == 0 {
            req += &cmd(&["NOPE"]);
            want += "-ERR unknown command 'NOPE', with args beginning with: \r\n";
            req += &(cmd(&["MULTI"]) + &cmd(&["INCR", "n"]) + &cmd(&["EXEC"]));
            want += &format!("+OK\r\n+QUEUED\r\n*1\r\n:{}\r\n", i + 1);
            req += &cmd(&["EVAL", "return redis.call('DECR', KEYS[1])", "1", "n"]);
            want += &format!(":{i}\r\n");
        }
    }
    req += &(cmd(&["RPUSH", "q", "a"]) + &cmd(&["BLPOP", "q", "0"]) + &cmd(&["GET", "n"]));
    want += ":1\r\n*2\r\n$1\r\nq\r\n$1\r\na\r\n$3\r\n300\r\n";
    s.write_all(req.as_bytes()).await.unwrap();
    let mut got = vec![0u8; want.len()];
    tokio::time::timeout(std::time::Duration::from_secs(10), s.read_exact(&mut got))
        .await
        .expect("all replies arrive")
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&got), want);
    server.check_invariants().unwrap();
}
