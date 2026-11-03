//! Differential test against a real Redis: the same commands go to a real
//! `redis-server` and to micro-redis, in lockstep, and every reply must be
//! identical. Skipped (with a message) if `redis-server` isn't installed.
//!
//! Run just this file with: `cargo test --release --test compat -- --nocapture`

mod common;

use common::Rng;
use micro_redis::client::Client;
use micro_redis::resp::Frame;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct RealRedis(Child);

impl Drop for RealRedis {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start a throwaway real Redis on a free port, or None if unavailable.
async fn real_redis() -> Option<(RealRedis, Client)> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()?
        .port();
    let child = Command::new("redis-server")
        .args([
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let guard = RealRedis(child);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(c) = Client::connect(("127.0.0.1", port)).await {
            return Some((guard, c));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

/// Make replies comparable: Redis returns sets, hash fields and KEYS in no
/// particular order, and TTLs depend on timing.
fn normalize(cmd: &[String], f: Frame) -> Frame {
    let name = cmd[0].to_ascii_lowercase();
    match (name.as_str(), f) {
        ("smembers" | "hkeys" | "hvals" | "keys", Frame::Array(mut items)) => {
            items.sort_by_key(|i| format!("{i:?}"));
            Frame::Array(items)
        }
        ("hgetall", Frame::Array(items)) => {
            let mut pairs: Vec<Vec<Frame>> = items.chunks(2).map(|c| c.to_vec()).collect();
            pairs.sort_by_key(|p| format!("{p:?}"));
            Frame::Array(pairs.concat())
        }
        ("ttl" | "pttl", Frame::Integer(n)) if n >= 0 => Frame::Integer(0),
        (_, other) => other,
    }
}

async fn both(real: &mut Client, ours: &mut Client, cmd: &[String]) -> (Frame, Frame) {
    let a = real.cmd(cmd).await.expect("real redis reply");
    let b = ours.cmd(cmd).await.expect("micro-redis reply");
    (normalize(cmd, a), normalize(cmd, b))
}

macro_rules! cmd {
    ($($x:expr),* $(,)?) => { vec![$($x.to_string()),*] };
}

/// Hand-picked cases: every command, with valid, invalid and wrong-type uses.
fn edge_cases() -> Vec<Vec<String>> {
    vec![
        // strings
        cmd!["SET", "s", "abc"],
        cmd!["GET", "s"],
        cmd!["GET", "nope"],
        cmd!["STRLEN", "s"],
        cmd!["APPEND", "s", "def"],
        cmd!["APPEND", "new", "x"],
        cmd!["GETSET", "s", "v2"],
        cmd!["SETNX", "s", "x"],
        cmd!["SETNX", "s2", "x"],
        cmd!["GETDEL", "s2"],
        cmd!["GETDEL", "s2"],
        cmd!["SET", "n", "10"],
        cmd!["INCR", "n"],
        cmd!["DECR", "n"],
        cmd!["INCRBY", "n", "-5"],
        cmd!["DECRBY", "n", "3"],
        cmd!["INCRBY", "n", "abc"],
        cmd!["INCR", "s"],
        cmd!["SET", "z", "01"],
        cmd!["INCR", "z"],
        cmd!["SET", "z", "-0"],
        cmd!["INCR", "z"],
        cmd!["SET", "big", "9223372036854775807"],
        cmd!["INCR", "big"],
        cmd!["DECRBY", "big", "-9223372036854775808"],
        cmd!["INCRBY", "fresh", "7"],
        cmd!["MSET", "a", "1", "b", "2"],
        cmd!["MSET", "a", "1", "b"],
        cmd!["MGET", "a", "b", "nope", "l1"],
        cmd!["SET", "o", "v", "NX"],
        cmd!["SET", "o", "v2", "NX"],
        cmd!["SET", "o", "v3", "XX"],
        cmd!["SET", "o2", "v", "XX"],
        cmd!["SET", "o", "v4", "GET"],
        cmd!["SET", "o", "v", "EX", "100"],
        cmd!["SET", "o", "v", "PX", "100000"],
        cmd!["SET", "o", "v", "EX", "0"],
        cmd!["SET", "o", "v", "EX", "-1"],
        cmd!["SET", "o", "v", "EX", "x"],
        cmd!["SET", "o", "v", "EX"],
        cmd!["SET", "o", "v", "NX", "XX"],
        cmd!["SET", "o", "v", "EX", "10", "PX", "10"],
        cmd!["SET", "o", "v", "KEEPTTL"],
        cmd!["SET", "o", "v", "BOGUS"],
        cmd!["SETEX", "e", "100", "v"],
        cmd!["SETEX", "e", "0", "v"],
        cmd!["SETEX", "e", "x", "v"],
        cmd!["PSETEX", "e", "100000", "v"],
        cmd!["PSETEX", "e", "-5", "v"],
        // lists
        cmd!["RPUSH", "l1", "a", "b", "c", "d", "e"],
        cmd!["LPUSH", "l1", "z"],
        cmd!["LLEN", "l1"],
        cmd!["LRANGE", "l1", "0", "-1"],
        cmd!["LRANGE", "l1", "-2", "100"],
        cmd!["LRANGE", "l1", "5", "1"],
        cmd!["LRANGE", "l1", "x", "1"],
        cmd!["LRANGE", "nope", "0", "-1"],
        cmd!["LINDEX", "l1", "0"],
        cmd!["LINDEX", "l1", "-1"],
        cmd!["LINDEX", "l1", "99"],
        cmd!["LINDEX", "l1", "x"],
        cmd!["LSET", "l1", "1", "Q"],
        cmd!["LSET", "l1", "99", "Q"],
        cmd!["LSET", "nope", "0", "Q"],
        cmd!["RPUSH", "l2", "x", "y", "x", "z", "x"],
        cmd!["LREM", "l2", "1", "x"],
        cmd!["LREM", "l2", "-1", "x"],
        cmd!["LREM", "l2", "0", "x"],
        cmd!["LREM", "l2", "0", "zz"],
        cmd!["LRANGE", "l2", "0", "-1"],
        cmd!["LTRIM", "l1", "1", "3"],
        cmd!["LRANGE", "l1", "0", "-1"],
        cmd!["LTRIM", "l1", "5", "2"],
        cmd!["EXISTS", "l1"],
        cmd!["RPUSH", "l3", "1", "2", "3", "4"],
        cmd!["LPOP", "l3"],
        cmd!["RPOP", "l3"],
        cmd!["LPOP", "l3", "5"],
        cmd!["LPOP", "l3"],
        cmd!["LPOP", "l3", "2"],
        cmd!["RPOP", "nope", "2"],
        cmd!["RPUSH", "l3", "1"],
        cmd!["LPOP", "l3", "0"],
        cmd!["LPOP", "l3", "-1"],
        cmd!["LPOP", "l3", "x"],
        cmd!["LPUSHX", "nope", "a"],
        cmd!["RPUSHX", "l3", "a", "b"],
        cmd!["LPUSH", "s", "x"],
        cmd!["RPUSH", "src", "1", "2", "3"],
        cmd!["RPOPLPUSH", "src", "dst"],
        cmd!["LMOVE", "src", "dst", "LEFT", "RIGHT"],
        cmd!["LMOVE", "src", "dst", "LEFT", "UP"],
        cmd!["LMOVE", "src", "s", "LEFT", "LEFT"],
        cmd!["RPOPLPUSH", "nope", "dst"],
        cmd!["LRANGE", "dst", "0", "-1"],
        cmd!["RPOPLPUSH", "src", "src"],
        cmd!["RPUSH", "one", "x"],
        cmd!["EXPIRE", "one", "100"],
        cmd!["RPOPLPUSH", "one", "one"],
        cmd!["TTL", "one"],
        // sets
        cmd!["SADD", "st", "a", "b", "a"],
        cmd!["SADD", "st", "c"],
        cmd!["SCARD", "st"],
        cmd!["SMEMBERS", "st"],
        cmd!["SISMEMBER", "st", "a"],
        cmd!["SISMEMBER", "st", "q"],
        cmd!["SMISMEMBER", "st", "a", "q"],
        cmd!["SREM", "st", "a", "q"],
        cmd!["SREM", "nope", "a"],
        cmd!["SADD", "s", "x"],
        cmd!["SCARD", "nope"],
        cmd!["SREM", "st", "b", "c"],
        cmd!["EXISTS", "st"],
        cmd!["SMEMBERS", "l1"],
        // hashes
        cmd!["HSET", "h", "f1", "v1", "f2", "v2"],
        cmd!["HSET", "h", "f1", "x"],
        cmd!["HSET", "h", "f1"],
        cmd!["HMSET", "h", "f3", "v3"],
        cmd!["HGET", "h", "f1"],
        cmd!["HGET", "h", "nope"],
        cmd!["HMGET", "h", "f1", "nope"],
        cmd!["HGETALL", "h"],
        cmd!["HLEN", "h"],
        cmd!["HEXISTS", "h", "f2"],
        cmd!["HKEYS", "h"],
        cmd!["HVALS", "h"],
        cmd!["HSETNX", "h", "f1", "y"],
        cmd!["HSETNX", "h", "f9", "y"],
        cmd!["HINCRBY", "h", "cnt", "5"],
        cmd!["HINCRBY", "h", "cnt", "-7"],
        cmd!["HINCRBY", "h", "f1", "1"],
        cmd!["HINCRBY", "h", "cnt", "x"],
        cmd!["HDEL", "h", "f1", "nope"],
        cmd!["HGETALL", "nope"],
        cmd!["HGET", "s", "f"],
        cmd!["HSET", "l1", "f", "v"],
        // keys and expiry
        cmd!["TYPE", "s"],
        cmd!["TYPE", "l1"],
        cmd!["TYPE", "h"],
        cmd!["TYPE", "nope"],
        cmd!["TYPE", "dst"],
        cmd!["EXISTS", "s", "s", "nope", "h"],
        cmd!["DEL", "a", "b", "nope"],
        cmd!["UNLINK", "n"],
        cmd!["KEYS", "*"],
        cmd!["KEYS", "l?"],
        cmd!["KEYS", "[hs]*"],
        cmd!["KEYS", "nomatch*"],
        cmd!["DBSIZE"],
        cmd!["TTL", "s"],
        cmd!["TTL", "nope"],
        cmd!["EXPIRE", "s", "100"],
        cmd!["TTL", "s"],
        cmd!["PTTL", "s"],
        cmd!["EXPIRE", "s", "50", "NX"],
        cmd!["EXPIRE", "s", "200", "GT"],
        cmd!["EXPIRE", "s", "10", "GT"],
        cmd!["EXPIRE", "s", "10", "LT"],
        cmd!["EXPIRE", "h", "10", "XX"],
        cmd!["EXPIRE", "s", "1", "NX", "XX"],
        cmd!["EXPIRE", "s", "x"],
        cmd!["EXPIRE", "nope", "10"],
        cmd!["PERSIST", "s"],
        cmd!["PERSIST", "s"],
        cmd!["TTL", "s"],
        cmd!["PEXPIRE", "s", "100000"],
        cmd!["EXPIRE", "s", "0"],
        cmd!["EXISTS", "s"],
        cmd!["SET", "t", "v", "EX", "100"],
        cmd!["SET", "t", "v2"],
        cmd!["TTL", "t"],
        cmd!["SET", "t", "v", "EX", "100"],
        cmd!["INCR", "t"],
        cmd!["SET", "t", "5", "EX", "100"],
        cmd!["INCR", "t"],
        cmd!["TTL", "t"],
        cmd!["SET", "t", "v", "KEEPTTL"],
        cmd!["TTL", "t"],
        // misc
        cmd!["PING"],
        cmd!["PING", "hi"],
        cmd!["ECHO", "hello"],
        cmd!["NOPE", "x"],
        cmd!["GET"],
        cmd!["HSET"],
        cmd!["LPOP"],
        cmd!["KEYS"],
        cmd!["DBSIZE", "x"],
        cmd!["FLUSHDB", "x"],
        // scripts
        cmd!["EVAL", "return 1", "0"],
        cmd!["EVAL", "return {1,'a',false,{2,'b'}}", "0"],
        cmd!["EVAL", "return redis.call('GET', KEYS[1])", "1", "o"],
        cmd!["EVAL", "return redis.call('INCR', KEYS[1])", "1", "o"],
        cmd!["EVAL", "return redis.pcall('INCR', KEYS[1])", "1", "o"],
        cmd!["EVAL", "return x.y", "0"],
        cmd!["EVAL", "return (", "0"],
        cmd!["EVAL", "return redis.call('NOPE')", "0"],
        cmd!["EVAL", "return redis.call('GET')", "0"],
        cmd!["EVAL", "return redis.error_reply('MY err')", "0"],
        cmd!["EVAL", "return redis.status_reply('FINE')", "0"],
        cmd!["EVAL", "return 3.99", "0"],
        cmd![
            "EVAL",
            "return {ARGV[1], #KEYS, #ARGV}",
            "2",
            "a",
            "b",
            "c",
            "d"
        ],
        cmd!["EVAL", "error('plain')", "0"],
        cmd![
            "EVAL",
            "local n = redis.call('INCRBY', KEYS[1], ARGV[1]) return n * 2",
            "1",
            "cnt",
            "21"
        ],
        cmd!["EVAL", "return redis.call('SET', KEYS[1], {})", "1", "x"],
        cmd!["EVAL", "return 1", "-1"],
        cmd!["EVAL", "return 1", "3", "a"],
        cmd!["EVAL", "return 1", "x"],
        cmd!["SCRIPT", "LOAD", "return 'loaded'"],
        cmd!["EVALSHA", "d6dbc5d0a8a0a3f0d9cc2b7ef62c6ac1b02ec9a8", "0"],
        cmd!["EVALSHA", "0000000000000000000000000000000000000000", "0"],
        cmd![
            "SCRIPT",
            "EXISTS",
            "d6dbc5d0a8a0a3f0d9cc2b7ef62c6ac1b02ec9a8",
            "ffff"
        ],
        cmd!["SCRIPT", "BOGUS"],
        // transactions
        cmd!["MULTI"],
        cmd!["SET", "m", "1"],
        cmd!["INCR", "m"],
        cmd!["LPUSH", "m", "x"],
        cmd!["GET", "m"],
        cmd!["PING"],
        cmd!["EXEC"],
        cmd!["MULTI"],
        cmd!["SET", "m", "2"],
        cmd!["NOPE"],
        cmd!["EXEC"],
        cmd!["GET", "m"],
        cmd!["MULTI"],
        cmd!["MULTI"],
        cmd!["DISCARD"],
        cmd!["EXEC"],
        cmd!["DISCARD"],
        cmd!["MULTI"],
        cmd!["BLPOP", "emptylist", "0"],
        cmd!["EVAL", "return redis.call('INCR', 'm2')", "0"],
        cmd!["EXEC"],
        cmd!["FLUSHDB"],
        cmd!["DBSIZE"],
    ]
}

/// Random commands over a small keyspace: valid, invalid and wrong-type mixes.
fn random_command(rng: &mut Rng) -> Vec<String> {
    let key = |rng: &mut Rng| format!("k{}", rng.below(6));
    let val = |rng: &mut Rng| match rng.below(5) {
        0 => format!("{}", rng.below(20) as i64 - 5),
        1 => "x".into(),
        2 => "".into(),
        _ => format!("v{}", rng.below(4)),
    };
    let num = |rng: &mut Rng| match rng.below(8) {
        0 => "abc".into(),
        1 => format!("-{}", rng.below(5)),
        _ => format!("{}", rng.below(6)),
    };
    // Blocking pops: a short timeout keeps the lockstep comparison moving when
    // every key is empty (both servers then wait 10 ms and reply nil); now and
    // then an invalid timeout, so the error replies are compared too.
    let bpop = |rng: &mut Rng, name: &str, first: String| {
        let mut c = vec![name.to_string(), first];
        for _ in 0..rng.below(3) {
            c.push(key(rng));
        }
        c.push(
            match rng.below(10) {
                0 => "-1",
                1 => "abc",
                _ => "0.01",
            }
            .to_string(),
        );
        c
    };
    let k = key(rng);
    match rng.below(36) {
        0 => cmd!["GET", k],
        1 => cmd!["SET", k, val(rng)],
        2 => cmd!["INCRBY", k, num(rng)],
        3 => cmd!["DEL", k, key(rng)],
        4 => cmd!["EXISTS", k, key(rng)],
        5 => cmd!["TYPE", k],
        6 => cmd!["LPUSH", k, val(rng), val(rng)],
        7 => cmd!["RPUSH", k, val(rng)],
        8 => cmd!["LPOP", k],
        9 => cmd!["RPOP", k, num(rng)],
        10 => cmd!["LRANGE", k, num(rng), num(rng)],
        11 => cmd!["LINDEX", k, num(rng)],
        12 => cmd!["LREM", k, num(rng), val(rng)],
        13 => cmd!["LLEN", k],
        14 => cmd!["LTRIM", k, num(rng), num(rng)],
        15 => cmd!["SADD", k, val(rng), val(rng)],
        16 => cmd!["SREM", k, val(rng)],
        17 => cmd!["SISMEMBER", k, val(rng)],
        18 => cmd!["SMEMBERS", k],
        19 => cmd!["SCARD", k],
        20 => cmd!["HSET", k, val(rng), val(rng)],
        21 => cmd!["HGET", k, val(rng)],
        22 => cmd!["HINCRBY", k, val(rng), num(rng)],
        23 => cmd!["HDEL", k, val(rng)],
        24 => cmd!["HGETALL", k],
        25 => cmd!["HLEN", k],
        26 => cmd!["DECR", k],
        27 => cmd!["APPEND", k, val(rng)],
        28 => cmd!["RPOPLPUSH", k, key(rng)],
        29 => cmd!["EXPIRE", k, "1000"],
        30 => cmd!["TTL", k],
        31 => cmd!["KEYS", "k*"],
        32 => cmd![
            "EVAL",
            "local v = redis.call('LPOP', KEYS[1]) if v then redis.call('SADD', KEYS[2], v) end return {v, redis.call('SCARD', KEYS[2])}",
            "2",
            k,
            key(rng)
        ],
        33 => bpop(rng, "BLPOP", k),
        34 => bpop(rng, "BRPOP", k),
        _ => cmd!["SET", k, val(rng), "XX", "GET"],
    }
}

#[tokio::test]
async fn replies_match_real_redis() {
    let Some((_guard, mut real)) = real_redis().await else {
        eprintln!("SKIPPED: redis-server not found on PATH");
        return;
    };
    let (addr, server) = common::start().await;
    let mut ours = common::client(addr).await;

    let mut checked = 0;
    let mut mismatches = Vec::new();
    for c in edge_cases() {
        let (a, b) = both(&mut real, &mut ours, &c).await;
        checked += 1;
        if a != b {
            mismatches.push(format!("{c:?}\n    redis: {a:?}\n    ours:  {b:?}"));
        }
    }

    let seed = std::env::var("COMPAT_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(42);
    let mut rng = Rng::new(seed);
    for round in 0..common::iters(5000) {
        let c = random_command(&mut rng);
        let (a, b) = both(&mut real, &mut ours, &c).await;
        checked += 1;
        if a != b {
            mismatches.push(format!(
                "#{round} {c:?}\n    redis: {a:?}\n    ours:  {b:?}"
            ));
        }
        if mismatches.len() > 20 {
            break;
        }
    }
    server.check_invariants().unwrap();
    assert!(
        mismatches.is_empty(),
        "{} of {checked} replies differ from real Redis:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    println!("{checked} commands: every reply identical to real Redis");
}
