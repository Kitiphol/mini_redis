//! Linearizability checking.
//!
//! Several clients run random operations concurrently. Each operation is
//! recorded with the time it was *invoked* (just before sending) and the time
//! it *completed* (just after the reply arrived). A Wing & Gong style search
//! then looks for one sequential order of all operations that
//!
//!   1. respects real time: if A completed before B was invoked, A comes first;
//!   2. reproduces every observed reply when run on a simple sequential model
//!      of the spec (a single-threaded HashMap).
//!
//! If no such order exists the history is not linearizable and the test
//! prints it. Because recorded intervals are slightly *wider* than the
//! server's real ones, the check can never report a false violation.
//!
//! Clients may work on different databases: the model keeps one keyspace
//! per database, so a value leaking from one database into another, or a
//! push on one database serving a waiter on another, makes a history fail.
//!
//! Blocking pops fit the model directly: a served BLPOP is a pop at the
//! moment it was served; a BLPOP that timed out is "all keys were empty" at
//! some moment during its wait.

mod common;

use bytes::Bytes;
use common::Rng;
use micro_redis::resp::Frame;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
enum Op {
    Set(&'static str, String),
    Get(&'static str),
    Exists(Vec<&'static str>),
    Push {
        left: bool,
        key: &'static str,
        vals: Vec<String>,
    },
    BPop {
        left: bool,
        keys: Vec<&'static str>,
        timeout: f64,
    },
}

impl Op {
    fn args(&self) -> Vec<String> {
        let s = |x: &str| x.to_string();
        match self {
            Op::Set(k, v) => vec![s("SET"), s(k), v.clone()],
            Op::Get(k) => vec![s("GET"), s(k)],
            Op::Exists(ks) => std::iter::once(s("EXISTS"))
                .chain(ks.iter().map(|k| s(k)))
                .collect(),
            Op::Push { left, key, vals } => {
                let mut a = vec![s(if *left { "LPUSH" } else { "RPUSH" }), s(key)];
                a.extend(vals.iter().cloned());
                a
            }
            Op::BPop {
                left,
                keys,
                timeout,
            } => {
                let mut a = vec![s(if *left { "BLPOP" } else { "BRPOP" })];
                a.extend(keys.iter().map(|k| s(k)));
                a.push(timeout.to_string());
                a
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Event {
    client: usize,
    /// Database the client selected (0 = never sent SELECT).
    db: usize,
    op: Op,
    invoked: Duration,
    completed: Duration,
    result: Frame,
}

// ---- the sequential model of the spec ---------------------------------------

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum MVal {
    Str(String),
    List(VecDeque<String>),
}

/// One keyspace per database: keys are `(database, key)`.
type Model = BTreeMap<(usize, &'static str), MVal>;

fn wrongtype() -> Frame {
    Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

fn bulk(s: &str) -> Frame {
    Frame::Bulk(Bytes::copy_from_slice(s.as_bytes()))
}

/// Apply `op` on database `db` of the model; return the reply the spec requires.
fn apply(m: &mut Model, db: usize, op: &Op) -> Frame {
    match op {
        Op::Set(k, v) => {
            m.insert((db, k), MVal::Str(v.clone()));
            Frame::ok()
        }
        Op::Get(k) => match m.get(&(db, *k)) {
            None => Frame::Null,
            Some(MVal::Str(v)) => bulk(v),
            Some(MVal::List(_)) => wrongtype(),
        },
        Op::Exists(ks) => {
            Frame::Integer(ks.iter().filter(|k| m.contains_key(&(db, **k))).count() as i64)
        }
        Op::Push { left, key, vals } => {
            let entry = m
                .entry((db, key))
                .or_insert_with(|| MVal::List(VecDeque::new()));
            let MVal::List(l) = entry else {
                return wrongtype();
            };
            for v in vals {
                if *left {
                    l.push_front(v.clone())
                } else {
                    l.push_back(v.clone())
                }
            }
            Frame::Integer(l.len() as i64)
        }
        Op::BPop { left, keys, .. } => {
            for k in keys {
                match m.get_mut(&(db, *k)) {
                    None => continue,
                    Some(MVal::Str(_)) => return wrongtype(),
                    Some(MVal::List(l)) => {
                        let v = if *left { l.pop_front() } else { l.pop_back() };
                        if l.is_empty() {
                            m.remove(&(db, *k));
                        }
                        if let Some(v) = v {
                            return Frame::Array(vec![bulk(k), bulk(&v)]);
                        }
                    }
                }
            }
            Frame::NullArray
        }
    }
}

// ---- the checker ------------------------------------------------------------

/// Is there a valid linearization of `h`? (Wing & Gong search + memoization.)
fn linearizable(h: &[Event]) -> bool {
    assert!(h.len() <= 64);
    type Seen = HashSet<(u64, Vec<((usize, &'static str), MVal)>)>;
    let mut failed: Seen = HashSet::new();
    fn go(h: &[Event], done: u64, model: &Model, failed: &mut Seen) -> bool {
        if done.count_ones() as usize == h.len() {
            return true;
        }
        let key = (
            done,
            model
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect::<Vec<_>>(),
        );
        if failed.contains(&key) {
            return false;
        }
        // Earliest completion among pending ops: anything invoked after it
        // cannot go next.
        let horizon = (0..h.len())
            .filter(|i| done & (1 << i) == 0)
            .map(|i| h[i].completed)
            .min()
            .unwrap_or(Duration::MAX);
        for i in 0..h.len() {
            if done & (1 << i) != 0 || h[i].invoked > horizon {
                continue;
            }
            let mut m = model.clone();
            if apply(&mut m, h[i].db, &h[i].op) == h[i].result && go(h, done | (1 << i), &m, failed)
            {
                return true;
            }
        }
        failed.insert(key);
        false
    }
    go(h, 0, &Model::new(), &mut failed)
}

fn random_op(rng: &mut Rng, client: usize, i: usize) -> Op {
    let val = format!("c{client}.{i}");
    let lists = ["a", "b"];
    match rng.below(100) {
        0..=14 => Op::Set("s", val),
        15..=29 => Op::Get("s"),
        30..=34 => Op::Set("a", val), // occasional type change: exercises WRONGTYPE
        35..=44 => {
            let mut ks = vec![*rng.pick(&["s", "a", "b"])];
            if rng.chance(50) {
                ks.push(*rng.pick(&["s", "a", "b"]));
            }
            Op::Exists(ks)
        }
        45..=69 => Op::Push {
            left: rng.chance(50),
            key: rng.pick(&lists),
            vals: (0..1 + rng.below(2))
                .map(|j| format!("{val}.{j}"))
                .collect(),
        },
        _ => {
            let keys = if rng.chance(50) {
                vec![*rng.pick(&lists)]
            } else if rng.chance(50) {
                vec!["a", "b"]
            } else {
                vec!["b", "a"]
            };
            Op::BPop {
                left: rng.chance(50),
                keys,
                timeout: 0.005 + rng.below(20) as f64 / 1000.0,
            }
        }
    }
}

/// Run `clients` concurrent clients, client `c` on database
/// `dbs[c % dbs.len()]` (database 0 without sending SELECT, as a client that
/// relies on the default would), and record what each one saw.
async fn record_history(
    seed: u64,
    clients: usize,
    ops_per_client: usize,
    dbs: &'static [usize],
) -> Vec<Event> {
    let (addr, server) = common::start().await;
    let origin = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let db = dbs[c % dbs.len()];
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng::new(seed * 31 + c as u64);
            let mut client = common::client(addr).await;
            if db != 0 {
                client.select(db).await.unwrap();
            }
            let mut events = Vec::new();
            for i in 0..ops_per_client {
                let op = random_op(&mut rng, c, i);
                let invoked = origin.elapsed();
                let result = client.cmd(&op.args()).await.unwrap();
                let completed = origin.elapsed();
                events.push(Event {
                    client: c,
                    db,
                    op,
                    invoked,
                    completed,
                    result,
                });
                if rng.chance(30) {
                    tokio::time::sleep(Duration::from_micros(rng.below(2000))).await;
                }
            }
            events
        }));
    }
    let mut h = Vec::new();
    for t in tasks {
        h.extend(t.await.unwrap());
    }
    server.check_invariants().unwrap();
    h
}

fn print_history(h: &[Event]) {
    let mut h = h.to_vec();
    h.sort_by_key(|e| e.invoked);
    for e in h {
        println!(
            "  client {} db {:<2} [{:>8.3} ms .. {:>8.3} ms] {:<40} -> {:?}",
            e.client,
            e.db,
            e.invoked.as_secs_f64() * 1e3,
            e.completed.as_secs_f64() * 1e3,
            e.op.args().join(" "),
            e.result
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_histories_are_linearizable() {
    let n = common::iters(250);
    for seed in 0..n as u64 {
        let h = record_history(seed, 4, 5, &[0]).await;
        if !linearizable(&h) {
            println!("NOT LINEARIZABLE (seed {seed}):");
            print_history(&h);
            panic!("history {seed} is not linearizable");
        }
    }
    println!("{n} random histories of 20 operations: all linearizable");
}

/// Bigger histories spread over three databases at once: 6 clients, two per
/// database, the database-0 pair never sending SELECT. The same key names are
/// used on every database, so the checker fails if a value, a push or a
/// wake-up crosses from one database to another, as well as for any ordering
/// violation inside a database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_database_histories_are_linearizable() {
    let n = common::iters(150);
    for seed in 0..n as u64 {
        let h = record_history(10_000 + seed, 6, 5, &[0, 7, 15]).await;
        if !linearizable(&h) {
            println!("NOT LINEARIZABLE (seed {seed}):");
            print_history(&h);
            panic!("multi-database history {seed} is not linearizable");
        }
    }
    println!("{n} random histories of 30 operations on 3 databases: all linearizable");
}

/// The checker itself must reject bad histories, or passing means nothing.
#[test]
fn checker_rejects_impossible_histories() {
    let ms = Duration::from_millis;
    let ev = |client, op, a, b, result| Event {
        client,
        db: 0,
        op,
        invoked: ms(a),
        completed: ms(b),
        result,
    };
    // SET completes, then a later GET misses it: stale read.
    let stale = vec![
        ev(0, Op::Set("s", "1".into()), 0, 1, Frame::ok()),
        ev(1, Op::Get("s"), 2, 3, Frame::Null),
    ];
    assert!(!linearizable(&stale));
    // Same but overlapping in time: either order is fine.
    let overlap = vec![
        ev(0, Op::Set("s", "1".into()), 0, 3, Frame::ok()),
        ev(1, Op::Get("s"), 1, 2, Frame::Null),
    ];
    assert!(linearizable(&overlap));
    // One pushed value delivered to two BLPOPs.
    let dup = vec![
        ev(
            0,
            Op::Push {
                left: false,
                key: "a",
                vals: vec!["x".into()],
            },
            0,
            1,
            Frame::Integer(1),
        ),
        ev(
            1,
            Op::BPop {
                left: true,
                keys: vec!["a"],
                timeout: 1.0,
            },
            0,
            2,
            Frame::Array(vec![bulk("a"), bulk("x")]),
        ),
        ev(
            2,
            Op::BPop {
                left: true,
                keys: vec!["a"],
                timeout: 1.0,
            },
            0,
            2,
            Frame::Array(vec![bulk("a"), bulk("x")]),
        ),
    ];
    assert!(!linearizable(&dup));
    // BLPOP timed out although the list was non-empty for its whole wait.
    let missed = vec![
        ev(
            0,
            Op::Push {
                left: false,
                key: "a",
                vals: vec!["x".into()],
            },
            0,
            1,
            Frame::Integer(1),
        ),
        ev(
            1,
            Op::BPop {
                left: true,
                keys: vec!["a"],
                timeout: 0.01,
            },
            2,
            12,
            Frame::NullArray,
        ),
    ];
    assert!(!linearizable(&missed));

    // Databases: the same key name on two databases is two different keys.
    let on = |db, e: Event| Event { db, ..e };
    // A SET on database 0, then a GET on database 3 sees it: a leak.
    let leak = vec![
        on(0, ev(0, Op::Set("s", "1".into()), 0, 1, Frame::ok())),
        on(3, ev(1, Op::Get("s"), 2, 3, bulk("1"))),
    ];
    assert!(!linearizable(&leak));
    // The correct version: database 3 doesn't have it.
    let isolated = vec![
        on(0, ev(0, Op::Set("s", "1".into()), 0, 1, Frame::ok())),
        on(3, ev(1, Op::Get("s"), 2, 3, Frame::Null)),
    ];
    assert!(linearizable(&isolated));
    // A push on database 0 serves a BLPOP waiting on database 3: a leak.
    let cross_wake = vec![
        on(
            3,
            ev(
                0,
                Op::BPop {
                    left: true,
                    keys: vec!["a"],
                    timeout: 1.0,
                },
                0,
                5,
                Frame::Array(vec![bulk("a"), bulk("x")]),
            ),
        ),
        on(
            0,
            ev(
                1,
                Op::Push {
                    left: false,
                    key: "a",
                    vals: vec!["x".into()],
                },
                1,
                2,
                Frame::Integer(1),
            ),
        ),
    ];
    assert!(!linearizable(&cross_wake));
}

/// Sequential cross-client read-after-write: the simplest linearizability
/// property. Fails if the server replies before applying a write
/// (feature `bug-reply-before-apply`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_after_acknowledged_write_from_another_client() {
    let (addr, _) = common::start().await;
    let mut writer = common::client(addr).await;
    let mut reader = common::client(addr).await;
    for i in 0..common::iters(200) {
        writer.set("k", i.to_string()).await.unwrap();
        let got = reader.get("k").await.unwrap();
        assert_eq!(
            got,
            Some(Bytes::from(i.to_string())),
            "write {i} acknowledged but not visible"
        );
    }
}

/// Many readers and writers on one key at full speed; each reader must never
/// go back in time (values are increasing per writer).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn readers_never_see_time_go_backwards() {
    let (addr, _) = common::start().await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut w = common::client(addr).await;
    let writer = {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut i: u64 = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                i += 1;
                w.set("counter", i.to_string()).await.unwrap();
            }
            i
        })
    };
    let mut readers = Vec::new();
    for _ in 0..6 {
        let mut r = common::client(addr).await;
        readers.push(tokio::spawn(async move {
            let mut last = 0u64;
            for _ in 0..2000 {
                if let Some(v) = r.get("counter").await.unwrap() {
                    let n: u64 = std::str::from_utf8(&v).unwrap().parse().unwrap();
                    assert!(n >= last, "read {n} after {last}");
                    last = n;
                }
            }
        }));
    }
    for r in readers {
        r.await.unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(writer.await.unwrap() > 0);
}
