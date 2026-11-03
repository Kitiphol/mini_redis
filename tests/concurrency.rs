//! Blocking pops under concurrency. These tests check *properties* — FIFO
//! order, exactly-once delivery, conservation of values — because thread
//! scheduling makes exact interleavings nondeterministic.

mod common;

use bytes::Bytes;
use common::Rng;
use micro_redis::resp::Frame;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiters_are_served_first_come_first_served() {
    let (addr, server) = common::start().await;
    let mut handles = Vec::new();
    for i in 0..5 {
        let mut c = common::client(addr).await;
        handles.push(tokio::spawn(
            async move { c.blpop(&["q"], 5.0).await.unwrap() },
        ));
        // Make sure client i is registered before client i+1 starts.
        common::wait_until("waiter registered", || server.total_waiters() == i + 1).await;
    }
    let mut p = common::client(addr).await;
    for i in 0..5 {
        p.rpush("q", &[format!("v{i}")]).await.unwrap();
    }
    for (i, h) in handles.into_iter().enumerate() {
        assert_eq!(
            h.await.unwrap(),
            Some((b("q"), b(&format!("v{i}")))),
            "waiter {i}"
        );
    }
    server.check_invariants().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn variadic_push_is_applied_whole_before_serving() {
    // Redis docs: with `BLPOP foo 0` blocked, `LPUSH foo a b c` delivers `c`.
    let (addr, server) = common::start().await;
    let mut w = common::client(addr).await;
    let h = tokio::spawn(async move { w.blpop(&["foo"], 0.0).await.unwrap() });
    common::wait_until("blocked", || server.total_waiters() == 1).await;
    let mut p = common::client(addr).await;
    assert_eq!(p.lpush("foo", &["a", "b", "c"]).await.unwrap(), 3);
    assert_eq!(h.await.unwrap(), Some((b("foo"), b("c"))));
    assert_eq!(
        p.blpop(&["foo"], 0.0).await.unwrap(),
        Some((b("foo"), b("b")))
    );
    assert_eq!(
        p.blpop(&["foo"], 0.0).await.unwrap(),
        Some((b("foo"), b("a")))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_push_can_serve_several_waiters() {
    let (addr, server) = common::start().await;
    let mut hs = Vec::new();
    for i in 0..2 {
        let mut w = common::client(addr).await;
        hs.push(tokio::spawn(
            async move { w.blpop(&["q"], 0.0).await.unwrap() },
        ));
        common::wait_until("blocked", || server.total_waiters() == i + 1).await;
    }
    let mut p = common::client(addr).await;
    assert_eq!(p.rpush("q", &["1", "2", "3"]).await.unwrap(), 3);
    assert_eq!(hs.remove(0).await.unwrap(), Some((b("q"), b("1"))));
    assert_eq!(hs.remove(0).await.unwrap(), Some((b("q"), b("2"))));
    assert_eq!(p.blpop(&["q"], 0.0).await.unwrap(), Some((b("q"), b("3"))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_waiter_gets_exactly_one_value() {
    let (addr, server) = common::start().await;
    for round in 0..common::iters(50) {
        let mut w = common::client(addr).await;
        let keys = ["k1", "k2", "k3", "k4", "k5"];
        let h = tokio::spawn(async move { w.blpop(&keys, 0.0).await.unwrap() });
        common::wait_until("blocked", || server.total_waiters() == 1).await;
        // Five concurrent pushes, one per key.
        let mut ps = Vec::new();
        for k in keys {
            let mut p = common::client(addr).await;
            ps.push(tokio::spawn(async move {
                p.rpush(k, &[format!("{k}-{round}")]).await.unwrap()
            }));
        }
        for p in ps {
            p.await.unwrap();
        }
        let (_, got) = h.await.unwrap().expect("served");
        let mut c = common::client(addr).await;
        let mut rest = Vec::new();
        while let Some((_, v)) = c.blpop(&keys, 0.001).await.unwrap() {
            rest.push(v);
        }
        assert_eq!(rest.len(), 4, "round {round}: exactly one value consumed");
        assert!(!rest.contains(&got), "round {round}: value delivered twice");
    }
    server.check_invariants().unwrap();
}

/// A client that has been waiting must beat a client that arrives while the
/// push is happening. Fails if the push releases the lock before serving
/// (feature `bug-split-serve`).
///
/// The newcomer doesn't send one BLPOP at a guessed moment; it keeps sending
/// very short BLPOPs from before the push until after it has completed. So
/// whatever the machine's timing, some attempt lands while the push is in
/// progress. On a correct server none of them can ever get the value: a push
/// hands its element to the waiting client before anyone else can look.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_client_is_never_overtaken_by_a_newcomer() {
    let (addr, server) = common::start().await;
    for round in 0..common::iters(40) {
        let mut w = common::client(addr).await;
        let waiter = tokio::spawn(async move { w.blpop(&["q"], 2.0).await.unwrap() });
        common::wait_until("blocked", || server.total_waiters() == 1).await;

        let mut p = common::client(addr).await;
        let mut n = common::client(addr).await;
        // The newcomer counts its attempts, so the test can make sure some
        // happen before the push starts and some after it has answered.
        let stop = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let (stop2, attempts2) = (Arc::clone(&stop), Arc::clone(&attempts));
        let newcomer = tokio::spawn(async move {
            while !stop2.load(Ordering::SeqCst) {
                if let Some(got) = n.blpop(&["q"], 0.0002).await.unwrap() {
                    return Some(got);
                }
                attempts2.fetch_add(1, Ordering::SeqCst);
            }
            None
        });
        common::wait_until("newcomer running", || attempts.load(Ordering::SeqCst) >= 3).await;

        let value = format!("v{round}");
        p.rpush("q", std::slice::from_ref(&value)).await.unwrap();
        let after = attempts.load(Ordering::SeqCst);
        // (A newcomer that overtook has already stopped: no need to wait.)
        common::wait_until("newcomer still running", || {
            attempts.load(Ordering::SeqCst) >= after + 3 || newcomer.is_finished()
        })
        .await;
        stop.store(true, Ordering::SeqCst);

        let overtook = newcomer.await.unwrap();
        assert_eq!(
            overtook, None,
            "round {round}: newcomer overtook the waiting client"
        );
        assert_eq!(
            waiter.await.unwrap(),
            Some((b("q"), b(&value))),
            "round {round}"
        );
    }
}

/// BLPOP and RPUSH racing: the BLPOP must always get the value, whichever
/// runs first. Fails with a "lost wake-up" if checking the keys and
/// registering as a waiter are two critical sections (`bug-split-register`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_lost_wakeups() {
    let (addr, server) = common::start().await;
    for round in 0..common::iters(300) {
        let mut w = common::client(addr).await;
        let mut p = common::client(addr).await;
        let waiter = tokio::spawn(async move { w.blpop(&["q"], 1.0).await.unwrap() });
        let value = format!("v{round}");
        let v2 = value.clone();
        // Sweep the push across 0 to 2.25 ms after the BLPOP is sent, so it
        // lands at every point of the BLPOP's own work, whatever the machine.
        let delay = Duration::from_micros(250 * (round as u64 % 10));
        let push = tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            p.rpush("q", &[v2]).await.unwrap()
        });
        push.await.unwrap();
        assert_eq!(
            waiter.await.unwrap(),
            Some((b("q"), b(&value))),
            "round {round}: BLPOP missed a push (lost wake-up)"
        );
    }
    server.check_invariants().unwrap();
}

/// Timeout racing a push: exactly one outcome, and the value is never lost
/// or duplicated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_vs_push_is_decided_exactly_once() {
    let (addr, server) = common::start().await;
    let (mut served, mut timed_out) = (0, 0);
    for round in 0..common::iters(300) {
        let mut w = common::client(addr).await;
        let mut p = common::client(addr).await;
        let waiter = tokio::spawn(async move { w.blpop(&["q"], 0.01).await.unwrap() });
        tokio::time::sleep(Duration::from_micros(9_000 + (round as u64 % 20) * 100)).await;
        p.rpush("q", &[format!("v{round}")]).await.unwrap();
        let got = waiter.await.unwrap();
        let left = p.blpop(&["q"], 0.001).await.unwrap();
        match (got, left) {
            (Some((_, v)), None) => {
                assert_eq!(v, b(&format!("v{round}")));
                served += 1;
            }
            (None, Some((_, v))) => {
                assert_eq!(v, b(&format!("v{round}")));
                timed_out += 1;
            }
            other => panic!("round {round}: value lost or duplicated: {other:?}"),
        }
    }
    assert_eq!(server.total_waiters(), 0);
    server.check_invariants().unwrap();
    println!("deadline race: served={served} timed_out={timed_out}");
}

/// Hang up a blocked raw connection the way a client that quits does: close
/// our sending side, then read whatever the server still sends until it
/// closes. Returns the value if the server had already served us.
///
/// (Closing only the sending side keeps the test's accounting exact: if the
/// server served us first, we can still read the reply. A client that slams
/// the socket shut loses that value — exactly as with real Redis.)
async fn hang_up(mut raw: TcpStream) -> Option<Bytes> {
    let _ = raw.shutdown().await;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), raw.read_to_end(&mut buf)).await;
    match micro_redis::resp::parse_frame(&buf) {
        Ok(Some((Frame::Array(items), _))) => match items.as_slice() {
            [Frame::Bulk(_), Frame::Bulk(v)] => Some(v.clone()),
            other => panic!("unexpected reply {other:?}"),
        },
        Ok(None) => None,
        other => panic!("unexpected bytes {other:?}"),
    }
}

/// A blocked client disconnecting while a push arrives: the value ends up in
/// exactly one place — delivered to that client (push won), still in the list
/// (disconnect won), or counted as dropped (served after the server already
/// saw the hang-up) — never two, never none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_vs_push_is_decided_exactly_once() {
    let (addr, server) = common::start().await;
    let (mut delivered, mut kept, mut dropped) = (0, 0, 0);
    for round in 0..common::iters(200) {
        let lost_before = server.lost_deliveries();
        let mut raw = common::raw(addr).await;
        raw.write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n")
            .await
            .unwrap();
        common::wait_until("blocked", || server.total_waiters() == 1).await;

        let mut p = common::client(addr).await;
        let hang = tokio::spawn(async move {
            if round % 2 == 0 {
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            hang_up(raw).await
        });
        p.rpush("q", &[format!("v{round}")]).await.unwrap();
        let got = hang.await.unwrap();
        common::wait_until("server done with client", || server.total_waiters() == 0).await;

        let in_list = p.blpop(&["q"], 0.001).await.unwrap().is_some() as u64;
        let lost = server.lost_deliveries() - lost_before;
        let places = got.is_some() as u64 + in_list + lost;
        assert_eq!(
            places, 1,
            "round {round}: delivered={got:?} in_list={in_list} lost={lost}"
        );
        delivered += got.is_some() as u32;
        kept += in_list;
        dropped += lost;
    }
    server.check_invariants().unwrap();
    println!("disconnect race: delivered={delivered} kept_in_list={kept} dropped={dropped}");
}

/// The big one. Many pushers and many blocked poppers (BLPOP and BRPOP on
/// random key subsets, random short timeouts, some clients that hang up
/// mid-wait). Afterwards:
///
///   received + still-in-lists + lost-with-disconnected-clients == pushed
///
/// with no value ever seen twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn conservation_under_stress() {
    for run in 0..common::iters(3) {
        let (addr, server) = common::start().await;
        const PUSHERS: usize = 6;
        const PER_PUSHER: usize = 300;
        const POPPERS: usize = 6;
        let keys = ["a", "b", "c"];
        let done = Arc::new(AtomicBool::new(false));

        let mut pushers = Vec::new();
        for p in 0..PUSHERS {
            pushers.push(tokio::spawn(async move {
                let mut rng = Rng::new((run * 100 + p) as u64);
                let mut c = common::client(addr).await;
                let mut i = 0;
                while i < PER_PUSHER {
                    let n = (1 + rng.below(3) as usize).min(PER_PUSHER - i);
                    let vals: Vec<String> = (i..i + n).map(|j| format!("p{p}-{j}")).collect();
                    let key = *rng.pick(&keys);
                    if rng.chance(50) {
                        c.lpush(key, &vals).await.unwrap();
                    } else {
                        c.rpush(key, &vals).await.unwrap();
                    }
                    i += n;
                    if rng.chance(20) {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        let mut poppers = Vec::new();
        for q in 0..POPPERS {
            let done = Arc::clone(&done);
            poppers.push(tokio::spawn(async move {
                let mut rng = Rng::new((run * 1000 + q + 50) as u64);
                let mut c = common::client(addr).await;
                let mut got: Vec<Bytes> = Vec::new();
                while !done.load(Ordering::SeqCst) {
                    let mut ks: Vec<&str> = keys.to_vec();
                    for i in (1..ks.len()).rev() {
                        ks.swap(i, rng.below(i as u64 + 1) as usize);
                    }
                    ks.truncate(1 + rng.below(3) as usize);
                    if rng.chance(8) {
                        // Hang up while (probably) blocked.
                        let mut raw = common::raw(addr).await;
                        let cmd = format!(
                            "*{}\r\n$5\r\nBLPOP\r\n{}$1\r\n0\r\n",
                            ks.len() + 2,
                            ks.iter()
                                .map(|k| format!("$1\r\n{k}\r\n"))
                                .collect::<String>()
                        );
                        raw.write_all(cmd.as_bytes()).await.unwrap();
                        tokio::time::sleep(Duration::from_micros(rng.below(3000))).await;
                        got.extend(hang_up(raw).await);
                        continue;
                    }
                    let timeout = 0.001 + rng.below(30) as f64 / 1000.0;
                    let r = if rng.chance(50) {
                        c.blpop(&ks, timeout).await.unwrap()
                    } else {
                        c.brpop(&ks, timeout).await.unwrap()
                    };
                    if let Some((_, v)) = r {
                        got.push(v);
                    }
                }
                got
            }));
        }

        for p in pushers {
            p.await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        done.store(true, Ordering::SeqCst);
        let mut received: Vec<Bytes> = Vec::new();
        for p in poppers {
            received.extend(p.await.unwrap());
        }
        // Let the server finish with every hung-up client.
        common::wait_until("no waiters left", || server.total_waiters() == 0).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut c = common::client(addr).await;
        let mut remaining = Vec::new();
        while let Some((_, v)) = c.blpop(&keys, 0.001).await.unwrap() {
            remaining.push(v);
        }

        let pushed: HashSet<Bytes> = (0..PUSHERS)
            .flat_map(|p| (0..PER_PUSHER).map(move |j| b(&format!("p{p}-{j}"))))
            .collect();
        let mut seen = HashSet::new();
        for v in received.iter().chain(remaining.iter()) {
            assert!(
                pushed.contains(v),
                "run {run}: value {v:?} was never pushed"
            );
            assert!(
                seen.insert(v.clone()),
                "run {run}: value {v:?} delivered twice"
            );
        }
        let lost = server.lost_deliveries() as usize;
        assert_eq!(
            received.len() + remaining.len() + lost,
            pushed.len(),
            "run {run}: received={} remaining={} lost={lost}",
            received.len(),
            remaining.len()
        );
        server.check_invariants().unwrap();
        println!(
            "conservation run {run}: pushed={} received={} remaining={} lost_to_disconnects={lost} duplicates=0",
            pushed.len(),
            received.len(),
            remaining.len()
        );
    }
}
