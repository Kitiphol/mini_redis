use super::*;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn keys(ks: &[&str]) -> Vec<Bytes> {
    ks.iter().map(|k| b(k)).collect()
}

fn block(db: &Db, ks: &[&str], end: End) -> WaitTicket {
    match db.pop_or_register(&keys(ks), end).expect("not wrongtype") {
        PopOutcome::Blocked(t) => t,
        PopOutcome::Ready(p) => panic!("expected to block, popped {p:?}"),
    }
}

fn got(t: &mut WaitTicket) -> Option<Delivery> {
    t.rx.try_recv().ok()
}

#[test]
fn strings_and_wrongtype() {
    let db = Db::new();
    assert_eq!(db.get(b"k"), Ok(None));
    db.set(b("k"), b("v"));
    assert_eq!(db.get(b"k"), Ok(Some(b("v"))));
    assert_eq!(
        db.push(b("k"), vec![b("x")], End::Left),
        Err(DbError::WrongType)
    );
    db.push(b("l"), vec![b("x")], End::Left).unwrap();
    assert_eq!(db.get(b"l"), Err(DbError::WrongType));
    db.set(b("l"), b("now a string"));
    assert_eq!(db.get(b"l"), Ok(Some(b("now a string"))));
    assert_eq!(db.exists(&keys(&["k", "k", "l", "nope"])), 3);
    db.check_invariants().unwrap();
}

#[test]
fn push_order_and_lengths() {
    let db = Db::new();
    assert_eq!(db.push(b("l"), keys(&["a", "b", "c"]), End::Left), Ok(3));
    assert_eq!(db.push(b("l"), keys(&["d"]), End::Right), Ok(4));
    // list is now c b a d
    let pop = |end| match db.pop_or_register(&keys(&["l"]), end).unwrap() {
        PopOutcome::Ready((_, v)) => v,
        PopOutcome::Blocked(_) => panic!("blocked"),
    };
    assert_eq!(pop(End::Left), b("c"));
    assert_eq!(pop(End::Right), b("d"));
    assert_eq!(pop(End::Left), b("b"));
    assert_eq!(pop(End::Left), b("a"));
    assert_eq!(db.exists(&keys(&["l"])), 0, "emptied list is deleted");
    db.check_invariants().unwrap();
}

#[test]
fn first_non_empty_key_wins_without_blocking() {
    let db = Db::new();
    db.push(b("k2"), keys(&["x"]), End::Right).unwrap();
    db.push(b("k3"), keys(&["y"]), End::Right).unwrap();
    match db
        .pop_or_register(&keys(&["k1", "k2", "k3"]), End::Left)
        .unwrap()
    {
        PopOutcome::Ready(p) => assert_eq!(p, (b("k2"), b("x"))),
        PopOutcome::Blocked(_) => panic!("should not block"),
    }
    assert_eq!(db.waiter_count(), 0);
}

#[test]
fn string_key_in_scan_is_wrongtype() {
    let db = Db::new();
    db.set(b("s"), b("v"));
    assert!(matches!(
        db.pop_or_register(&keys(&["s"]), End::Left),
        Err(DbError::WrongType)
    ));
    // A non-empty list before the string is popped first, as in Redis.
    db.push(b("l"), keys(&["x"]), End::Right).unwrap();
    assert!(matches!(
        db.pop_or_register(&keys(&["l", "s"]), End::Left),
        Ok(PopOutcome::Ready(_))
    ));
}

#[test]
fn push_hands_off_fifo_inside_the_push() {
    let db = Db::new();
    let mut a = block(&db, &["q"], End::Left);
    let mut b2 = block(&db, &["q"], End::Left);
    let mut c = block(&db, &["q"], End::Left);
    db.check_invariants().unwrap();
    assert_eq!(db.waiter_count(), 3);

    // Returned length is measured before serving.
    assert_eq!(db.push(b("q"), keys(&["1", "2"]), End::Right), Ok(2));
    // Delivered *during* the push: already in the receivers.
    assert_eq!(got(&mut a), Some(Ok((b("q"), b("1")))));
    assert_eq!(got(&mut b2), Some(Ok((b("q"), b("2")))));
    assert_eq!(got(&mut c), None);
    assert_eq!(db.exists(&keys(&["q"])), 0);
    assert_eq!(db.waiter_count(), 1);
    db.check_invariants().unwrap();
}

#[test]
fn variadic_lpush_serves_after_the_whole_push() {
    // Redis docs: with `BLPOP foo 0` waiting, `LPUSH foo a b c` gives `c`.
    let db = Db::new();
    let mut w = block(&db, &["foo"], End::Left);
    assert_eq!(db.push(b("foo"), keys(&["a", "b", "c"]), End::Left), Ok(3));
    assert_eq!(got(&mut w), Some(Ok((b("foo"), b("c")))));
    // Remaining list: b a
    match db.pop_or_register(&keys(&["foo"]), End::Left).unwrap() {
        PopOutcome::Ready(p) => assert_eq!(p.1, b("b")),
        PopOutcome::Blocked(_) => panic!(),
    }
}

#[test]
fn brpop_waiter_takes_from_the_right() {
    let db = Db::new();
    let mut w = block(&db, &["q"], End::Right);
    db.push(b("q"), keys(&["a", "b", "c"]), End::Right).unwrap();
    assert_eq!(got(&mut w), Some(Ok((b("q"), b("c")))));
}

#[test]
fn multi_key_waiter_is_served_exactly_once() {
    let db = Db::new();
    let mut w = block(&db, &["k1", "k2", "k3", "k4", "k5"], End::Left);
    db.push(b("k3"), keys(&["x"]), End::Right).unwrap();
    db.push(b("k1"), keys(&["y"]), End::Right).unwrap();
    assert_eq!(got(&mut w), Some(Ok((b("k3"), b("x")))));
    assert_eq!(db.waiter_count(), 0);
    // The second push found no waiter: its value stays.
    assert_eq!(db.exists(&keys(&["k1", "k3"])), 1);
    db.check_invariants().unwrap();
}

#[test]
fn duplicate_keys_queue_once() {
    let db = Db::new();
    let mut w = block(&db, &["q", "q"], End::Left);
    db.check_invariants().unwrap();
    db.push(b("q"), keys(&["a", "b"]), End::Right).unwrap();
    assert_eq!(got(&mut w), Some(Ok((b("q"), b("a")))));
    assert_eq!(db.exists(&keys(&["q"])), 1);
}

#[test]
fn cancel_vs_serve_exactly_one_wins() {
    let db = Db::new();
    let w = block(&db, &["q"], End::Left);
    assert_eq!(db.cancel(w.id), Cancel::Cancelled);
    assert_eq!(db.cancel(w.id), Cancel::AlreadyServed); // idempotent
    db.push(b("q"), keys(&["v"]), End::Right).unwrap();
    assert_eq!(db.exists(&keys(&["q"])), 1, "cancelled waiter took nothing");

    let db = Db::new();
    let mut w = block(&db, &["q"], End::Left);
    db.push(b("q"), keys(&["v"]), End::Right).unwrap();
    assert_eq!(db.cancel(w.id), Cancel::AlreadyServed);
    assert_eq!(
        got(&mut w),
        Some(Ok((b("q"), b("v")))),
        "value waits in receiver"
    );
    db.check_invariants().unwrap();
}

#[test]
fn dead_receiver_puts_value_back_and_next_waiter_gets_it() {
    let db = Db::new();
    let dead = block(&db, &["q"], End::Left);
    let mut live = block(&db, &["q"], End::Left);
    drop(dead.rx); // task vanished without cancelling
    db.push(b("q"), keys(&["v"]), End::Right).unwrap();
    assert_eq!(got(&mut live), Some(Ok((b("q"), b("v")))));
    assert_eq!(db.waiter_count(), 0);

    let dead = block(&db, &["q"], End::Left);
    drop(dead.rx);
    db.push(b("q"), keys(&["v2"]), End::Right).unwrap();
    assert_eq!(db.exists(&keys(&["q"])), 1, "nobody alive: value stays");
    db.check_invariants().unwrap();
}

#[test]
fn earlier_key_turned_string_gives_wrongtype_like_redis() {
    let db = Db::new();
    let mut w = block(&db, &["a", "b"], End::Left);
    db.set(b("a"), b("str"));
    db.push(b("b"), keys(&["x"]), End::Right).unwrap();
    assert_eq!(got(&mut w), Some(Err(DbError::WrongType)));
    assert_eq!(
        db.exists(&keys(&["b"])),
        1,
        "nothing popped for the errored client"
    );
    db.check_invariants().unwrap();
}

#[test]
fn databases_are_independent() {
    let d0 = Db::new();
    let d1 = Db::new();
    let mut w = block(&d1, &["q"], End::Left);
    d0.push(b("q"), keys(&["x"]), End::Right).unwrap();
    assert_eq!(got(&mut w), None);
    assert_eq!(d0.exists(&keys(&["q"])), 1);
}
