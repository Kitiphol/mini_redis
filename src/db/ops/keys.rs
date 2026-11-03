//! Commands on keys of any type: DEL, EXISTS, TYPE, KEYS, FLUSHDB, the TTL
//! family, plus PING and ECHO (allowed in transactions and scripts).

use super::{Reply, int, int_arg};
use crate::db::glob;
use crate::db::state::State;
use crate::error;
use crate::resp::Frame;
use bytes::Bytes;
use std::time::Duration;

/// `DEL key [key ...]` / `UNLINK`: number of keys removed.
pub fn del(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(int(a.iter().filter(|k| st.remove(k).is_some()).count()))
}

/// `EXISTS key [key ...]`: a key named twice counts twice.
pub fn exists(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(int(a.iter().filter(|k| st.contains(k)).count()))
}

/// `TYPE key`
pub fn type_of(st: &mut State, a: &[Bytes]) -> Reply {
    let name = st.get(&a[0]).map_or("none", |v| v.type_name());
    Ok(Frame::Simple(name.into()))
}

/// `KEYS pattern`
pub fn keys(st: &mut State, a: &[Bytes]) -> Reply {
    let mut found: Vec<Bytes> = st
        .keys()
        .into_iter()
        .filter(|k| glob::matches(&a[0], k))
        .collect();
    found.sort();
    Ok(Frame::Array(found.into_iter().map(Frame::Bulk).collect()))
}

/// `FLUSHDB [ASYNC|SYNC]`
pub fn flushdb(st: &mut State, a: &[Bytes]) -> Reply {
    match a {
        [] => {}
        [m] if m.eq_ignore_ascii_case(b"async") || m.eq_ignore_ascii_case(b"sync") => {}
        _ => return Err(error::syntax_error()),
    }
    st.flush();
    Ok(Frame::ok())
}

/// `EXPIRE key seconds [NX|XX|GT|LT]` / `PEXPIRE` (`unit_ms` = 1000 or 1).
pub fn expire(st: &mut State, a: &[Bytes], unit_ms: i64, name: &str) -> Reply {
    let amount = int_arg(&a[1])?;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for opt in &a[2..] {
        match opt.to_ascii_lowercase().as_slice() {
            b"nx" => nx = true,
            b"xx" => xx = true,
            b"gt" => gt = true,
            b"lt" => lt = true,
            _ => {
                return Err(Frame::error(format!(
                    "ERR Unsupported option {}",
                    String::from_utf8_lossy(opt)
                )));
            }
        }
    }
    if nx && (xx || gt || lt) {
        return Err(Frame::error(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(Frame::error(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }
    let Some(ms) = amount.checked_mul(unit_ms) else {
        return Err(error::invalid_expire(name));
    };
    if !st.contains(&a[0]) {
        return Ok(Frame::Integer(0));
    }
    let now = st.now();
    let new_at = if ms <= 0 {
        Some(now)
    } else {
        now.checked_add(Duration::from_millis(ms as u64))
    };
    let Some(new_at) = new_at else {
        return Err(error::invalid_expire(name));
    };
    let current = st.deadline(&a[0]);
    let allowed = match current {
        // No TTL counts as an infinite TTL for GT/LT, as in Redis.
        None => !xx && !gt,
        Some(cur) => !nx && (!gt || new_at > cur) && (!lt || new_at < cur),
    };
    if !allowed {
        return Ok(Frame::Integer(0));
    }
    st.set_deadline(&a[0], new_at);
    Ok(Frame::Integer(1))
}

/// `TTL key` / `PTTL key`: -2 missing, -1 no TTL, otherwise remaining time.
pub fn ttl(st: &mut State, a: &[Bytes], unit_ms: u128) -> Reply {
    if !st.contains(&a[0]) {
        return Ok(Frame::Integer(-2));
    }
    let Some(at) = st.deadline(&a[0]) else {
        return Ok(Frame::Integer(-1));
    };
    let ms = at.saturating_duration_since(st.now()).as_millis();
    // Redis rounds TTL to the nearest second.
    let value = if unit_ms == 1 { ms } else { (ms + 500) / 1000 };
    Ok(Frame::Integer(value as i64))
}

/// `PERSIST key`: 1 if a TTL was removed.
pub fn persist(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(Frame::Integer(st.persist(&a[0]) as i64))
}

/// `PING [message]`
pub fn ping(a: &[Bytes]) -> Frame {
    match a {
        [] => Frame::Simple("PONG".into()),
        [msg] => Frame::Bulk(msg.clone()),
        _ => error::wrong_arity("ping"),
    }
}
