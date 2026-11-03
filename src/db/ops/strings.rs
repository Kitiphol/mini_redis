//! String commands: GET, SET (with options), SETEX, SETNX, GETSET, GETDEL,
//! MGET, MSET, the INCR family, APPEND, STRLEN.

use super::{Reply, bulk_or_nil, int, int_arg, wrong};
use crate::db::state::State;
use crate::db::value::Value;
use crate::error;
use crate::resp::{Frame, parse_i64};
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// `GET key`
pub fn get(st: &mut State, a: &[Bytes]) -> Reply {
    st.string(&a[0]).map(bulk_or_nil).map_err(wrong)
}

/// When a SET-like command should make the key expire.
enum Expiry {
    Keep,
    Clear,
    At(Instant),
}

/// `SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]`
pub fn set(st: &mut State, a: &[Bytes]) -> Reply {
    let (key, value) = (&a[0], &a[1]);
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut expiry = Expiry::Clear;
    let mut expiry_given = false;
    let mut i = 2;
    while i < a.len() {
        let opt = a[i].to_ascii_lowercase();
        match opt.as_slice() {
            b"nx" if !xx => nx = true,
            b"xx" if !nx => xx = true,
            b"get" => get = true,
            b"keepttl" if !expiry_given => {
                expiry = Expiry::Keep;
                expiry_given = true;
            }
            b"ex" | b"px" | b"exat" | b"pxat" if !expiry_given && i + 1 < a.len() => {
                let n = int_arg(&a[i + 1])?;
                expiry = Expiry::At(deadline(st.now(), opt.as_slice(), n, "set")?);
                expiry_given = true;
                i += 1;
            }
            _ => return Err(error::syntax_error()),
        }
        i += 1;
    }

    let old = match st.get(key) {
        None => None,
        Some(Value::Str(s)) => Some(s.clone()),
        Some(_) if get => return Err(wrong(crate::db::DbError::WrongType)),
        Some(_) => Some(Bytes::new()), // exists, not a string: fine for plain SET
    };
    let exists = old.is_some();
    if (nx && exists) || (xx && !exists) {
        return Ok(if get {
            bulk_or_nil(old.as_ref())
        } else {
            Frame::Null
        });
    }
    match expiry {
        Expiry::Keep => st.insert_keep_ttl(key.clone(), Value::Str(value.clone())),
        Expiry::Clear => st.insert(key.clone(), Value::Str(value.clone())),
        Expiry::At(at) => {
            st.insert(key.clone(), Value::Str(value.clone()));
            st.set_deadline(key, at);
        }
    }
    Ok(if get {
        bulk_or_nil(old.as_ref())
    } else {
        Frame::ok()
    })
}

/// Turn an EX/PX/EXAT/PXAT amount into a deadline.
fn deadline(now: Instant, kind: &[u8], n: i64, cmd: &str) -> Result<Instant, Frame> {
    if n <= 0 {
        return Err(error::invalid_expire(cmd));
    }
    let n = n as u64;
    let after = match kind {
        b"ex" => n.checked_mul(1000).map(Duration::from_millis),
        b"px" => Some(Duration::from_millis(n)),
        b"exat" | b"pxat" => {
            let target_ms = if kind == b"exat" {
                n.checked_mul(1000)
            } else {
                Some(n)
            };
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            // A time in the past is accepted: the key expires immediately.
            target_ms.map(|t| Duration::from_millis(t.saturating_sub(now_ms)))
        }
        _ => None,
    };
    after
        .and_then(|d| now.checked_add(d))
        .ok_or_else(|| error::invalid_expire(cmd))
}

/// `SETEX key seconds value` / `PSETEX key ms value`
pub fn setex(st: &mut State, a: &[Bytes], unit_ms: i64, cmd: &str) -> Reply {
    let n = int_arg(&a[1])?;
    let kind: &[u8] = if unit_ms == 1 { b"px" } else { b"ex" };
    let at = deadline(st.now(), kind, n, cmd)?;
    st.insert(a[0].clone(), Value::Str(a[2].clone()));
    st.set_deadline(&a[0], at);
    Ok(Frame::ok())
}

/// `SETNX key value`: 1 if set.
pub fn setnx(st: &mut State, a: &[Bytes]) -> Reply {
    if st.contains(&a[0]) {
        return Ok(Frame::Integer(0));
    }
    st.insert(a[0].clone(), Value::Str(a[1].clone()));
    Ok(Frame::Integer(1))
}

/// `GETSET key value`
pub fn getset(st: &mut State, a: &[Bytes]) -> Reply {
    let old = st.string(&a[0]).map_err(wrong)?.cloned();
    st.insert(a[0].clone(), Value::Str(a[1].clone()));
    Ok(bulk_or_nil(old.as_ref()))
}

/// `GETDEL key`
pub fn getdel(st: &mut State, a: &[Bytes]) -> Reply {
    let old = st.string(&a[0]).map_err(wrong)?.cloned();
    if old.is_some() {
        st.remove(&a[0]);
    }
    Ok(bulk_or_nil(old.as_ref()))
}

/// `MGET key [key ...]`: non-strings read as nil (no error), as in Redis.
pub fn mget(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(Frame::Array(
        a.iter()
            .map(|k| match st.get(k) {
                Some(Value::Str(s)) => Frame::Bulk(s.clone()),
                _ => Frame::Null,
            })
            .collect(),
    ))
}

/// `MSET key value [key value ...]`
pub fn mset(st: &mut State, a: &[Bytes]) -> Reply {
    if !a.len().is_multiple_of(2) {
        return Err(error::wrong_arity("mset"));
    }
    for pair in a.chunks(2) {
        st.insert(pair[0].clone(), Value::Str(pair[1].clone()));
    }
    Ok(Frame::ok())
}

/// INCR / DECR / INCRBY / DECRBY: add `by` to an integer string.
pub fn incr_by(st: &mut State, key: &Bytes, by: i64) -> Reply {
    let current = match st.string(key).map_err(wrong)? {
        None => 0,
        Some(s) => parse_i64(s).ok_or_else(error::not_an_integer)?,
    };
    let next = current
        .checked_add(by)
        .ok_or_else(error::increment_overflow)?;
    // Keep any TTL, as Redis does for INCR.
    st.insert_keep_ttl(key.clone(), Value::Str(Bytes::from(next.to_string())));
    Ok(Frame::Integer(next))
}

/// `APPEND key value`: the new length.
pub fn append(st: &mut State, a: &[Bytes]) -> Reply {
    let mut joined = st
        .string(&a[0])
        .map_err(wrong)?
        .map_or_else(Vec::new, |s| s.to_vec());
    joined.extend_from_slice(&a[1]);
    let len = joined.len();
    st.insert_keep_ttl(a[0].clone(), Value::Str(Bytes::from(joined)));
    Ok(int(len))
}

/// `STRLEN key`
pub fn strlen(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(int(st.string(&a[0]).map_err(wrong)?.map_or(0, |s| s.len())))
}
