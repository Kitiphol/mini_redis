//! List commands. Every push goes through `State::push` or `mark_ready`, so
//! clients blocked in BLPOP/BRPOP are served when the critical section ends.

use super::{Reply, bulk_or_nil, bulks, int, int_arg, wrong};
use crate::db::End;
use crate::db::state::State;
use crate::error;
use crate::resp::Frame;
use bytes::Bytes;

/// `LPUSH`/`RPUSH key value [value ...]`; with `only_if_exists`, LPUSHX/RPUSHX.
pub fn push(st: &mut State, a: &[Bytes], end: End, only_if_exists: bool) -> Reply {
    if only_if_exists && st.list(&a[0]).map_err(wrong)?.is_none() {
        return Ok(Frame::Integer(0));
    }
    st.push(&a[0], a[1..].to_vec(), end).map(int).map_err(wrong)
}

/// `LPOP key [count]` / `RPOP key [count]`
pub fn pop(st: &mut State, a: &[Bytes], end: End) -> Reply {
    let count = match a.get(1) {
        None => None,
        Some(raw) => {
            let n = int_arg(raw).map_err(|_| error::must_be_positive())?;
            Some(usize::try_from(n).map_err(|_| error::must_be_positive())?)
        }
    };
    if a.len() > 2 {
        return Err(error::wrong_arity(if end == End::Left {
            "lpop"
        } else {
            "rpop"
        }));
    }
    let Some(list) = st.list(&a[0]).map_err(wrong)? else {
        return Ok(if count.is_some() {
            Frame::NullArray
        } else {
            Frame::Null
        });
    };
    let reply = match count {
        None => bulk_or_nil(end.pop(list).as_ref()),
        Some(n) => {
            let taken: Vec<Bytes> = (0..n).map_while(|_| end.pop(list)).collect();
            bulks(&taken)
        }
    };
    st.cleanup(&a[0]);
    Ok(reply)
}

/// `LLEN key`
pub fn llen(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(int(st.list(&a[0]).map_err(wrong)?.map_or(0, |l| l.len())))
}

/// Resolve Redis-style start/stop indexes (negative = from the end) into a
/// clamped inclusive range, or None if it selects nothing.
fn range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut start = if start < 0 { len + start } else { start };
    let mut stop = if stop < 0 { len + stop } else { stop };
    start = start.max(0);
    stop = stop.min(len - 1);
    if start > stop || start >= len {
        return None;
    }
    Some((start as usize, stop as usize))
}

/// `LRANGE key start stop`
pub fn lrange(st: &mut State, a: &[Bytes]) -> Reply {
    let (start, stop) = (int_arg(&a[1])?, int_arg(&a[2])?);
    let Some(list) = st.list(&a[0]).map_err(wrong)? else {
        return Ok(Frame::Array(vec![]));
    };
    Ok(match range(list.len(), start, stop) {
        None => Frame::Array(vec![]),
        Some((s, e)) => bulks(list.range(s..=e)),
    })
}

/// Turn a possibly-negative index into a position, if in range.
fn index(len: usize, i: i64) -> Option<usize> {
    let i = if i < 0 { len as i64 + i } else { i };
    (0..len as i64).contains(&i).then_some(i as usize)
}

/// `LINDEX key index`
/// (Like Redis, the key is looked up before the index is parsed.)
pub fn lindex(st: &mut State, a: &[Bytes]) -> Reply {
    let Some(list) = st.list(&a[0]).map_err(wrong)? else {
        return Ok(Frame::Null);
    };
    let i = int_arg(&a[1])?;
    Ok(bulk_or_nil(index(list.len(), i).and_then(|p| list.get(p))))
}

/// `LSET key index element`
/// (Like Redis, the key is looked up before the index is parsed.)
pub fn lset(st: &mut State, a: &[Bytes]) -> Reply {
    let Some(list) = st.list(&a[0]).map_err(wrong)? else {
        return Err(error::no_such_key());
    };
    let i = int_arg(&a[1])?;
    let Some(p) = index(list.len(), i) else {
        return Err(error::index_out_of_range());
    };
    list[p] = a[2].clone();
    Ok(Frame::ok())
}

/// `LREM key count element`: count > 0 from the head, < 0 from the tail,
/// 0 removes all. Returns the number removed.
pub fn lrem(st: &mut State, a: &[Bytes]) -> Reply {
    let count = int_arg(&a[1])?;
    let target = &a[2];
    let Some(list) = st.list(&a[0]).map_err(wrong)? else {
        return Ok(Frame::Integer(0));
    };
    let limit = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };
    let mut removed = 0;
    if count >= 0 {
        let mut i = 0;
        while i < list.len() && removed < limit {
            if list[i] == *target {
                list.remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
    } else {
        let mut i = list.len();
        while i > 0 && removed < limit {
            i -= 1;
            if list[i] == *target {
                list.remove(i);
                removed += 1;
            }
        }
    }
    st.cleanup(&a[0]);
    Ok(int(removed))
}

/// `LTRIM key start stop`
pub fn ltrim(st: &mut State, a: &[Bytes]) -> Reply {
    let (start, stop) = (int_arg(&a[1])?, int_arg(&a[2])?);
    let Some(list) = st.list(&a[0]).map_err(wrong)? else {
        return Ok(Frame::ok());
    };
    match range(list.len(), start, stop) {
        None => list.clear(),
        Some((s, e)) => {
            list.truncate(e + 1);
            list.drain(..s);
        }
    }
    st.cleanup(&a[0]);
    Ok(Frame::ok())
}

/// `LMOVE source destination LEFT|RIGHT LEFT|RIGHT`
pub fn lmove_cmd(st: &mut State, a: &[Bytes]) -> Reply {
    let (Some(from), Some(to)) = (End::parse(&a[2]), End::parse(&a[3])) else {
        return Err(error::syntax_error());
    };
    lmove(st, &a[0], &a[1], from, to)
}

/// Atomically pop from `src` and push onto `dst` (RPOPLPUSH, LMOVE).
pub fn lmove(st: &mut State, src: &Bytes, dst: &Bytes, from: End, to: End) -> Reply {
    if st.list(src).map_err(wrong)?.is_none() {
        return Ok(Frame::Null);
    }
    // Check the destination's type before changing anything.
    st.list(dst).map_err(wrong)?;
    let Some(v) = st.list(src).map_err(wrong)?.and_then(|l| from.pop(l)) else {
        return Ok(Frame::Null);
    };
    // Push before deleting an emptied source, as Redis does: when source and
    // destination are the same one-element list, it never stops existing
    // (and keeps its TTL).
    st.push(dst, vec![v.clone()], to).map_err(wrong)?;
    st.cleanup(src);
    Ok(Frame::Bulk(v))
}

/// BLPOP/BRPOP inside a transaction: never blocks. Pops if it can, otherwise
/// replies nil — what Redis does inside MULTI.
pub fn blocking_pop_now(st: &mut State, a: &[Bytes], end: End) -> Reply {
    let (keys, timeout) = a.split_at(a.len() - 1);
    crate::command::args::timeout(&timeout[0])?;
    match st.try_pop(keys, end).map_err(wrong)? {
        Some((k, v)) => Ok(Frame::Array(vec![Frame::Bulk(k), Frame::Bulk(v)])),
        None => Ok(Frame::NullArray),
    }
}
