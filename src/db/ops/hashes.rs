//! Hash commands: HSET, HMSET, HSETNX, HGET, HMGET, HGETALL, HDEL, HLEN,
//! HEXISTS, HINCRBY, HKEYS, HVALS.

use super::{Reply, bulk_or_nil, int, int_arg, wrong};
use crate::db::state::State;
use crate::error;
use crate::resp::{Frame, parse_i64};
use bytes::Bytes;

/// `HSET key field value [field value ...]`: number of new fields.
pub fn hset(st: &mut State, a: &[Bytes], cmd: &str) -> Reply {
    if a.len() % 2 != 1 {
        return Err(error::wrong_arity(cmd));
    }
    let hash = st.hash_or_new(&a[0]).map_err(wrong)?;
    let added = a[1..]
        .chunks(2)
        .filter(|pair| hash.insert(pair[0].clone(), pair[1].clone()).is_none())
        .count();
    Ok(int(added))
}

/// `HSETNX key field value`: 1 if the field was set.
pub fn hsetnx(st: &mut State, a: &[Bytes]) -> Reply {
    let hash = st.hash_or_new(&a[0]).map_err(wrong)?;
    if hash.contains_key(&a[1]) {
        return Ok(Frame::Integer(0));
    }
    hash.insert(a[1].clone(), a[2].clone());
    Ok(Frame::Integer(1))
}

/// `HGET key field`
pub fn hget(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(bulk_or_nil(
        st.hash(&a[0]).map_err(wrong)?.and_then(|h| h.get(&a[1])),
    ))
}

/// `HMGET key field [field ...]`
pub fn hmget(st: &mut State, a: &[Bytes]) -> Reply {
    let hash = st.hash(&a[0]).map_err(wrong)?;
    Ok(Frame::Array(
        a[1..]
            .iter()
            .map(|f| bulk_or_nil(hash.as_ref().and_then(|h| h.get(f))))
            .collect(),
    ))
}

/// `HGETALL key`: field, value, field, value, ...
pub fn hgetall(st: &mut State, a: &[Bytes]) -> Reply {
    let mut out = Vec::new();
    if let Some(h) = st.hash(&a[0]).map_err(wrong)? {
        for (f, v) in h.iter() {
            out.push(Frame::Bulk(f.clone()));
            out.push(Frame::Bulk(v.clone()));
        }
    }
    Ok(Frame::Array(out))
}

/// `HDEL key field [field ...]`: number of fields removed.
pub fn hdel(st: &mut State, a: &[Bytes]) -> Reply {
    let Some(hash) = st.hash(&a[0]).map_err(wrong)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = a[1..].iter().filter(|f| hash.remove(*f).is_some()).count();
    st.cleanup(&a[0]);
    Ok(int(removed))
}

/// `HLEN key`
pub fn hlen(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(int(st.hash(&a[0]).map_err(wrong)?.map_or(0, |h| h.len())))
}

/// `HEXISTS key field`
pub fn hexists(st: &mut State, a: &[Bytes]) -> Reply {
    let found = st
        .hash(&a[0])
        .map_err(wrong)?
        .is_some_and(|h| h.contains_key(&a[1]));
    Ok(Frame::Integer(found as i64))
}

/// `HINCRBY key field increment`: the new value.
pub fn hincrby(st: &mut State, a: &[Bytes]) -> Reply {
    let by = int_arg(&a[2])?;
    let hash = st.hash_or_new(&a[0]).map_err(wrong)?;
    let current = match hash.get(&a[1]) {
        None => 0,
        Some(v) => parse_i64(v).ok_or_else(error::hash_not_integer)?,
    };
    let next = current
        .checked_add(by)
        .ok_or_else(error::increment_overflow)?;
    hash.insert(a[1].clone(), Bytes::from(next.to_string()));
    Ok(Frame::Integer(next))
}

/// `HKEYS key`
pub fn hkeys(st: &mut State, a: &[Bytes]) -> Reply {
    let keys: Vec<Bytes> = st
        .hash(&a[0])
        .map_err(wrong)?
        .map_or_else(Vec::new, |h| h.keys().cloned().collect());
    Ok(Frame::Array(keys.into_iter().map(Frame::Bulk).collect()))
}

/// `HVALS key`
pub fn hvals(st: &mut State, a: &[Bytes]) -> Reply {
    let vals: Vec<Bytes> = st
        .hash(&a[0])
        .map_err(wrong)?
        .map_or_else(Vec::new, |h| h.values().cloned().collect());
    Ok(Frame::Array(vals.into_iter().map(Frame::Bulk).collect()))
}
