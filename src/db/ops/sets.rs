//! Set commands: SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS.

use super::{Reply, int, wrong};
use crate::db::state::State;
use crate::resp::Frame;
use bytes::Bytes;

/// `SADD key member [member ...]`: number of members newly added.
pub fn sadd(st: &mut State, a: &[Bytes]) -> Reply {
    let set = st.set_or_new(&a[0]).map_err(wrong)?;
    let added = a[1..].iter().filter(|m| set.insert((*m).clone())).count();
    Ok(int(added))
}

/// `SREM key member [member ...]`: number of members removed.
pub fn srem(st: &mut State, a: &[Bytes]) -> Reply {
    let Some(set) = st.set(&a[0]).map_err(wrong)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = a[1..].iter().filter(|m| set.remove(*m)).count();
    st.cleanup(&a[0]);
    Ok(int(removed))
}

/// `SISMEMBER key member`
pub fn sismember(st: &mut State, a: &[Bytes]) -> Reply {
    let found = st
        .set(&a[0])
        .map_err(wrong)?
        .is_some_and(|s| s.contains(&a[1]));
    Ok(Frame::Integer(found as i64))
}

/// `SMISMEMBER key member [member ...]`
pub fn smismember(st: &mut State, a: &[Bytes]) -> Reply {
    let set = st.set(&a[0]).map_err(wrong)?;
    Ok(Frame::Array(
        a[1..]
            .iter()
            .map(|m| Frame::Integer(set.as_ref().is_some_and(|s| s.contains(m)) as i64))
            .collect(),
    ))
}

/// `SCARD key`
pub fn scard(st: &mut State, a: &[Bytes]) -> Reply {
    Ok(int(st.set(&a[0]).map_err(wrong)?.map_or(0, |s| s.len())))
}

/// `SMEMBERS key` (in no particular order, like Redis).
pub fn smembers(st: &mut State, a: &[Bytes]) -> Reply {
    let members: Vec<Bytes> = st
        .set(&a[0])
        .map_err(wrong)?
        .map_or_else(Vec::new, |s| s.iter().cloned().collect());
    Ok(Frame::Array(members.into_iter().map(Frame::Bulk).collect()))
}
