//! Parsing individual command arguments.

use super::NUM_DBS;
use crate::error;
use crate::resp::{Frame, parse_i64};
use bytes::Bytes;
use std::time::Duration;

/// Timeouts longer than this are treated as "wait forever".
const FOREVER_THRESHOLD: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

/// The arguments of a request: an array whose items are strings.
/// (Integers and simple strings are accepted too, as Redis does.)
pub fn from_frame(frame: Frame) -> Result<Vec<Bytes>, Frame> {
    let Frame::Array(items) = frame else {
        return Err(error::not_a_command());
    };
    items
        .into_iter()
        .map(|item| match item {
            Frame::Bulk(b) => Ok(b),
            Frame::Simple(s) => Ok(Bytes::from(s)),
            Frame::Integer(n) => Ok(Bytes::from(n.to_string())),
            _ => Err(error::not_a_command()),
        })
        .collect()
}

/// `SELECT` index: an integer in `0..NUM_DBS`.
pub fn db_index(raw: &[u8]) -> Result<usize, Frame> {
    let idx = parse_i64(raw).ok_or_else(error::not_an_integer)?;
    match usize::try_from(idx) {
        Ok(i) if i < NUM_DBS => Ok(i),
        _ => Err(error::db_index_out_of_range()),
    }
}

/// EVAL/EVALSHA `numkeys`: how many of the `available` arguments are keys.
pub fn numkeys(raw: &[u8], available: usize) -> Result<usize, Frame> {
    let n = parse_i64(raw).ok_or_else(error::not_an_integer)?;
    if n < 0 {
        return Err(error::negative_numkeys());
    }
    match usize::try_from(n) {
        Ok(n) if n <= available => Ok(n),
        _ => Err(error::too_many_numkeys()),
    }
}

/// BLPOP/BRPOP timeout: a double in seconds. `0` means forever (`None`).
pub fn timeout(raw: &[u8]) -> Result<Option<Duration>, Frame> {
    let s = std::str::from_utf8(raw).map_err(|_| error::timeout_not_float())?;
    // Rust's f64 parser accepts surrounding text Redis rejects, and "NaN".
    if s.trim() != s {
        return Err(error::timeout_not_float());
    }
    let secs: f64 = s.parse().map_err(|_| error::timeout_not_float())?;
    if secs.is_nan() {
        return Err(error::timeout_not_float());
    }
    if secs < 0.0 {
        return Err(error::timeout_negative());
    }
    if secs == 0.0 {
        return Ok(None);
    }
    match Duration::try_from_secs_f64(secs) {
        Ok(d) if d > FOREVER_THRESHOLD => Ok(None),
        Ok(d) => Ok(Some(d)),
        Err(_) => Ok(None), // +inf or overflow: wait forever
    }
}
