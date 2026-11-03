//! Every error reply the server can send, in one place.
//!
//! Wording matches real Redis wherever Redis has the same error, so clients
//! and `redis-cli` behave as expected. The two course-specific errors
//! (`select_not_first`, `too_many_keys`) are marked below.

use crate::resp::Frame;
use bytes::Bytes;
use std::fmt::Display;

/// `-ERR wrong number of arguments for 'get' command`
pub fn wrong_arity(command: &str) -> Frame {
    Frame::error(format!(
        "ERR wrong number of arguments for '{command}' command"
    ))
}

/// `-ERR unknown command 'FOO', with args beginning with: 'x' `
/// (`name` keeps the client's casing, as Redis does).
pub fn unknown_command(name: &str, args: &[Bytes]) -> Frame {
    let mut msg = format!("ERR unknown command '{name}', with args beginning with: ");
    for a in args.iter().take(16) {
        msg.push_str(&format!("'{}' ", String::from_utf8_lossy(a)));
    }
    Frame::error(msg)
}

/// `SET` with options (the course allows only `SET key value`).
pub fn syntax_error() -> Frame {
    Frame::error("ERR syntax error")
}

/// `SELECT abc`
pub fn not_an_integer() -> Frame {
    Frame::error("ERR value is not an integer or out of range")
}

/// `SELECT 16`, `SELECT -1`
pub fn db_index_out_of_range() -> Frame {
    Frame::error("ERR DB index is out of range")
}

/// Course rule: SELECT after the session has started.
pub fn select_not_first() -> Frame {
    Frame::error("ERR SELECT is only allowed as the first command")
}

/// `BLPOP q abc`, `BLPOP q nan`
pub fn timeout_not_float() -> Frame {
    Frame::error("ERR timeout is not a float or out of range")
}

/// `BLPOP q -1`
pub fn timeout_negative() -> Frame {
    Frame::error("ERR timeout is negative")
}

/// Course rule: BLPOP/BRPOP may wait on at most `max` keys.
pub fn too_many_keys(command: &str, max: usize) -> Frame {
    Frame::error(format!(
        "ERR {} accepts at most {max} keys",
        command.to_ascii_uppercase()
    ))
}

/// INCR on a value that would overflow a 64-bit integer.
pub fn increment_overflow() -> Frame {
    Frame::error("ERR increment or decrement would overflow")
}

/// HINCRBY on a field that isn't an integer.
pub fn hash_not_integer() -> Frame {
    Frame::error("ERR hash value is not an integer")
}

/// LSET on a missing key.
pub fn no_such_key() -> Frame {
    Frame::error("ERR no such key")
}

/// LSET with an index outside the list.
pub fn index_out_of_range() -> Frame {
    Frame::error("ERR index out of range")
}

/// SET EX 0, SETEX with a non-positive time, or an overflowing expire.
pub fn invalid_expire(command: &str) -> Frame {
    Frame::error(format!("ERR invalid expire time in '{command}' command"))
}

/// LPOP/RPOP with a negative count.
pub fn must_be_positive() -> Frame {
    Frame::error("ERR value is out of range, must be positive")
}

// ---- transactions ---------------------------------------------------------

pub fn nested_multi() -> Frame {
    Frame::error("ERR MULTI calls can not be nested")
}

pub fn exec_without_multi() -> Frame {
    Frame::error("ERR EXEC without MULTI")
}

pub fn discard_without_multi() -> Frame {
    Frame::error("ERR DISCARD without MULTI")
}

pub fn exec_abort() -> Frame {
    Frame::error("EXECABORT Transaction discarded because of previous errors.")
}

/// A command this server cannot queue inside MULTI (e.g. SELECT, FLUSHALL).
pub fn not_in_multi(command: &str) -> Frame {
    Frame::error(format!(
        "ERR '{command}' is not supported inside MULTI by this server"
    ))
}

// ---- scripting ------------------------------------------------------------

pub fn no_script() -> Frame {
    Frame::error("NOSCRIPT No matching script. Please use EVAL.")
}

pub fn negative_numkeys() -> Frame {
    Frame::error("ERR Number of keys can't be negative")
}

pub fn too_many_numkeys() -> Frame {
    Frame::error("ERR Number of keys can't be greater than number of args")
}

/// `SCRIPT <unknown subcommand>`
pub fn unknown_subcommand(command: &str, sub: &[u8]) -> Frame {
    Frame::error(format!(
        "ERR unknown subcommand '{}'. Try {} HELP.",
        String::from_utf8_lossy(sub),
        command.to_ascii_uppercase()
    ))
}

/// A request that is valid RESP but not an array of strings.
pub fn not_a_command() -> Frame {
    protocol("expected an array of bulk strings")
}

/// Malformed RESP. The connection is closed after sending this.
pub fn protocol(detail: impl Display) -> Frame {
    Frame::error(format!("ERR Protocol error: {detail}"))
}

/// A client sent more unparsed bytes than the server will buffer.
pub fn request_too_large() -> Frame {
    protocol("request too large")
}

/// Turn a database error (`WRONGTYPE`) into a reply.
pub fn db(e: crate::db::DbError) -> Frame {
    Frame::error(e.to_string())
}
