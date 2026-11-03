//! Every data command: the command table and the code that runs each one.
//!
//! * `mod.rs`     — the [`Op`] table (name, arity, flags), [`DataCmd`], dispatch
//! * [`keys`]     — DEL, EXISTS, TYPE, KEYS, DBSIZE, FLUSHDB, EXPIRE, TTL, ...
//! * [`strings`]  — GET, SET (with options), INCR family, MGET, MSET, ...
//! * [`lists`]    — LPUSH, RPUSH, LPOP, LRANGE, LREM, LMOVE, ...
//! * [`sets`]     — SADD, SREM, SISMEMBER, SMEMBERS, ...
//! * [`hashes`]   — HSET, HGET, HGETALL, HINCRBY, ...
//!
//! Every executor runs inside a critical section (see `Db::critical`) and
//! returns the reply frame. Argument *counts* are checked when a command is
//! parsed (like Redis' arity table); argument *values* (numbers, options) are
//! checked when it runs, so inside `MULTI` they produce an error in `EXEC`'s
//! reply, exactly like Redis.

mod hashes;
mod keys;
mod lists;
mod sets;
mod strings;

use super::DbError;
use super::state::State;
use crate::error;
use crate::resp::{Frame, parse_i64};
use bytes::Bytes;

/// The command may not be called from a Lua script (Redis rejects blocking
/// commands there).
const NO_SCRIPT: u8 = 1;

macro_rules! op_table {
    ($( $variant:ident = $name:literal, $arity:expr, $flags:expr; )*) => {
        /// A data command: something that reads or writes one database.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum Op { $($variant),* }

        impl Op {
            /// Find a command by its lowercase name.
            pub fn lookup(name: &str) -> Option<Op> {
                match name { $($name => Some(Op::$variant),)* _ => None }
            }
            /// Lowercase command name.
            pub fn name(self) -> &'static str {
                match self { $(Op::$variant => $name),* }
            }
            /// Redis-style arity, counting the command name: `n` means exactly
            /// n words, `-n` means at least n.
            pub fn arity(self) -> i32 {
                match self { $(Op::$variant => $arity),* }
            }
            fn flags(self) -> u8 {
                match self { $(Op::$variant => $flags),* }
            }
        }
    };
}

op_table! {
    // keys
    Del = "del", -2, 0;
    Unlink = "unlink", -2, 0;
    Exists = "exists", -2, 0;
    Type = "type", 2, 0;
    Keys = "keys", 2, 0;
    DbSize = "dbsize", 1, 0;
    FlushDb = "flushdb", -1, 0;
    Expire = "expire", -3, 0;
    PExpire = "pexpire", -3, 0;
    Ttl = "ttl", 2, 0;
    PTtl = "pttl", 2, 0;
    Persist = "persist", 2, 0;
    Ping = "ping", -1, 0;
    Echo = "echo", 2, 0;
    // strings
    Get = "get", 2, 0;
    Set = "set", -3, 0;
    SetEx = "setex", 4, 0;
    PSetEx = "psetex", 4, 0;
    SetNx = "setnx", 3, 0;
    GetSet = "getset", 3, 0;
    GetDel = "getdel", 2, 0;
    MGet = "mget", -2, 0;
    MSet = "mset", -3, 0;
    Incr = "incr", 2, 0;
    Decr = "decr", 2, 0;
    IncrBy = "incrby", 3, 0;
    DecrBy = "decrby", 3, 0;
    Append = "append", 3, 0;
    StrLen = "strlen", 2, 0;
    // lists
    LPush = "lpush", -3, 0;
    RPush = "rpush", -3, 0;
    LPushX = "lpushx", -3, 0;
    RPushX = "rpushx", -3, 0;
    LPop = "lpop", -2, 0;
    RPop = "rpop", -2, 0;
    LLen = "llen", 2, 0;
    LRange = "lrange", 4, 0;
    LIndex = "lindex", 3, 0;
    LSet = "lset", 4, 0;
    LRem = "lrem", 4, 0;
    LTrim = "ltrim", 4, 0;
    RPopLPush = "rpoplpush", 3, 0;
    LMove = "lmove", 5, 0;
    BLPop = "blpop", -3, NO_SCRIPT;
    BRPop = "brpop", -3, NO_SCRIPT;
    // sets
    SAdd = "sadd", -3, 0;
    SRem = "srem", -3, 0;
    SIsMember = "sismember", 3, 0;
    SMIsMember = "smismember", -3, 0;
    SCard = "scard", 2, 0;
    SMembers = "smembers", 2, 0;
    // hashes
    HSet = "hset", -4, 0;
    HMSet = "hmset", -4, 0;
    HSetNx = "hsetnx", 4, 0;
    HGet = "hget", 3, 0;
    HMGet = "hmget", -3, 0;
    HGetAll = "hgetall", 2, 0;
    HDel = "hdel", -3, 0;
    HLen = "hlen", 2, 0;
    HExists = "hexists", 3, 0;
    HIncrBy = "hincrby", 4, 0;
    HKeys = "hkeys", 2, 0;
    HVals = "hvals", 2, 0;
}

impl Op {
    /// Is `words` (including the command name) an acceptable count?
    pub fn arity_ok(self, words: usize) -> bool {
        let a = self.arity();
        if a >= 0 {
            words == a as usize
        } else {
            words >= a.unsigned_abs() as usize
        }
    }

    /// May a Lua script call this command?
    pub fn allowed_in_scripts(self) -> bool {
        self.flags() & NO_SCRIPT == 0
    }
}

/// Why a data command could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    UnknownCommand,
    WrongArity,
}

/// A data command with its arguments (the command name excluded).
#[derive(Debug, Clone, PartialEq)]
pub struct DataCmd {
    pub op: Op,
    pub args: Vec<Bytes>,
}

impl DataCmd {
    /// Build a command from its name (any casing) and arguments.
    pub fn parse(name: &[u8], args: Vec<Bytes>) -> Result<DataCmd, ParseError> {
        let lower = String::from_utf8_lossy(name).to_ascii_lowercase();
        let op = Op::lookup(&lower).ok_or(ParseError::UnknownCommand)?;
        if !op.arity_ok(args.len() + 1) {
            return Err(ParseError::WrongArity);
        }
        Ok(DataCmd { op, args })
    }
}

/// Run one data command against the locked state.
pub(super) fn execute(st: &mut State, cmd: &DataCmd) -> Frame {
    let a = cmd.args.as_slice();
    let result = match cmd.op {
        Op::Del | Op::Unlink => keys::del(st, a),
        Op::Exists => keys::exists(st, a),
        Op::Type => keys::type_of(st, a),
        Op::Keys => keys::keys(st, a),
        Op::DbSize => Ok(Frame::Integer(st.len() as i64)),
        Op::FlushDb => keys::flushdb(st, a),
        Op::Expire => keys::expire(st, a, 1000, "expire"),
        Op::PExpire => keys::expire(st, a, 1, "pexpire"),
        Op::Ttl => keys::ttl(st, a, 1000),
        Op::PTtl => keys::ttl(st, a, 1),
        Op::Persist => keys::persist(st, a),
        Op::Ping => Ok(keys::ping(a)),
        Op::Echo => Ok(Frame::Bulk(a[0].clone())),

        Op::Get => strings::get(st, a),
        Op::Set => strings::set(st, a),
        Op::SetEx => strings::setex(st, a, 1000, "setex"),
        Op::PSetEx => strings::setex(st, a, 1, "psetex"),
        Op::SetNx => strings::setnx(st, a),
        Op::GetSet => strings::getset(st, a),
        Op::GetDel => strings::getdel(st, a),
        Op::MGet => strings::mget(st, a),
        Op::MSet => strings::mset(st, a),
        Op::Incr => strings::incr_by(st, &a[0], 1),
        Op::Decr => strings::incr_by(st, &a[0], -1),
        Op::IncrBy => int_arg(&a[1]).and_then(|n| strings::incr_by(st, &a[0], n)),
        Op::DecrBy => int_arg(&a[1]).and_then(|n| match n.checked_neg() {
            Some(n) => strings::incr_by(st, &a[0], n),
            None => Err(Frame::error("ERR decrement would overflow")),
        }),
        Op::Append => strings::append(st, a),
        Op::StrLen => strings::strlen(st, a),

        Op::LPush => lists::push(st, a, super::End::Left, false),
        Op::RPush => lists::push(st, a, super::End::Right, false),
        Op::LPushX => lists::push(st, a, super::End::Left, true),
        Op::RPushX => lists::push(st, a, super::End::Right, true),
        Op::LPop => lists::pop(st, a, super::End::Left),
        Op::RPop => lists::pop(st, a, super::End::Right),
        Op::LLen => lists::llen(st, a),
        Op::LRange => lists::lrange(st, a),
        Op::LIndex => lists::lindex(st, a),
        Op::LSet => lists::lset(st, a),
        Op::LRem => lists::lrem(st, a),
        Op::LTrim => lists::ltrim(st, a),
        Op::RPopLPush => lists::lmove(st, &a[0], &a[1], super::End::Right, super::End::Left),
        Op::LMove => lists::lmove_cmd(st, a),
        Op::BLPop => lists::blocking_pop_now(st, a, super::End::Left),
        Op::BRPop => lists::blocking_pop_now(st, a, super::End::Right),

        Op::SAdd => sets::sadd(st, a),
        Op::SRem => sets::srem(st, a),
        Op::SIsMember => sets::sismember(st, a),
        Op::SMIsMember => sets::smismember(st, a),
        Op::SCard => sets::scard(st, a),
        Op::SMembers => sets::smembers(st, a),

        Op::HSet => hashes::hset(st, a, "hset"),
        Op::HMSet => hashes::hset(st, a, "hmset").map(|_| Frame::ok()),
        Op::HSetNx => hashes::hsetnx(st, a),
        Op::HGet => hashes::hget(st, a),
        Op::HMGet => hashes::hmget(st, a),
        Op::HGetAll => hashes::hgetall(st, a),
        Op::HDel => hashes::hdel(st, a),
        Op::HLen => hashes::hlen(st, a),
        Op::HExists => hashes::hexists(st, a),
        Op::HIncrBy => hashes::hincrby(st, a),
        Op::HKeys => hashes::hkeys(st, a),
        Op::HVals => hashes::hvals(st, a),
    };
    result.unwrap_or_else(|e| e)
}

// ---- helpers shared by the executors -----------------------------------

/// Executors return `Ok(reply)` or `Err(error reply)`; both are frames.
type Reply = Result<Frame, Frame>;

/// Turn a type error into its reply.
fn wrong(e: DbError) -> Frame {
    error::db(e)
}

/// An integer argument, with Redis' error if it isn't one.
fn int_arg(raw: &[u8]) -> Result<i64, Frame> {
    parse_i64(raw).ok_or_else(error::not_an_integer)
}

fn bulk_or_nil(v: Option<&Bytes>) -> Frame {
    v.map_or(Frame::Null, |b| Frame::Bulk(b.clone()))
}

fn bulks<'a>(items: impl IntoIterator<Item = &'a Bytes>) -> Frame {
    Frame::Array(items.into_iter().map(|b| Frame::Bulk(b.clone())).collect())
}

fn int(n: usize) -> Frame {
    Frame::Integer(n as i64)
}
