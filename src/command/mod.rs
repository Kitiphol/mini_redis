//! Turning a request frame into a typed, validated [`Command`].
//!
//! Argument counts (and the BLPOP rules) are validated here, *before* any
//! lock is taken, so the critical sections in `db` only ever do data work.
//!
//! * `mod.rs`  — the [`Command`] type and parsing by command name
//! * [`args`]  — parsing individual arguments (database index, timeout, numkeys)
//!
//! Data commands (GET, LPUSH, HSET, ...) are looked up in the command table
//! in `db::ops`; this module handles the commands that are about the
//! connection, the server, transactions or scripts.

pub mod args;

#[cfg(test)]
mod tests;

use crate::db::{DataCmd, Op, ops::ParseError};
use crate::error;
use crate::resp::Frame;
use bytes::Bytes;

/// Number of independent databases (`SELECT 0` .. `SELECT 15`).
pub const NUM_DBS: usize = 16;
/// Course rule: a single BLPOP/BRPOP may wait on at most 5 keys.
pub const MAX_BLOCKING_KEYS: usize = 5;

/// A validated command, ready to execute.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// `PING [message]` (answered without touching a database).
    Ping(Option<Bytes>),
    /// `SELECT index`
    Select(usize),
    /// A data command on the selected database. BLPOP/BRPOP are data
    /// commands too: they block only when run outside a transaction.
    Data(DataCmd),
    /// `MULTI`, `EXEC`, `DISCARD`
    Multi,
    Exec,
    Discard,
    /// `EVAL script numkeys ...` / `EVALSHA sha1 numkeys ...`
    Eval(ScriptCall),
    /// `SCRIPT LOAD|EXISTS|FLUSH`
    ScriptLoad(Bytes),
    ScriptExists(Vec<Bytes>),
    ScriptFlush,
    /// `FLUSHALL [ASYNC|SYNC]`: empty every database.
    FlushAll,
    /// `CLIENT ...`: connection metadata (name, library info, id).
    Client(ClientCmd),
    /// `COMMAND ...`: introspection, which `redis-cli` asks for on start.
    CommandInfo,
    /// `QUIT`: reply OK, then close.
    Quit,
}

/// Which script an EVAL/EVALSHA runs, with its keys and arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct ScriptCall {
    pub script: ScriptRef,
    pub keys: Vec<Bytes>,
    pub args: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScriptRef {
    /// EVAL: the script's source.
    Source(Bytes),
    /// EVALSHA: the SHA1 of a script loaded earlier (lowercase hex).
    Sha(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCmd {
    SetName(Bytes),
    GetName,
    SetInfo,
    Id,
}

impl Command {
    /// Lowercase command name, for logs.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Ping(_) => "ping",
            Command::Select(_) => "select",
            Command::Data(c) => c.op.name(),
            Command::Multi => "multi",
            Command::Exec => "exec",
            Command::Discard => "discard",
            Command::Eval(c) => match c.script {
                ScriptRef::Source(_) => "eval",
                ScriptRef::Sha(_) => "evalsha",
            },
            Command::ScriptLoad(_) | Command::ScriptExists(_) | Command::ScriptFlush => "script",
            Command::FlushAll => "flushall",
            Command::Client(_) => "client",
            Command::CommandInfo => "command",
            Command::Quit => "quit",
        }
    }

    /// Commands that only describe the connection (what client libraries
    /// send while connecting). They don't start the session, so SELECT is
    /// still allowed after them.
    pub fn is_connection_setup(&self) -> bool {
        matches!(self, Command::Client(_) | Command::CommandInfo)
    }

    /// MULTI, EXEC and DISCARD: run immediately even inside a transaction.
    pub fn controls_multi(&self) -> bool {
        matches!(self, Command::Multi | Command::Exec | Command::Discard)
    }
}

/// Outcome of parsing a request.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    Command(Command),
    /// The request is invalid; send this error reply and keep the connection.
    Error(Frame),
    /// An empty request (`*0\r\n` or a blank inline line): Redis ignores it.
    Empty,
}

/// Parse a request frame into a command.
pub fn parse(frame: Frame) -> Parsed {
    let mut args = match args::from_frame(frame) {
        Ok(args) => args,
        Err(e) => return Parsed::Error(e),
    };
    if args.is_empty() {
        return Parsed::Empty;
    }
    let rest = args.split_off(1);
    let original = String::from_utf8_lossy(&args[0]).into_owned();
    match from_name(&original, rest) {
        Ok(cmd) => Parsed::Command(cmd),
        Err(e) => Parsed::Error(e),
    }
}

/// Build a command from its name (any casing) and its arguments.
fn from_name(original: &str, a: Vec<Bytes>) -> Result<Command, Frame> {
    let name = original.to_ascii_lowercase();
    match name.as_str() {
        "ping" => {
            arity(&name, a.len() <= 1)?;
            Ok(Command::Ping(a.into_iter().next()))
        }
        "select" => {
            arity(&name, a.len() == 1)?;
            Ok(Command::Select(args::db_index(&a[0])?))
        }
        "multi" => no_args(&name, a, Command::Multi),
        "exec" => no_args(&name, a, Command::Exec),
        "discard" => no_args(&name, a, Command::Discard),
        "eval" => eval(&name, a, false),
        "evalsha" => eval(&name, a, true),
        "script" => script(a),
        "flushall" => match a.as_slice() {
            [] => Ok(Command::FlushAll),
            [m] if m.eq_ignore_ascii_case(b"async") || m.eq_ignore_ascii_case(b"sync") => {
                Ok(Command::FlushAll)
            }
            _ => Err(error::syntax_error()),
        },
        "client" => client(a),
        "command" => Ok(Command::CommandInfo),
        "quit" => Ok(Command::Quit),
        "blpop" | "brpop" => blocking_pop(original, a),
        _ => data(original, a),
    }
}

/// A data command from the table in `db::ops`.
fn data(original: &str, a: Vec<Bytes>) -> Result<Command, Frame> {
    match DataCmd::parse(original.as_bytes(), a.clone()) {
        Ok(cmd) => Ok(Command::Data(cmd)),
        Err(ParseError::UnknownCommand) => Err(error::unknown_command(original, &a)),
        Err(ParseError::WrongArity) => Err(error::wrong_arity(&original.to_ascii_lowercase())),
    }
}

/// BLPOP/BRPOP: a data command, with the course's rules checked up front.
fn blocking_pop(original: &str, a: Vec<Bytes>) -> Result<Command, Frame> {
    let name = original.to_ascii_lowercase();
    arity(&name, a.len() >= 2)?;
    args::timeout(&a[a.len() - 1])?;
    if a.len() - 1 > MAX_BLOCKING_KEYS {
        return Err(error::too_many_keys(&name, MAX_BLOCKING_KEYS));
    }
    let op = if name == "blpop" {
        Op::BLPop
    } else {
        Op::BRPop
    };
    Ok(Command::Data(DataCmd { op, args: a }))
}

fn eval(name: &str, mut a: Vec<Bytes>, by_sha: bool) -> Result<Command, Frame> {
    arity(name, a.len() >= 2)?;
    let numkeys = args::numkeys(&a[1], a.len() - 2)?;
    let rest = a.split_off(2);
    let (keys, argv) = rest.split_at(numkeys);
    let script = if by_sha {
        ScriptRef::Sha(String::from_utf8_lossy(&a[0]).to_ascii_lowercase())
    } else {
        ScriptRef::Source(a[0].clone())
    };
    Ok(Command::Eval(ScriptCall {
        script,
        keys: keys.to_vec(),
        args: argv.to_vec(),
    }))
}

fn script(a: Vec<Bytes>) -> Result<Command, Frame> {
    let Some(sub) = a.first() else {
        return Err(error::wrong_arity("script"));
    };
    let sub_lower = sub.to_ascii_lowercase();
    match (sub_lower.as_slice(), a.len()) {
        (b"load", 2) => Ok(Command::ScriptLoad(a[1].clone())),
        (b"exists", n) if n >= 2 => Ok(Command::ScriptExists(a[1..].to_vec())),
        (b"flush", 1 | 2) => Ok(Command::ScriptFlush),
        (b"load" | b"exists" | b"flush", _) => Err(Frame::error(format!(
            "ERR wrong number of arguments for 'script|{}' command",
            String::from_utf8_lossy(&sub_lower)
        ))),
        _ => Err(error::unknown_subcommand("script", sub)),
    }
}

fn client(a: Vec<Bytes>) -> Result<Command, Frame> {
    let Some(sub) = a.first() else {
        return Err(error::wrong_arity("client"));
    };
    match (sub.to_ascii_lowercase().as_slice(), a.len()) {
        (b"setname", 2) => Ok(Command::Client(ClientCmd::SetName(a[1].clone()))),
        (b"getname", 1) => Ok(Command::Client(ClientCmd::GetName)),
        (b"setinfo", 3) => Ok(Command::Client(ClientCmd::SetInfo)),
        (b"id", 1) => Ok(Command::Client(ClientCmd::Id)),
        _ => Err(error::unknown_subcommand("client", sub)),
    }
}

/// Fail with the standard arity error unless `ok`.
fn arity(name: &str, ok: bool) -> Result<(), Frame> {
    if ok {
        Ok(())
    } else {
        Err(error::wrong_arity(name))
    }
}

fn no_args(name: &str, a: Vec<Bytes>, cmd: Command) -> Result<Command, Frame> {
    arity(name, a.is_empty())?;
    Ok(cmd)
}
