//! Running one script in a sandboxed Lua 5.1 state, the way Redis does.
//!
//! The script sees `KEYS`, `ARGV` and a `redis` table with `call`, `pcall`,
//! `error_reply`, `status_reply`, `sha1hex` and `log`. Reading or creating an
//! undeclared global is an error, as in Redis. Values convert between Redis
//! replies and Lua with Redis' rules:
//!
//! | Redis reply      | Lua value          | back to Redis            |
//! |------------------|--------------------|--------------------------|
//! | integer          | number             | integer (truncated)      |
//! | bulk string      | string             | bulk string              |
//! | nil              | `false`            | nil (`nil` too)          |
//! | array            | table (sequence)   | array, up to first nil   |
//! | status `+OK`     | `{ok = "OK"}`      | status                   |
//! | error            | `{err = "..."}`    | error                    |
//! |                  | `true`             | integer 1                |

use crate::db::{DataCmd, Txn, ops::ParseError};
use crate::resp::Frame;
use bytes::Bytes;
use mlua::{Function, Lua, LuaOptions, MultiValue, StdLib, Table, Value};
use std::cell::Cell;

/// Installed before the user's script. `redis.call` and `redis.pcall` are
/// attached for each run (they need the locked database). Both are Rust
/// functions, like Redis' C functions, so `return redis.call(...)` is not a
/// Lua tail call and error messages can name the script's line.
const PRELUDE: &str = r#"
redis = {}
redis.error_reply = function(msg) return {err = msg} end
redis.status_reply = function(msg) return {ok = msg} end
redis.log = function() end
redis.LOG_DEBUG, redis.LOG_VERBOSE, redis.LOG_NOTICE, redis.LOG_WARNING = 0, 1, 2, 3
__micro_run = function(f) return pcall(f) end
loadstring, dofile, loadfile, load, print = nil, nil, nil, nil, nil
"#;

/// Locks the global table once KEYS and ARGV are in place.
const SANDBOX: &str = r#"
setmetatable(_G, {
  __index = function(_, name)
    error("Script attempted to access nonexistent global variable '" .. tostring(name) .. "'", 2)
  end,
  __newindex = function(_, name)
    error("Attempt to modify a readonly table", 2)
  end,
})
"#;

/// A compiled script in its own Lua state, ready to run once.
pub struct Prepared {
    lua: Lua,
    func: Function,
    sha: String,
}

impl Prepared {
    /// Compile `source` (outside any database lock).
    pub fn compile(sha: &str, source: &[u8]) -> Result<Prepared, Frame> {
        let setup = || -> mlua::Result<Lua> {
            let lua = Lua::new_with(
                StdLib::TABLE | StdLib::STRING | StdLib::MATH,
                LuaOptions::default(),
            )?;
            lua.load(PRELUDE).set_name("=prelude").exec()?;
            let redis: Table = lua.globals().get("redis")?;
            redis.set(
                "sha1hex",
                lua.create_function(|_, s: mlua::LuaString| Ok(super::sha1_hex(&s.as_bytes())))?,
            )?;
            Ok(lua)
        };
        let lua = setup().map_err(|e| Frame::error(format!("ERR Lua setup failed: {e}")))?;
        let func = lua
            .load(source)
            .set_name("=user_script")
            .into_function()
            .map_err(|e| match e {
                mlua::Error::SyntaxError { message, .. } => Frame::error(format!(
                    "ERR Error compiling script (new function): {message}"
                )),
                other => Frame::error(format!("ERR Error compiling script: {other}")),
            })?;
        Ok(Prepared {
            lua,
            func,
            sha: sha.to_string(),
        })
    }

    /// Run the script against the locked database `t`.
    pub fn run(&self, t: &mut Txn<'_>, keys: &[Bytes], args: &[Bytes]) -> Frame {
        self.run_inner(t, keys, args)
            .unwrap_or_else(|e| Frame::error(format!("ERR Lua error: {e}")))
    }

    fn run_inner(&self, t: &mut Txn<'_>, keys: &[Bytes], args: &[Bytes]) -> mlua::Result<Frame> {
        let lua = &self.lua;
        let globals = lua.globals();
        globals.raw_set("KEYS", strings(lua, keys)?)?;
        globals.raw_set("ARGV", strings(lua, args)?)?;
        lua.load(SANDBOX).set_name("=sandbox").exec()?;
        let redis: Table = globals.raw_get("redis")?;
        let run: Function = globals.raw_get("__micro_run")?;
        // Line of the last failing redis.call, for Redis' error suffix.
        let error_line = Cell::new(None::<usize>);
        // `redis.call` and `redis.pcall` share the locked database.
        let txn = std::cell::RefCell::new(t);

        lua.scope(|scope| {
            let call = scope.create_function(|lua, argv: MultiValue| {
                match call_redis(lua, &mut txn.borrow_mut(), argv) {
                    // redis.call raises errors (the script stops unless it
                    // catches them); line 1 of the stack is the script.
                    Frame::Error(e) => {
                        error_line.set(user_line(lua));
                        Err(mlua::Error::external(CallError(e)))
                    }
                    reply => to_lua(lua, reply),
                }
            })?;
            let pcall = scope.create_function(|lua, argv: MultiValue| {
                // redis.pcall returns errors as {err = "..."} tables.
                to_lua(lua, call_redis(lua, &mut txn.borrow_mut(), argv))
            })?;
            redis.raw_set("call", call)?;
            redis.raw_set("pcall", pcall)?;
            let (ok, value): (bool, Value) = run.call(self.func.clone())?;
            Ok(if ok {
                from_lua(&value)
            } else {
                self.failure(&value, error_line.get())
            })
        })
    }

    /// The error reply for a script that raised an error, with Redis'
    /// suffix: "... script: <sha>, on @user_script:<line>."
    fn failure(&self, value: &Value, call_line: Option<usize>) -> Frame {
        let (message, line) = match value {
            Value::Error(e) => match find_call_error(e) {
                Some(msg) => (msg, call_line),
                None => (format!("ERR {e}"), call_line),
            },
            Value::Table(t) => match t.raw_get::<Option<mlua::LuaString>>("err") {
                Ok(Some(e)) => (e.to_string_lossy(), call_line),
                _ => ("ERR script raised an error".to_string(), call_line),
            },
            Value::String(s) => {
                let text = s.to_string_lossy();
                let line = text
                    .strip_prefix("user_script:")
                    .and_then(|rest| rest.split(':').next())
                    .and_then(|n| n.parse().ok());
                (format!("ERR {text}"), line)
            }
            other => (format!("ERR {other:?}"), call_line),
        };
        Frame::error(format!(
            "{message} script: {}, on @user_script:{}.",
            self.sha,
            line.unwrap_or(0)
        ))
    }
}

/// An error reply raised by `redis.call`.
#[derive(Debug)]
struct CallError(String);

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CallError {}

/// Dig the `CallError` out of the error a script raised, if it is one.
fn find_call_error(e: &mlua::Error) -> Option<String> {
    match e {
        mlua::Error::CallbackError { cause, .. } => find_call_error(cause),
        mlua::Error::ExternalError(inner) => inner.downcast_ref::<CallError>().map(|c| c.0.clone()),
        _ => None,
    }
}

/// Run one data command for `redis.call` / `redis.pcall`.
fn call_redis(lua: &Lua, t: &mut &mut Txn<'_>, argv: MultiValue) -> Frame {
    let mut words = Vec::with_capacity(argv.len());
    for v in argv {
        match lua.coerce_string(v) {
            Ok(Some(s)) => words.push(Bytes::copy_from_slice(&s.as_bytes())),
            _ => {
                return Frame::error(
                    "ERR Lua redis lib command arguments must be strings or integers",
                );
            }
        }
    }
    let Some((name, rest)) = words.split_first() else {
        return Frame::error("ERR Please specify at least one argument for this redis lib call");
    };
    match DataCmd::parse(name, rest.to_vec()) {
        Ok(cmd) if !cmd.op.allowed_in_scripts() => {
            Frame::error("ERR This Redis command is not allowed from script")
        }
        Ok(cmd) => t.run(&cmd),
        Err(ParseError::WrongArity) => {
            Frame::error("ERR Wrong number of args calling Redis command from script")
        }
        Err(ParseError::UnknownCommand) => match name.to_ascii_lowercase().as_slice() {
            b"select" => Frame::error("ERR SELECT is not supported inside scripts by this server"),
            b"multi" | b"exec" | b"discard" | b"eval" | b"evalsha" | b"script" | b"flushall"
            | b"client" | b"quit" => {
                Frame::error("ERR This Redis command is not allowed from script")
            }
            _ => Frame::error("ERR Unknown Redis command called from script"),
        },
    }
}

/// Current line in the user's script (the nearest user frame on the stack).
fn user_line(lua: &Lua) -> Option<usize> {
    (1..16).find_map(|level| {
        lua.inspect_stack(level, |d| {
            let src = d.source();
            let is_user = src.short_src.as_deref() == Some("user_script");
            if is_user { d.current_line() } else { None }
        })
        .flatten()
    })
}

/// A Lua sequence of strings (KEYS, ARGV).
fn strings(lua: &Lua, items: &[Bytes]) -> mlua::Result<Table> {
    lua.create_sequence_from(
        items
            .iter()
            .map(|b| lua.create_string(b))
            .collect::<mlua::Result<Vec<_>>>()?,
    )
}

/// Redis reply → Lua value.
fn to_lua(lua: &Lua, frame: Frame) -> mlua::Result<Value> {
    Ok(match frame {
        Frame::Integer(n) => Value::Integer(n),
        Frame::Bulk(b) => Value::String(lua.create_string(&b)?),
        Frame::Null | Frame::NullArray => Value::Boolean(false),
        Frame::Simple(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", s)?;
            Value::Table(t)
        }
        Frame::Error(e) => {
            let t = lua.create_table()?;
            t.raw_set("err", e)?;
            Value::Table(t)
        }
        Frame::Array(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                t.raw_set(i + 1, to_lua(lua, item)?)?;
            }
            Value::Table(t)
        }
    })
}

/// Lua value returned by a script → Redis reply.
fn from_lua(value: &Value) -> Frame {
    match value {
        Value::Nil => Frame::Null,
        Value::Boolean(true) => Frame::Integer(1),
        Value::Boolean(false) => Frame::Null,
        Value::Integer(n) => Frame::Integer(*n),
        Value::Number(f) => Frame::Integer(f.trunc() as i64),
        Value::String(s) => Frame::Bulk(Bytes::copy_from_slice(&s.as_bytes())),
        Value::Table(t) => {
            if let Ok(Some(e)) = t.raw_get::<Option<mlua::LuaString>>("err") {
                return Frame::Error(e.to_string_lossy());
            }
            if let Ok(Some(ok)) = t.raw_get::<Option<mlua::LuaString>>("ok") {
                return Frame::Simple(ok.to_string_lossy());
            }
            let mut items = Vec::new();
            for i in 1.. {
                match t.raw_get::<Value>(i) {
                    Ok(Value::Nil) | Err(_) => break,
                    Ok(v) => items.push(from_lua(&v)),
                }
            }
            Frame::Array(items)
        }
        _ => Frame::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    fn eval(db: &Db, src: &str, keys: &[&str], args: &[&str]) -> Frame {
        let p = Prepared::compile(&crate::scripting::sha1_hex(src.as_bytes()), src.as_bytes())
            .unwrap_or_else(|e| panic!("compile: {e:?}"));
        let k: Vec<Bytes> = keys.iter().map(|s| Bytes::from(s.to_string())).collect();
        let a: Vec<Bytes> = args.iter().map(|s| Bytes::from(s.to_string())).collect();
        db.critical(|t| p.run(t, &k, &a))
    }

    fn bulk(s: &str) -> Frame {
        Frame::Bulk(Bytes::from(s.to_string()))
    }

    #[test]
    fn conversions_match_redis() {
        let db = Db::new();
        assert_eq!(eval(&db, "return 3.99", &[], &[]), Frame::Integer(3));
        assert_eq!(eval(&db, "return true", &[], &[]), Frame::Integer(1));
        assert_eq!(eval(&db, "return false", &[], &[]), Frame::Null);
        assert_eq!(eval(&db, "return nil", &[], &[]), Frame::Null);
        // `false` becomes nil but does not end the array; a real nil would.
        assert_eq!(
            eval(&db, "return {1, 'two', false, 4}", &[], &[]),
            Frame::Array(vec![
                Frame::Integer(1),
                bulk("two"),
                Frame::Null,
                Frame::Integer(4)
            ])
        );
        assert_eq!(
            eval(&db, "return {1, nil, 3}", &[], &[]),
            Frame::Array(vec![Frame::Integer(1)])
        );
        assert_eq!(
            eval(&db, "return {ok='yes'}", &[], &[]),
            Frame::Simple("yes".into())
        );
        assert_eq!(
            eval(&db, "return redis.error_reply('MY boom')", &[], &[]),
            Frame::error("MY boom")
        );
        assert_eq!(
            eval(&db, "return {KEYS[1], ARGV[2]}", &["k"], &["a", "b"]),
            Frame::Array(vec![bulk("k"), bulk("b")])
        );
        assert_eq!(
            eval(&db, "return redis.sha1hex('abc')", &[], &[]),
            bulk("a9993e364706816aba3e25717850c26c9cd0d89d")
        );
    }

    #[test]
    fn calls_reach_the_database_atomically() {
        let db = Db::new();
        let src = "redis.call('SET', KEYS[1], 10)
                   local n = redis.call('INCRBY', KEYS[1], ARGV[1])
                   redis.call('RPUSH', 'q', n, 'x')
                   return {n, redis.call('LLEN', 'q'), redis.call('GET', 'missing')}";
        assert_eq!(
            eval(&db, src, &["c"], &["5"]),
            Frame::Array(vec![Frame::Integer(15), Frame::Integer(2), Frame::Null])
        );
        assert_eq!(db.get(b"c"), Ok(Some(Bytes::from("15"))));
        db.check_invariants().unwrap();
    }

    #[test]
    fn errors_match_redis_format() {
        let db = Db::new();
        db.set(Bytes::from("s"), Bytes::from("abc"));
        let src = "return redis.call('INCR', KEYS[1])";
        let sha = crate::scripting::sha1_hex(src.as_bytes());
        assert_eq!(
            eval(&db, src, &["s"], &[]),
            Frame::error(format!(
                "ERR value is not an integer or out of range script: {sha}, on @user_script:1."
            ))
        );
        assert_eq!(
            eval(&db, "return redis.pcall('INCR', KEYS[1])", &["s"], &[]),
            Frame::error("ERR value is not an integer or out of range")
        );
        let Frame::Error(e) = eval(&db, "return x.y", &[], &[]) else {
            panic!()
        };
        assert!(e.starts_with("ERR user_script:1: Script attempted to access nonexistent global variable 'x' script: "), "{e}");
        let Frame::Error(e) = eval(&db, "return redis.call('NOPE')", &[], &[]) else {
            panic!()
        };
        assert!(
            e.starts_with("ERR Unknown Redis command called from script script: "),
            "{e}"
        );
        let Frame::Error(e) = eval(&db, "return redis.call('BLPOP', 'q', 0)", &[], &[]) else {
            panic!()
        };
        assert!(
            e.starts_with("ERR This Redis command is not allowed from script"),
            "{e}"
        );
        assert!(matches!(
            Prepared::compile("x", b"return ("),
            Err(Frame::Error(e)) if e == "ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'"
        ));
    }

    #[test]
    fn a_failing_script_keeps_earlier_writes_like_redis() {
        // Redis scripts do not roll back: writes before the error stay.
        let db = Db::new();
        let src = "redis.call('SET', 'a', 1); redis.call('INCR', 'a'); return redis.call('LPUSH', 'a', 'x')";
        assert!(matches!(eval(&db, src, &[], &[]), Frame::Error(_)));
        assert_eq!(db.get(b"a"), Ok(Some(Bytes::from("2"))));
    }
}
