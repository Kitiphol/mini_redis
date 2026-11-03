//! Lua scripting: `EVAL`, `EVALSHA` and `SCRIPT LOAD|EXISTS|FLUSH`.
//!
//! * `mod.rs`  — the script cache (SHA1 → source) and the two entry points
//! * [`lua`]   — compiling a script into a sandboxed Lua 5.1 state (the Lua
//!   version Redis uses) and running it against a locked database
//!
//! **Atomicity.** A script runs inside ONE critical section of its database
//! (`Db::critical`): no other command on that database can run in the middle
//! of it, exactly like Redis. Its linearization point is any instant of that
//! critical section. Compiling happens *before* the lock is taken, so the
//! lock is held only while the script actually runs.

mod lua;

use crate::command::{ScriptCall, ScriptRef};
use crate::db::{Db, Txn};
use crate::error;
use crate::resp::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;

/// Lowercase hex SHA1 of a script, as EVALSHA and SCRIPT LOAD use.
pub fn sha1_hex(source: &[u8]) -> String {
    sha1_smol::Sha1::from(source).digest().to_string()
}

/// Scripts the server has seen (via EVAL or SCRIPT LOAD), by SHA1.
/// Shared by all databases, like Redis' script cache.
#[derive(Default)]
pub struct ScriptCache {
    scripts: Mutex<HashMap<String, Bytes>>,
}

impl ScriptCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Bytes>> {
        self.scripts.lock().expect("script cache mutex poisoned")
    }

    /// Remember a script; returns its SHA1.
    pub fn load(&self, source: &Bytes) -> String {
        let sha = sha1_hex(source);
        self.lock()
            .entry(sha.clone())
            .or_insert_with(|| source.clone());
        sha
    }

    pub fn exists(&self, sha: &[u8]) -> bool {
        let sha = String::from_utf8_lossy(sha).to_ascii_lowercase();
        self.lock().contains_key(&sha)
    }

    pub fn flush(&self) {
        self.lock().clear();
    }

    /// The source and SHA1 for an EVAL/EVALSHA (EVAL also caches it).
    fn resolve(&self, script: &ScriptRef) -> Result<(String, Bytes), Frame> {
        match script {
            ScriptRef::Source(src) => Ok((self.load(src), src.clone())),
            ScriptRef::Sha(sha) => match self.lock().get(sha) {
                Some(src) => Ok((sha.clone(), src.clone())),
                None => Err(error::no_script()),
            },
        }
    }
}

/// A script found in the cache, with its arguments: everything needed to run it.
#[derive(Debug, Clone)]
pub struct Resolved {
    sha: String,
    source: Bytes,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
}

/// Look up an EVAL/EVALSHA's script (EVAL also caches it).
pub fn resolve(cache: &ScriptCache, call: ScriptCall) -> Result<Resolved, Frame> {
    let (sha, source) = cache.resolve(&call.script)?;
    Ok(Resolved {
        sha,
        source,
        keys: call.keys,
        args: call.args,
    })
}

/// Compile and run a script inside an atomic step (a transaction's, or its own).
pub fn run(t: &mut Txn<'_>, s: &Resolved) -> Frame {
    match lua::Prepared::compile(&s.sha, &s.source) {
        Ok(p) => p.run(t, &s.keys, &s.args),
        Err(e) => e,
    }
}

/// `EVAL`/`EVALSHA` as a command of its own: one atomic step.
///
/// The script is compiled *before* the lock is taken, so the lock is held
/// only while it runs.
pub fn eval(db: &Db, cache: &ScriptCache, call: ScriptCall) -> Frame {
    let s = match resolve(cache, call) {
        Ok(s) => s,
        Err(e) => return e,
    };
    match lua::Prepared::compile(&s.sha, &s.source) {
        Ok(p) => db.critical(|t| p.run(t, &s.keys, &s.args)),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_matches_redis() {
        // Values printed by a real Redis 7: SCRIPT LOAD "return 1", sha1hex('abc').
        assert_eq!(
            sha1_hex(b"return 1"),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
    }
}
