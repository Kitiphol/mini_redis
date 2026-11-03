//! The data of one database. Everything here runs with the database lock
//! held (`Db` is the only caller), so each method is one atomic step.
//!
//! Two things happen implicitly on every access, so command code never has
//! to think about them:
//!
//! * **Expiry.** A key whose deadline has passed is deleted the moment any
//!   command looks at it ("lazy expiry", as in Redis). `now` is fixed once per
//!   critical section, so a whole command, transaction or script sees one
//!   consistent time.
//! * **Serving blocked clients.** Any push records its key in `ready`; when
//!   the critical section ends, `Db` calls [`State::serve_ready`], which hands
//!   values to clients blocked in BLPOP/BRPOP. Like Redis, this happens after
//!   the whole command, transaction or script, never in the middle of one.

use super::value::{End, Value};
use super::waiters::WaiterRegistry;
use super::{DbError, Popped, WaitTicket};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

pub(super) struct State {
    data: HashMap<Bytes, Value>,
    /// Deadlines of keys that have a TTL. Always a subset of `data`'s keys.
    expires: HashMap<Bytes, Instant>,
    waiters: WaiterRegistry,
    /// List keys pushed to during the current critical section.
    ready: Vec<Bytes>,
    /// The time of the current critical section.
    now: Instant,
}

impl Default for State {
    fn default() -> State {
        State {
            data: HashMap::new(),
            expires: HashMap::new(),
            waiters: WaiterRegistry::default(),
            ready: Vec::new(),
            now: Instant::now(),
        }
    }
}

impl State {
    /// Start a critical section: fix the clock for it.
    pub fn begin(&mut self) {
        self.now = Instant::now();
    }

    pub fn now(&self) -> Instant {
        self.now
    }

    // ---- generic key access (with lazy expiry) -------------------------

    /// Delete `key` if its TTL has passed.
    fn expire_if_due(&mut self, key: &[u8]) {
        if self.expires.get(key).is_some_and(|&t| t <= self.now) {
            self.expires.remove(key);
            self.data.remove(key);
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Value> {
        self.expire_if_due(key);
        self.data.get(key)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        self.expire_if_due(key);
        self.data.get_mut(key)
    }

    pub fn contains(&mut self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Store a value and clear any TTL (what SET, MSET, GETSET do).
    pub fn insert(&mut self, key: Bytes, value: Value) {
        self.expires.remove(&key);
        self.data.insert(key, value);
    }

    /// Store a value, keeping an existing TTL (SET ... KEEPTTL).
    pub fn insert_keep_ttl(&mut self, key: Bytes, value: Value) {
        self.expire_if_due(&key);
        self.data.insert(key, value);
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Value> {
        self.expire_if_due(key);
        self.expires.remove(key);
        self.data.remove(key)
    }

    /// Delete `key` if it is an aggregate (list, set, hash) with no elements.
    pub fn cleanup(&mut self, key: &[u8]) {
        if self.data.get(key).is_some_and(Value::is_empty_aggregate) {
            self.data.remove(key);
            self.expires.remove(key);
        }
    }

    /// Get the value, or create it with `make` if the key is missing.
    pub fn get_or_insert_with(&mut self, key: &Bytes, make: impl FnOnce() -> Value) -> &mut Value {
        self.expire_if_due(key);
        self.data.entry(key.clone()).or_insert_with(make)
    }

    // ---- typed access --------------------------------------------------

    pub fn string(&mut self, key: &[u8]) -> Result<Option<&Bytes>, DbError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Str(s)) => Ok(Some(s)),
            Some(_) => Err(DbError::WrongType),
        }
    }

    pub fn list(&mut self, key: &[u8]) -> Result<Option<&mut VecDeque<Bytes>>, DbError> {
        match self.get_mut(key) {
            None => Ok(None),
            Some(Value::List(l)) => Ok(Some(l)),
            Some(_) => Err(DbError::WrongType),
        }
    }

    pub fn set(&mut self, key: &[u8]) -> Result<Option<&mut HashSet<Bytes>>, DbError> {
        match self.get_mut(key) {
            None => Ok(None),
            Some(Value::Set(s)) => Ok(Some(s)),
            Some(_) => Err(DbError::WrongType),
        }
    }

    pub fn hash(&mut self, key: &[u8]) -> Result<Option<&mut HashMap<Bytes, Bytes>>, DbError> {
        match self.get_mut(key) {
            None => Ok(None),
            Some(Value::Hash(h)) => Ok(Some(h)),
            Some(_) => Err(DbError::WrongType),
        }
    }

    pub fn list_or_new(&mut self, key: &Bytes) -> Result<&mut VecDeque<Bytes>, DbError> {
        match self.get_or_insert_with(key, || Value::List(VecDeque::new())) {
            Value::List(l) => Ok(l),
            _ => Err(DbError::WrongType),
        }
    }

    pub fn set_or_new(&mut self, key: &Bytes) -> Result<&mut HashSet<Bytes>, DbError> {
        match self.get_or_insert_with(key, || Value::Set(HashSet::new())) {
            Value::Set(s) => Ok(s),
            _ => Err(DbError::WrongType),
        }
    }

    pub fn hash_or_new(&mut self, key: &Bytes) -> Result<&mut HashMap<Bytes, Bytes>, DbError> {
        match self.get_or_insert_with(key, || Value::Hash(HashMap::new())) {
            Value::Hash(h) => Ok(h),
            _ => Err(DbError::WrongType),
        }
    }

    // ---- expiry --------------------------------------------------------

    pub fn deadline(&mut self, key: &[u8]) -> Option<Instant> {
        self.expire_if_due(key);
        self.expires.get(key).copied()
    }

    /// Set a deadline on an existing key. A deadline in the past deletes it.
    pub fn set_deadline(&mut self, key: &Bytes, at: Instant) {
        if at <= self.now {
            self.remove(key);
        } else if self.data.contains_key(key) {
            self.expires.insert(key.clone(), at);
        }
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        self.expire_if_due(key);
        self.expires.remove(key).is_some()
    }

    /// Delete up to `max` keys whose TTL has passed (background "active
    /// expiry", so keys nobody reads again don't stay in memory forever).
    pub fn sweep_expired(&mut self, max: usize) -> usize {
        let now = self.now;
        let due: Vec<Bytes> = self
            .expires
            .iter()
            .filter(|&(_, &t)| t <= now)
            .take(max)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &due {
            self.expires.remove(k);
            self.data.remove(k);
        }
        due.len()
    }

    // ---- whole keyspace ------------------------------------------------

    /// All live keys (expired ones are deleted on the way).
    pub fn keys(&mut self) -> Vec<Bytes> {
        self.sweep_expired(usize::MAX);
        self.data.keys().cloned().collect()
    }

    pub fn len(&mut self) -> usize {
        self.sweep_expired(usize::MAX);
        self.data.len()
    }

    pub fn stored_keys(&self) -> usize {
        self.data.len()
    }

    pub fn flush(&mut self) {
        self.data.clear();
        self.expires.clear();
    }

    // ---- lists and blocking --------------------------------------------

    /// Push every value onto a list (creating it), remember the key so that
    /// blocked clients are served at the end of the critical section, and
    /// return the new length.
    pub fn push(&mut self, key: &Bytes, values: Vec<Bytes>, end: End) -> Result<usize, DbError> {
        if values.is_empty() {
            // Not reachable from the protocol (arity check), but never leave
            // an empty list behind.
            return Ok(self.list(key)?.map_or(0, |l| l.len()));
        }
        let list = self.list_or_new(key)?;
        for v in values {
            end.push(list, v);
        }
        let len = list.len();
        self.mark_ready(key);
        Ok(len)
    }

    /// Note that `key` received list elements in this critical section.
    pub fn mark_ready(&mut self, key: &Bytes) {
        if self.waiters.has_waiters(key) && !self.ready.contains(key) {
            self.ready.push(key.clone());
        }
    }

    /// BLPOP's non-blocking step: pop from the first non-empty key, in the
    /// client's key order. A non-list key before any data is WRONGTYPE.
    pub fn try_pop(&mut self, keys: &[Bytes], end: End) -> Result<Option<Popped>, DbError> {
        for key in keys {
            let Some(list) = self.list(key)? else {
                continue;
            };
            let v = end.pop(list);
            self.cleanup(key);
            if let Some(v) = v {
                return Ok(Some((key.clone(), v)));
            }
        }
        Ok(None)
    }

    pub fn register(&mut self, keys: &[Bytes], end: End) -> WaitTicket {
        self.waiters.register(keys, end)
    }

    /// Returns true if the waiter was still registered (never served).
    pub fn cancel(&mut self, id: u64) -> bool {
        self.waiters.remove(id).is_some()
    }

    pub fn waiter_count(&self) -> usize {
        self.waiters.len()
    }

    /// Serve blocked clients on every key pushed to in this critical section.
    pub fn serve_ready(&mut self) {
        for key in std::mem::take(&mut self.ready) {
            self.serve(&key);
        }
    }

    /// Hand elements of `key` to its waiters, oldest first, until either the
    /// list or the queue is empty. Must run in the same critical section as
    /// the write that made `key` non-empty.
    fn serve(&mut self, key: &Bytes) {
        while self.has_elements(key) {
            let Some(waiter) = self.waiters.take_oldest(key) else {
                break;
            };

            // Re-run BLPOP's own semantics for this client, as Redis does:
            // it pops from the first non-empty key *in its own key order*.
            // Earlier keys cannot hold a non-empty list (it would have been
            // served already), but one may now hold another type (e.g. via
            // SET) — then the client gets WRONGTYPE, as a fresh BLPOP would.
            if self.earlier_key_is_not_a_list(&waiter.keys, key) {
                let _ = waiter.tx.send(Err(DbError::WrongType));
                continue;
            }

            let Some(Value::List(list)) = self.data.get_mut(key) else {
                break;
            };
            let Some(v) = waiter.end.pop(list) else {
                break;
            };
            if let Err(Ok((_, v))) = waiter.tx.send(Ok((key.clone(), v))) {
                // The receiver is gone (its task died without cancelling).
                // Put the element back where it came from; nothing observable
                // happened, and the next waiter gets a chance.
                waiter.end.push(list, v);
            }
        }
        self.cleanup(key);
    }

    fn has_elements(&mut self, key: &Bytes) -> bool {
        matches!(self.get(key), Some(Value::List(l)) if !l.is_empty())
    }

    fn earlier_key_is_not_a_list(&mut self, keys: &[Bytes], key: &Bytes) -> bool {
        let earlier: Vec<Bytes> = keys.iter().take_while(|k| *k != key).cloned().collect();
        earlier
            .iter()
            .any(|k| matches!(self.get(k), Some(v) if !matches!(v, Value::List(_))))
    }

    // ---- invariants ----------------------------------------------------

    /// 1. No key holds an empty list, set or hash.
    /// 2. Every TTL belongs to an existing key.
    /// 3. The waiter registry is internally consistent.
    /// 4. No waiter is queued on a key that holds a non-empty list (a push
    ///    would have served it).
    pub fn check_invariants(&self) -> Result<(), String> {
        for (k, v) in &self.data {
            if v.is_empty_aggregate() {
                return Err(format!("key {k:?} holds an empty {}", v.type_name()));
            }
            if let Value::List(_) = v
                && self.waiters.has_waiters(k)
            {
                return Err(format!("key {k:?} is non-empty but has waiters queued"));
            }
        }
        if let Some(k) = self.expires.keys().find(|k| !self.data.contains_key(*k)) {
            return Err(format!("TTL left for missing key {k:?}"));
        }
        if !self.ready.is_empty() {
            return Err("ready keys left after a critical section".into());
        }
        self.waiters.check_invariants()
    }
}
