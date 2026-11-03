//! The registry of clients blocked in BLPOP/BRPOP.
//!
//! Each waiter has an id from an increasing counter. A waiter is stored once
//! (in `waiters`) and its id is queued under each of its keys. Because ids
//! only increase, the smallest id in a key's set is the oldest waiter, so the
//! set *is* the FIFO queue, and removing a cancelled waiter is O(log n).

use super::{Delivery, End, WaitTicket};
use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use tokio::sync::oneshot;

/// One blocked client.
pub(super) struct Waiter {
    /// Keys in the order the client gave them.
    pub keys: Vec<Bytes>,
    pub end: End,
    /// The hand-off channel. Sending never blocks.
    pub tx: oneshot::Sender<Delivery>,
}

#[derive(Default)]
pub(super) struct WaiterRegistry {
    /// Every live (not yet served, not cancelled) waiter, by id.
    waiters: HashMap<u64, Waiter>,
    /// Per key: ids of waiters blocked on it, oldest first.
    queues: HashMap<Bytes, BTreeSet<u64>>,
    next_id: u64,
}

impl WaiterRegistry {
    /// Register a new waiter on `keys` and return its ticket.
    pub fn register(&mut self, keys: &[Bytes], end: End) -> WaitTicket {
        let id = self.next_id;
        self.next_id += 1;
        let (tx, rx) = oneshot::channel();
        for key in keys {
            // A set, so `BLPOP q q 0` queues once.
            self.queues.entry(key.clone()).or_default().insert(id);
        }
        let keys = keys.to_vec();
        self.waiters.insert(id, Waiter { keys, end, tx });
        WaitTicket { id, rx }
    }

    /// Remove waiter `id` from the registry and from every key's queue.
    pub fn remove(&mut self, id: u64) -> Option<Waiter> {
        let waiter = self.waiters.remove(&id)?;
        for key in &waiter.keys {
            if let Some(q) = self.queues.get_mut(key) {
                q.remove(&id);
                if q.is_empty() {
                    self.queues.remove(key);
                }
            }
        }
        Some(waiter)
    }

    /// Remove and return the oldest waiter blocked on `key`.
    pub fn take_oldest(&mut self, key: &Bytes) -> Option<Waiter> {
        let id = *self.queues.get(key)?.first()?;
        self.remove(id)
    }

    /// Is anyone blocked on `key`?
    pub fn has_waiters(&self, key: &Bytes) -> bool {
        self.queues.contains_key(key)
    }

    /// Number of blocked clients.
    pub fn len(&self) -> usize {
        self.waiters.len()
    }

    /// Queues and the waiter table agree with each other.
    pub fn check_invariants(&self) -> Result<(), String> {
        for (k, q) in &self.queues {
            if q.is_empty() {
                return Err(format!("empty queue left for {k:?}"));
            }
            for id in q {
                let Some(w) = self.waiters.get(id) else {
                    return Err(format!("queue {k:?} holds dead waiter {id}"));
                };
                if !w.keys.contains(k) {
                    return Err(format!("waiter {id} queued on foreign key {k:?}"));
                }
            }
        }
        for (id, w) in &self.waiters {
            for k in &w.keys {
                if !self.queues.get(k).is_some_and(|q| q.contains(id)) {
                    return Err(format!("waiter {id} missing from queue {k:?}"));
                }
            }
        }
        Ok(())
    }
}
