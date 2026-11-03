//! Stored values and list helpers.

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

/// Which end of a list an operation works on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// LPUSH / BLPOP
    Left,
    /// RPUSH / BRPOP
    Right,
}

/// A stored value. Lists, sets and hashes are never empty: like Redis, the
/// key is deleted when its last element goes (see `State::cleanup`).
#[derive(Debug, Clone)]
pub enum Value {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
}

impl Value {
    /// The name `TYPE` reports.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::Hash(_) => "hash",
        }
    }

    /// An aggregate with no elements left (it must be deleted).
    pub fn is_empty_aggregate(&self) -> bool {
        match self {
            Value::Str(_) => false,
            Value::List(l) => l.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::Hash(h) => h.is_empty(),
        }
    }
}

impl End {
    /// Remove one element from this end of `list`.
    pub fn pop(self, list: &mut VecDeque<Bytes>) -> Option<Bytes> {
        match self {
            End::Left => list.pop_front(),
            End::Right => list.pop_back(),
        }
    }

    /// Add one element at this end of `list`.
    pub fn push(self, list: &mut VecDeque<Bytes>, v: Bytes) {
        match self {
            End::Left => list.push_front(v),
            End::Right => list.push_back(v),
        }
    }

    /// `LEFT` / `RIGHT` arguments of LMOVE (case-insensitive).
    pub fn parse(arg: &[u8]) -> Option<End> {
        if arg.eq_ignore_ascii_case(b"left") {
            Some(End::Left)
        } else if arg.eq_ignore_ascii_case(b"right") {
            Some(End::Right)
        } else {
            None
        }
    }
}
