//! Building reply frames from command results.

use crate::db::{Delivery, Popped};
use crate::error;
use crate::resp::Frame;

/// `[key, value]` for a successful BLPOP/BRPOP.
pub(super) fn popped((key, value): Popped) -> Frame {
    Frame::Array(vec![Frame::Bulk(key), Frame::Bulk(value)])
}

/// What a served blocked client receives: a value or WRONGTYPE.
pub(super) fn delivery(d: Delivery) -> Frame {
    match d {
        Ok(p) => popped(p),
        Err(e) => error::db(e),
    }
}
