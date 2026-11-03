//! RESP2 (REdis Serialization Protocol, version 2), written from scratch.
//!
//! * [`frame`]  — the [`Frame`] type and its `redis-cli`-style display
//! * [`parse`]  — the incremental parser and its safety limits
//! * [`inline`] — inline commands (`PING\r\n` typed into `nc`)
//! * [`encode`] — turning frames back into bytes
//!
//! The parser is *incremental*: TCP is a byte stream, so a buffer may hold
//! half a frame, exactly one frame, or several. Parsing returns
//!
//! * `Ok(Some((frame, consumed)))` — a complete frame and how many bytes it used,
//! * `Ok(None)` — not enough bytes yet; read more and call again,
//! * `Err(ProtocolError)` — the bytes can never form a valid frame.
//!
//! It never panics on any input and never allocates based on an untrusted
//! length before the bytes have actually arrived.

pub mod encode;
pub mod frame;
pub mod inline;
pub mod parse;

pub use encode::{encode, encode_command};
pub use frame::Frame;
pub use inline::{parse_request, split_args};
pub use parse::{
    MAX_ARRAY_LEN, MAX_BULK_LEN, MAX_DEPTH, MAX_LINE_LEN, ProtocolError, parse_frame, parse_i64,
};
