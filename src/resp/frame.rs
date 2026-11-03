//! The RESP2 value type.

use bytes::{Bytes, BytesMut};
use std::fmt;

/// One RESP2 value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n` — binary safe.
    Bulk(Bytes),
    /// `$-1\r\n` — the RESP2 nil bulk string (GET on a missing key).
    Null,
    /// `*2\r\n...` — an array of frames.
    Array(Vec<Frame>),
    /// `*-1\r\n` — the RESP2 nil array (BLPOP timeout).
    NullArray,
}

impl Frame {
    /// An error reply.
    pub fn error(msg: impl Into<String>) -> Frame {
        Frame::Error(msg.into())
    }

    /// `+OK`
    pub fn ok() -> Frame {
        Frame::Simple("OK".into())
    }

    /// The encoded bytes of this frame.
    pub fn to_bytes(&self) -> BytesMut {
        let mut out = BytesMut::new();
        super::encode(self, &mut out);
        out
    }
}

/// Human-readable rendering in the style of `redis-cli`.
impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        render(self, f, 0)
    }
}

fn render(frame: &Frame, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
    match frame {
        Frame::Simple(s) => write!(f, "{s}"),
        Frame::Error(e) => write!(f, "(error) {e}"),
        Frame::Integer(n) => write!(f, "(integer) {n}"),
        Frame::Bulk(b) => write!(f, "\"{}\"", b.escape_ascii()),
        Frame::Null | Frame::NullArray => write!(f, "(nil)"),
        Frame::Array(items) if items.is_empty() => write!(f, "(empty array)"),
        Frame::Array(items) => {
            let width = items.len().to_string().len();
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    write!(f, "\n{:indent$}", "")?;
                }
                write!(f, "{:>width$}) ", i + 1)?;
                render(item, f, indent + width + 2)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_like_redis_cli() {
        let f = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"q")),
            Frame::Bulk(Bytes::from_static(b"x")),
        ]);
        assert_eq!(f.to_string(), "1) \"q\"\n2) \"x\"");
        assert_eq!(Frame::Null.to_string(), "(nil)");
        assert_eq!(Frame::Integer(3).to_string(), "(integer) 3");
        assert_eq!(Frame::Array(vec![]).to_string(), "(empty array)");
    }
}
