//! Turning frames into bytes.

use super::Frame;
use bytes::{BufMut, Bytes, BytesMut};

/// Encode a frame onto the end of `out`.
pub fn encode(frame: &Frame, out: &mut BytesMut) {
    match frame {
        Frame::Simple(s) => line(out, b'+', s.as_bytes()),
        Frame::Error(s) => line(out, b'-', s.as_bytes()),
        Frame::Integer(n) => line(out, b':', n.to_string().as_bytes()),
        Frame::Bulk(b) => {
            line(out, b'$', b.len().to_string().as_bytes());
            out.put_slice(b);
            out.put_slice(b"\r\n");
        }
        Frame::Null => out.put_slice(b"$-1\r\n"),
        Frame::Array(items) => {
            line(out, b'*', items.len().to_string().as_bytes());
            for item in items {
                encode(item, out);
            }
        }
        Frame::NullArray => out.put_slice(b"*-1\r\n"),
    }
}

/// Encode a command (an array of bulk strings), as a client sends it.
pub fn encode_command<A: AsRef<[u8]>>(args: &[A], out: &mut BytesMut) {
    let frame = Frame::Array(
        args.iter()
            .map(|a| Frame::Bulk(Bytes::copy_from_slice(a.as_ref())))
            .collect(),
    );
    encode(&frame, out);
}

/// A one-line value: tag, body, CRLF.
fn line(out: &mut BytesMut, tag: u8, body: &[u8]) {
    out.put_u8(tag);
    // Simple strings and errors must not contain CR/LF; replace them so a
    // message can never break framing.
    out.extend(
        body.iter()
            .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
    );
    out.put_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::parse_frame;

    fn roundtrip(f: Frame) {
        let bytes = f.to_bytes();
        assert_eq!(parse_frame(&bytes), Ok(Some((f, bytes.len()))));
    }

    #[test]
    fn roundtrips_every_type() {
        roundtrip(Frame::Simple("OK".into()));
        roundtrip(Frame::Error("ERR boom".into()));
        roundtrip(Frame::Integer(-42));
        roundtrip(Frame::Integer(i64::MAX));
        roundtrip(Frame::Integer(i64::MIN));
        roundtrip(Frame::Bulk(Bytes::from_static(b"hello")));
        roundtrip(Frame::Bulk(Bytes::from_static(b"")));
        roundtrip(Frame::Bulk(Bytes::from_static(b"a\r\nb\0c")));
        roundtrip(Frame::Null);
        roundtrip(Frame::NullArray);
        roundtrip(Frame::Array(vec![]));
        roundtrip(Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"k")),
            Frame::Array(vec![Frame::Integer(1), Frame::Null]),
        ]));
    }

    #[test]
    fn line_values_cannot_break_framing() {
        assert_eq!(&Frame::error("ERR a\r\nb").to_bytes()[..], b"-ERR a  b\r\n");
    }

    #[test]
    fn commands_are_arrays_of_bulk_strings() {
        let mut out = BytesMut::new();
        encode_command(&["GET", "k"], &mut out);
        assert_eq!(&out[..], b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    }
}
