//! The incremental RESP2 parser.

use super::Frame;
use bytes::Bytes;

/// Largest bulk string accepted (same as Redis' default `proto-max-bulk-len`).
pub const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Largest array accepted.
pub const MAX_ARRAY_LEN: usize = 1024 * 1024;
/// Largest header line (`*3`, `$5`, `+OK`, ...) or inline command.
pub const MAX_LINE_LEN: usize = 64 * 1024;
/// Maximum nesting depth of arrays.
pub const MAX_DEPTH: usize = 32;

/// Bytes that can never become a valid frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    #[error("invalid type byte '{0}'")]
    InvalidType(char),
    #[error("invalid integer")]
    InvalidInteger,
    #[error("invalid bulk length")]
    InvalidBulkLength,
    #[error("invalid multibulk length")]
    InvalidArrayLength,
    #[error("expected '\\r\\n' after bulk data")]
    MissingCrlf,
    #[error("line too long")]
    LineTooLong,
    #[error("arrays nested too deeply")]
    TooDeep,
    #[error("unbalanced quotes in request")]
    UnbalancedQuotes,
}

/// `Ok(Some((value, bytes_consumed)))`, `Ok(None)` (incomplete) or an error.
pub type ParseResult<T> = Result<Option<(T, usize)>, ProtocolError>;

/// Parse one complete RESP2 frame from the start of `buf`.
pub fn parse_frame(buf: &[u8]) -> ParseResult<Frame> {
    parse_at(buf, 0, 0)
}

/// Strict ASCII integer parser, matching Redis' `string2ll`: an optional
/// `-`, then digits with no leading zero (so `01` and `-0` are rejected),
/// fitting in an i64.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s == b"0" {
        return Some(0);
    }
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    if digits.is_empty() || digits.len() > 19 || digits[0] == b'0' {
        return None;
    }
    let mut v: i64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?;
        v = if neg {
            v.checked_sub(i64::from(d - b'0'))?
        } else {
            v.checked_add(i64::from(d - b'0'))?
        };
    }
    Some(v)
}

/// Find `\r\n` starting at `start`; returns the index of `\r`.
fn find_crlf(buf: &[u8], start: usize) -> Result<Option<usize>, ProtocolError> {
    let hay = &buf[start..];
    match hay.windows(2).position(|w| w == b"\r\n") {
        Some(i) if i > MAX_LINE_LEN => Err(ProtocolError::LineTooLong),
        Some(i) => Ok(Some(start + i)),
        None if hay.len() > MAX_LINE_LEN => Err(ProtocolError::LineTooLong),
        None => Ok(None),
    }
}

/// Parse a frame starting at byte `pos`; returns the frame and the index
/// just past it.
fn parse_at(buf: &[u8], pos: usize, depth: usize) -> ParseResult<Frame> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::TooDeep);
    }
    let Some(&tag) = buf.get(pos) else {
        return Ok(None);
    };
    let Some(eol) = find_crlf(buf, pos + 1)? else {
        return Ok(None);
    };
    let line = &buf[pos + 1..eol];
    let after = eol + 2;
    match tag {
        b'+' => Ok(Some((Frame::Simple(lossy(line)), after))),
        b'-' => Ok(Some((Frame::Error(lossy(line)), after))),
        b':' => {
            let n = parse_i64(line).ok_or(ProtocolError::InvalidInteger)?;
            Ok(Some((Frame::Integer(n), after)))
        }
        b'$' => parse_bulk(buf, line, after),
        b'*' => parse_array(buf, line, after, depth),
        other => Err(ProtocolError::InvalidType(printable(other))),
    }
}

fn parse_bulk(buf: &[u8], header: &[u8], after: usize) -> ParseResult<Frame> {
    let n = parse_i64(header).ok_or(ProtocolError::InvalidBulkLength)?;
    if n == -1 {
        return Ok(Some((Frame::Null, after)));
    }
    let len = usize::try_from(n).map_err(|_| ProtocolError::InvalidBulkLength)?;
    if len > MAX_BULK_LEN {
        return Err(ProtocolError::InvalidBulkLength);
    }
    // Only look at the payload once all of it (plus CRLF) is here.
    let end = after + len;
    if buf.len() < end + 2 {
        return Ok(None);
    }
    if &buf[end..end + 2] != b"\r\n" {
        return Err(ProtocolError::MissingCrlf);
    }
    let data = Bytes::copy_from_slice(&buf[after..end]);
    Ok(Some((Frame::Bulk(data), end + 2)))
}

fn parse_array(buf: &[u8], header: &[u8], after: usize, depth: usize) -> ParseResult<Frame> {
    let n = parse_i64(header).ok_or(ProtocolError::InvalidArrayLength)?;
    if n == -1 {
        return Ok(Some((Frame::NullArray, after)));
    }
    let len = usize::try_from(n).map_err(|_| ProtocolError::InvalidArrayLength)?;
    if len > MAX_ARRAY_LEN {
        return Err(ProtocolError::InvalidArrayLength);
    }
    // Do not pre-allocate `len` slots: the length is untrusted.
    let mut items = Vec::with_capacity(len.min(16));
    let mut cur = after;
    for _ in 0..len {
        match parse_at(buf, cur, depth + 1)? {
            Some((item, next)) => {
                items.push(item);
                cur = next;
            }
            None => return Ok(None),
        }
    }
    Ok(Some((Frame::Array(items), cur)))
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn printable(b: u8) -> char {
    if b.is_ascii_graphic() { b as char } else { '?' }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_prefix_of_a_valid_frame_is_incomplete() {
        let bytes = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nva\r\nl\r\n";
        for cut in 0..bytes.len() {
            assert_eq!(parse_frame(&bytes[..cut]), Ok(None), "cut at {cut}");
        }
        let (f, n) = parse_frame(bytes).unwrap().unwrap();
        assert_eq!(n, bytes.len());
        assert_eq!(
            f,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"SET")),
                Frame::Bulk(Bytes::from_static(b"k")),
                Frame::Bulk(Bytes::from_static(b"va\r\nl")),
            ])
        );
    }

    #[test]
    fn two_frames_in_one_buffer() {
        let bytes = b"+OK\r\n:5\r\n";
        let (f, n) = parse_frame(bytes).unwrap().unwrap();
        assert_eq!((f, n), (Frame::Simple("OK".into()), 5));
        let (f2, n2) = parse_frame(&bytes[n..]).unwrap().unwrap();
        assert_eq!((f2, n2), (Frame::Integer(5), 4));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_frame(b"?x\r\n"), Err(ProtocolError::InvalidType('?')));
        assert_eq!(parse_frame(b":12a\r\n"), Err(ProtocolError::InvalidInteger));
        assert_eq!(
            parse_frame(b"$-2\r\n"),
            Err(ProtocolError::InvalidBulkLength)
        );
        assert_eq!(
            parse_frame(b"$abc\r\n"),
            Err(ProtocolError::InvalidBulkLength)
        );
        assert_eq!(
            parse_frame(b"*-5\r\n"),
            Err(ProtocolError::InvalidArrayLength)
        );
        assert_eq!(parse_frame(b"$3\r\nabcXY"), Err(ProtocolError::MissingCrlf));
        assert_eq!(
            parse_frame(b"$99999999999\r\n"),
            Err(ProtocolError::InvalidBulkLength)
        );
        assert_eq!(
            parse_frame(b"*99999999999\r\n"),
            Err(ProtocolError::InvalidArrayLength)
        );
        let nested = "*1\r\n".repeat(MAX_DEPTH + 2);
        assert_eq!(parse_frame(nested.as_bytes()), Err(ProtocolError::TooDeep));
    }

    #[test]
    fn huge_announced_length_is_just_incomplete_not_allocated() {
        // 100 MB announced, 3 bytes present: must wait, not allocate.
        assert_eq!(parse_frame(b"$100000000\r\nabc"), Ok(None));
        assert_eq!(parse_frame(b"*1000000\r\n:1\r\n"), Ok(None));
    }

    #[test]
    fn long_line_without_crlf_is_rejected() {
        let mut v = vec![b'+'];
        v.extend(std::iter::repeat_n(b'a', MAX_LINE_LEN + 10));
        assert_eq!(parse_frame(&v), Err(ProtocolError::LineTooLong));
    }

    #[test]
    fn integers_are_strict() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"-0"), None);
        assert_eq!(parse_i64(b"007"), None);
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775808"), None);
        assert_eq!(parse_i64(b""), None);
        assert_eq!(parse_i64(b"-"), None);
        assert_eq!(parse_i64(b"+1"), None);
        assert_eq!(parse_i64(b" 1"), None);
    }

    /// Fuzz-style check: random bytes never panic the parser.
    #[test]
    fn random_bytes_never_panic() {
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let alphabet = b"*$+-:\r\n0123456789-abc";
        for _ in 0..20_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let len = (seed % 40) as usize;
            let mut buf = Vec::with_capacity(len);
            let mut s = seed;
            for _ in 0..len {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                buf.push(alphabet[(s >> 33) as usize % alphabet.len()]);
            }
            let _ = parse_frame(&buf);
            let _ = crate::resp::parse_request(&buf);
        }
    }
}
