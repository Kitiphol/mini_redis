//! Client requests, including *inline commands*.
//!
//! Client libraries and `redis-cli` send a RESP array. People typing into
//! `nc` or `telnet` send a plain line such as `SET greeting "hello world"`,
//! which real Redis also accepts. Both become an array of bulk strings.

use super::Frame;
use super::parse::{MAX_LINE_LEN, ParseResult, ProtocolError, parse_frame};
use bytes::Bytes;

/// Parse one client request from the start of `buf`.
pub fn parse_request(buf: &[u8]) -> ParseResult<Frame> {
    match buf.first() {
        None => Ok(None),
        Some(b'*') => parse_frame(buf),
        Some(_) => parse_inline(buf),
    }
}

fn parse_inline(buf: &[u8]) -> ParseResult<Frame> {
    let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        if buf.len() > MAX_LINE_LEN {
            return Err(ProtocolError::LineTooLong);
        }
        return Ok(None);
    };
    if nl > MAX_LINE_LEN {
        return Err(ProtocolError::LineTooLong);
    }
    let mut line = &buf[..nl];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let args = split_args(line)?;
    let frame = Frame::Array(args.into_iter().map(Frame::Bulk).collect());
    Ok(Some((frame, nl + 1)))
}

/// Split a line into arguments: whitespace-separated, with single and double
/// quotes, and `\n`, `\r`, `\t` escapes inside double quotes. Also used by
/// `micro-redis-cli` for what the user types.
pub fn split_args(line: &[u8]) -> Result<Vec<Bytes>, ProtocolError> {
    let mut args = Vec::new();
    let mut i = 0;
    while i < line.len() {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        let (arg, next) = if line[i] == b'"' || line[i] == b'\'' {
            quoted(line, i)?
        } else {
            bare(line, i)
        };
        args.push(Bytes::from(arg));
        i = next;
    }
    Ok(args)
}

/// An unquoted word starting at `i`.
fn bare(line: &[u8], mut i: usize) -> (Vec<u8>, usize) {
    let start = i;
    while i < line.len() && !line[i].is_ascii_whitespace() {
        i += 1;
    }
    (line[start..i].to_vec(), i)
}

/// A quoted word whose opening quote is at `i`.
fn quoted(line: &[u8], mut i: usize) -> Result<(Vec<u8>, usize), ProtocolError> {
    let q = line[i];
    i += 1;
    let mut cur = Vec::new();
    loop {
        match line.get(i) {
            None => return Err(ProtocolError::UnbalancedQuotes),
            Some(&c) if c == q => {
                i += 1;
                break;
            }
            Some(b'\\') if q == b'"' && i + 1 < line.len() => {
                cur.push(match line[i + 1] {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    other => other,
                });
                i += 2;
            }
            Some(&c) => {
                cur.push(c);
                i += 1;
            }
        }
    }
    // A closing quote must be followed by whitespace or the end of the line.
    if i < line.len() && !line[i].is_ascii_whitespace() {
        return Err(ProtocolError::UnbalancedQuotes);
    }
    Ok((cur, i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_commands() {
        let (f, n) = parse_request(b"set  k \"hello world\"\r\nGET k\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(n, 22);
        assert_eq!(
            f,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"set")),
                Frame::Bulk(Bytes::from_static(b"k")),
                Frame::Bulk(Bytes::from_static(b"hello world")),
            ])
        );
        assert_eq!(parse_request(b"PING"), Ok(None));
        assert_eq!(
            parse_request(b"SET k \"oops\r\n"),
            Err(ProtocolError::UnbalancedQuotes)
        );
    }

    #[test]
    fn quotes_and_escapes() {
        let args = split_args(br#"a 'b c' "d\te" "x\"y""#).unwrap();
        assert_eq!(
            args,
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b c"),
                Bytes::from_static(b"d\te"),
                Bytes::from_static(b"x\"y"),
            ]
        );
        assert_eq!(split_args(b"\"a\"b"), Err(ProtocolError::UnbalancedQuotes));
        assert_eq!(split_args(b"   "), Ok(vec![]));
    }

    #[test]
    fn resp_arrays_still_go_to_the_resp_parser() {
        let (f, _) = parse_request(b"*1\r\n$4\r\nPING\r\n").unwrap().unwrap();
        assert_eq!(
            f,
            Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
        );
    }
}
