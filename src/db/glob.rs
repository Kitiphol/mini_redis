//! Glob-style pattern matching for `KEYS`, following Redis' `stringmatchlen`:
//!
//! * `*` any sequence (including empty), `?` any one byte
//! * `[abc]`, `[a-z]`, `[^a]` character classes
//! * `\x` matches `x` literally

/// Does `string` match `pattern`?
pub fn matches(pattern: &[u8], string: &[u8]) -> bool {
    match_from(pattern, string, 0)
}

fn match_from(mut p: &[u8], mut s: &[u8], depth: usize) -> bool {
    // Guard against pathological patterns like "*****...a".
    if depth > 1000 {
        return false;
    }
    while let Some(&c) = p.first() {
        match c {
            b'*' => {
                while p.len() > 1 && p[1] == b'*' {
                    p = &p[1..];
                }
                if p.len() == 1 {
                    return true;
                }
                for i in 0..=s.len() {
                    if match_from(&p[1..], &s[i..], depth + 1) {
                        return true;
                    }
                }
                return false;
            }
            b'?' => {
                if s.is_empty() {
                    return false;
                }
                s = &s[1..];
                p = &p[1..];
            }
            b'[' => {
                let Some(&ch) = s.first() else {
                    return false;
                };
                let (matched, rest) = class(&p[1..], ch);
                if !matched {
                    return false;
                }
                p = rest;
                s = &s[1..];
            }
            b'\\' if p.len() >= 2 => {
                if s.first() != Some(&p[1]) {
                    return false;
                }
                s = &s[1..];
                p = &p[2..];
            }
            _ => {
                if s.first() != Some(&c) {
                    return false;
                }
                s = &s[1..];
                p = &p[1..];
            }
        }
    }
    s.is_empty()
}

/// Match `ch` against a class whose `[` was already consumed. Returns whether
/// it matched and the pattern after the closing `]`.
fn class(mut p: &[u8], ch: u8) -> (bool, &[u8]) {
    let negate = p.first() == Some(&b'^');
    if negate {
        p = &p[1..];
    }
    let mut hit = false;
    loop {
        match p {
            [] => break,
            [b']', rest @ ..] => {
                p = rest;
                break;
            }
            [b'\\', c, rest @ ..] => {
                hit |= *c == ch;
                p = rest;
            }
            [lo, b'-', hi, rest @ ..] if *hi != b']' => {
                let (lo, hi) = if lo <= hi { (*lo, *hi) } else { (*hi, *lo) };
                hit |= (lo..=hi).contains(&ch);
                p = rest;
            }
            [c, rest @ ..] => {
                hit |= *c == ch;
                p = rest;
            }
        }
    }
    (hit != negate, p)
}

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn patterns() {
        let m = |p: &str, s: &str| matches(p.as_bytes(), s.as_bytes());
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("proc:*", "proc:ab12:3"));
        assert!(!m("proc:*", "job:0001:queue"));
        assert!(m("h?llo", "hello"));
        assert!(!m("h?llo", "hllo"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-b]llo", "hbllo"));
        assert!(m("job:*:queue", "job:0001:queue"));
        assert!(m("a\\*b", "a*b"));
        assert!(!m("a\\*b", "axb"));
        assert!(m("**x", "abcx"));
    }
}
