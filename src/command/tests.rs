use super::*;

fn p(args: &[&str]) -> Parsed {
    parse(Frame::Array(
        args.iter()
            .map(|a| Frame::Bulk(Bytes::copy_from_slice(a.as_bytes())))
            .collect(),
    ))
}

fn err(args: &[&str]) -> String {
    match p(args) {
        Parsed::Error(Frame::Error(e)) => e,
        other => panic!("expected error, got {other:?}"),
    }
}

fn data(op: Op, args: &[&str]) -> Parsed {
    Parsed::Command(Command::Data(DataCmd {
        op,
        args: args
            .iter()
            .map(|a| Bytes::copy_from_slice(a.as_bytes()))
            .collect(),
    }))
}

#[test]
fn names_are_case_insensitive() {
    assert_eq!(p(&["pInG"]), Parsed::Command(Command::Ping(None)));
    assert_eq!(p(&["Get", "k"]), data(Op::Get, &["k"]));
    assert_eq!(p(&["hGETall", "h"]), data(Op::HGetAll, &["h"]));
}

#[test]
fn arity_errors() {
    for (args, name) in [
        (&["GET"][..], "get"),
        (&["SET", "k"], "set"),
        (&["LPUSH", "k"], "lpush"),
        (&["BLPOP", "k"], "blpop"),
        (&["EXISTS"], "exists"),
        (&["PING", "a", "b"], "ping"),
        (&["SELECT"], "select"),
        (&["HGET", "h"], "hget"),
        (&["SADD", "s"], "sadd"),
        (&["MULTI", "x"], "multi"),
        (&["EVAL", "return 1"], "eval"),
    ] {
        assert_eq!(
            err(args),
            format!("ERR wrong number of arguments for '{name}' command")
        );
    }
}

#[test]
fn set_options_are_checked_when_run() {
    // Like Redis, option *values* are checked when the command runs.
    assert_eq!(
        p(&["SET", "k", "v", "EX", "10"]),
        data(Op::Set, &["k", "v", "EX", "10"])
    );
    assert_eq!(p(&["SET", "k", "v"]), data(Op::Set, &["k", "v"]));
}

#[test]
fn select_validation() {
    assert_eq!(p(&["SELECT", "15"]), Parsed::Command(Command::Select(15)));
    assert_eq!(err(&["SELECT", "16"]), "ERR DB index is out of range");
    assert_eq!(err(&["SELECT", "-1"]), "ERR DB index is out of range");
    assert_eq!(
        err(&["SELECT", "abc"]),
        "ERR value is not an integer or out of range"
    );
}

#[test]
fn blocking_pop_validation() {
    assert_eq!(
        p(&["BLPOP", "a", "b", "0.5"]),
        data(Op::BLPop, &["a", "b", "0.5"])
    );
    assert_eq!(p(&["brpop", "a", "0"]), data(Op::BRPop, &["a", "0"]));
    assert_eq!(
        err(&["BLPOP", "a", "b", "c", "d", "e", "f", "0"]),
        "ERR BLPOP accepts at most 5 keys"
    );
    assert_eq!(
        err(&["brpop", "a", "b", "c", "d", "e", "f", "0"]),
        "ERR BRPOP accepts at most 5 keys"
    );
    assert!(matches!(
        p(&["BLPOP", "a", "b", "c", "d", "e", "0"]),
        Parsed::Command(_)
    ));
    assert_eq!(err(&["BLPOP", "a", "-1"]), "ERR timeout is negative");
    for bad in ["abc", "nan", " 1", "1s", ""] {
        assert_eq!(
            err(&["BLPOP", "a", bad]),
            "ERR timeout is not a float or out of range",
            "timeout {bad:?}"
        );
    }
    assert!(matches!(p(&["BLPOP", "a", "inf"]), Parsed::Command(_)));
}

#[test]
fn eval_validation() {
    assert_eq!(
        p(&["EVAL", "return 1", "1", "k", "a"]),
        Parsed::Command(Command::Eval(ScriptCall {
            script: ScriptRef::Source(Bytes::from_static(b"return 1")),
            keys: vec![Bytes::from_static(b"k")],
            args: vec![Bytes::from_static(b"a")],
        }))
    );
    assert!(matches!(
        p(&["EVALSHA", "ABCDEF", "0"]),
        Parsed::Command(Command::Eval(ScriptCall { script: ScriptRef::Sha(s), .. })) if s == "abcdef"
    ));
    assert_eq!(
        err(&["EVAL", "x", "-1"]),
        "ERR Number of keys can't be negative"
    );
    assert_eq!(
        err(&["EVAL", "x", "3", "a"]),
        "ERR Number of keys can't be greater than number of args"
    );
    assert_eq!(
        err(&["EVAL", "x", "z"]),
        "ERR value is not an integer or out of range"
    );
}

#[test]
fn script_and_client_subcommands() {
    assert_eq!(
        p(&["SCRIPT", "LOAD", "return 1"]),
        Parsed::Command(Command::ScriptLoad(Bytes::from_static(b"return 1")))
    );
    assert_eq!(
        p(&["script", "flush"]),
        Parsed::Command(Command::ScriptFlush)
    );
    assert!(err(&["SCRIPT", "BOGUS"]).starts_with("ERR unknown subcommand 'BOGUS'"));
    assert_eq!(
        p(&["CLIENT", "SETINFO", "LIB-NAME", "x"]),
        Parsed::Command(Command::Client(ClientCmd::SetInfo))
    );
    assert!(matches!(p(&["CLIENT", "SETINFO", "x"]), Parsed::Command(_)).not());
}

#[test]
fn unknown_command_message() {
    assert_eq!(
        err(&["FOO", "x"]),
        "ERR unknown command 'FOO', with args beginning with: 'x' "
    );
}

#[test]
fn empty_request_is_ignored() {
    assert_eq!(parse(Frame::Array(vec![])), Parsed::Empty);
}

#[test]
fn non_array_request_is_an_error() {
    assert!(matches!(parse(Frame::Integer(1)), Parsed::Error(_)));
    assert!(matches!(
        parse(Frame::Array(vec![Frame::Null])),
        Parsed::Error(_)
    ));
}

trait Not {
    fn not(self) -> bool;
}
impl Not for bool {
    fn not(self) -> bool {
        !self
    }
}
