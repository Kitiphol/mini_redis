//! `micro-redis-cli` — a tiny redis-cli built on this crate's own RESP client.
//!
//! ```text
//! micro-redis-cli [-h HOST] [-p PORT] [-n DB] [COMMAND ARGS...]
//! ```
//! With a command: run it once and print the reply.
//! Without: interactive prompt (one command per line, quotes allowed).

use micro_redis::client::{Client, ClientError};
use micro_redis::resp::{Frame, split_args};
use std::io::{BufRead, Write};
use std::process::ExitCode;

const USAGE: &str = "\
micro-redis-cli — talk RESP2 to micro-redis (or any Redis)

USAGE:
    micro-redis-cli [-h HOST] [-p PORT] [-n DB] [COMMAND [ARG ...]]

    -h HOST   server host        [default: 127.0.0.1]
    -p PORT   server port        [default: 6379]
    -n DB     database number; sent as SELECT right after connecting
    --help    print this help

Without a COMMAND, starts an interactive prompt. Type `quit` to leave.";

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mut host = "127.0.0.1".to_string();
    let mut port = 6379u16;
    let mut db: Option<String> = None;
    let mut command: Vec<String> = Vec::new();

    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if !command.is_empty() {
            command.push(a);
            continue;
        }
        match a.as_str() {
            "-h" => host = it.next().unwrap_or(host),
            "-p" => match it.next().and_then(|p| p.parse().ok()) {
                Some(p) => port = p,
                None => return usage("invalid -p value"),
            },
            "-n" => db = it.next(),
            "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            _ => command.push(a),
        }
    }

    let mut client = match Client::connect((host.as_str(), port)).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Could not connect to {host}:{port}: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(n) = db {
        match client.cmd(&["SELECT", n.as_str()]).await {
            Ok(Frame::Simple(_)) => {}
            Ok(other) => {
                eprintln!("SELECT {n} failed: {other}");
                return ExitCode::FAILURE;
            }
            Err(e) => return fail(e),
        }
    }

    if !command.is_empty() {
        return match client.cmd(&command).await {
            Ok(reply) => {
                println!("{reply}");
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        };
    }

    // Interactive mode.
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        print!("{host}:{port}> ");
        let _ = std::io::stdout().flush();
        line.clear();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => return ExitCode::SUCCESS,
            Ok(_) => {}
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
        let args = match split_args(line.trim_end().as_bytes()) {
            Ok(a) => a,
            Err(e) => {
                println!("Invalid argument(s): {e}");
                continue;
            }
        };
        if args.is_empty() {
            continue;
        }
        if args[0].eq_ignore_ascii_case(b"quit") || args[0].eq_ignore_ascii_case(b"exit") {
            return ExitCode::SUCCESS;
        }
        match client.cmd(&args).await {
            Ok(reply) => println!("{reply}"),
            Err(e) => return fail(e),
        }
    }
}

fn usage(msg: &str) -> ExitCode {
    eprintln!("error: {msg}\n\n{USAGE}");
    ExitCode::from(2)
}

fn fail(e: ClientError) -> ExitCode {
    eprintln!("error: {e}");
    ExitCode::FAILURE
}
