//! Command-line configuration for the `micro-redis` server binary.

/// Help text for `micro-redis --help`.
pub const USAGE: &str = "\
micro-redis — a Redis-compatible server (RESP2)

USAGE:
    micro-redis [OPTIONS]

OPTIONS:
    -p, --port <PORT>    TCP port to listen on            [default: 6379]
    -b, --bind <ADDR>    Address to bind                  [default: 0.0.0.0]
                         0.0.0.0 accepts connections from other machines;
                         127.0.0.1 accepts only this machine
    -h, --help           Print this help

Logging is controlled by RUST_LOG (e.g. RUST_LOG=debug shows every command).";

/// Server settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub port: u16,
    pub bind: String,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            port: 6379,
            bind: "0.0.0.0".into(),
        }
    }
}

impl Config {
    /// The `host:port` address to listen on.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }
}

/// What the command line asked for.
#[derive(Debug, PartialEq, Eq)]
pub enum Cli {
    Run(Config),
    Help,
}

/// Parse arguments (without the program name). Accepts both `--port 7000`
/// and `--port=7000`.
pub fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<Cli, String> {
    let mut config = Config::default();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f.to_string(), Some(v.to_string())),
            None => (arg, None),
        };
        let mut value = |name: &str| {
            inline
                .clone()
                .or_else(|| it.next())
                .ok_or_else(|| format!("missing value for {name}"))
        };
        match flag.as_str() {
            "-p" | "--port" => {
                let v = value("--port")?;
                config.port = v.parse().map_err(|_| format!("invalid port '{v}'"))?;
            }
            "-b" | "--bind" => config.bind = value("--bind")?,
            "-h" | "--help" => return Ok(Cli::Help),
            other => return Err(format!("unknown argument '{other}'")),
        }
    }
    Ok(Cli::Run(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, String> {
        parse_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn defaults() {
        assert_eq!(parse(&[]), Ok(Cli::Run(Config::default())));
        assert_eq!(Config::default().addr(), "0.0.0.0:6379");
    }

    #[test]
    fn flags() {
        let want = Config {
            port: 7000,
            bind: "0.0.0.0".into(),
        };
        assert_eq!(
            parse(&["--port", "7000", "-b", "0.0.0.0"]),
            Ok(Cli::Run(want.clone()))
        );
        assert_eq!(
            parse(&["--port=7000", "--bind=0.0.0.0"]),
            Ok(Cli::Run(want))
        );
        assert_eq!(parse(&["-h"]), Ok(Cli::Help));
    }

    #[test]
    fn errors() {
        assert!(parse(&["--port", "notaport"]).is_err());
        assert!(parse(&["--port"]).is_err());
        assert!(parse(&["--nope"]).is_err());
    }
}
