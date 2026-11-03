//! `micro-redis` server binary: parse flags, set up logging, serve.

use micro_redis::config::{self, Cli};
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match config::parse_args(std::env::args().skip(1)) {
        Ok(Cli::Run(config)) => config,
        Ok(Cli::Help) => {
            println!("{}", config::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("error: {e}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = config.addr();
    tracing::info!(%addr, "starting");
    match micro_redis::run(&addr).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: could not serve on {addr}: {e}");
            ExitCode::FAILURE
        }
    }
}
