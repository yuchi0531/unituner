use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use unituner::config::Config;

/// Tuner sharing and abstraction server (mirakurun-compatible subset).
#[derive(Debug, Parser)]
#[command(name = "unituner", version, about)]
struct Args {
    /// Directory containing server.yml, tuners.yml, channels.yml and pipelines.yml.
    #[arg(long, value_name = "DIR", default_value = "/etc/unituner")]
    config_dir: PathBuf,
}

fn init_logging() {
    use tracing_subscriber::EnvFilter;

    let filter = std::env::var("UNITUNER_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".to_string());
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

fn main() -> ExitCode {
    init_logging();
    let args = Args::parse();

    tracing::info!(config_dir = %args.config_dir.display(), "loading configuration");

    let config = match Config::load(&args.config_dir) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("{error}");
            eprintln!("unituner: {error}");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        tuners = config.tuners.len(),
        channels = config.channels.len(),
        pipelines = config.pipelines.pipelines.len(),
        bindings = config.pipelines.types.len(),
        addr = %config.server.addr,
        allowed_cidrs = ?config.server.allowed_cidrs,
        idle_timeout_secs = config.server.idle_timeout_secs,
        subscriber_buffer_chunks = config.server.subscriber_buffer_chunks,
        chunk_size_bytes = config.server.chunk_size_bytes,
        "configuration loaded"
    );
    println!("phase 1: config loaded");

    ExitCode::SUCCESS
}
