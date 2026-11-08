use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use unituner::api::{self, AppState};
use unituner::config::Config;
use unituner::scan;
use unituner::services;
use unituner::webui;

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
            // Print unconditionally so the failure is visible even when the log
            // filter is set to `off`.
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

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("unituner: failed to start the async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(serve(Arc::new(config), args.config_dir)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("unituner: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Binds the configured address and serves the mirakurun-compatible API plus
/// the management UI (SPEC §9, §10) until SIGINT/SIGTERM.
async fn serve(config: Arc<Config>, config_dir: PathBuf) -> Result<(), String> {
    // `/api/services` and service streams are usable immediately: the last
    // scan's table is restored from `services.json` (SPEC §8.2) before the
    // listener exists. A missing/corrupt cache only warns and starts empty;
    // `startup_scan` below then decides from the fingerprint whether to tune.
    let service_table = services::load_service_cache(&config_dir);
    let state = AppState::new(Arc::clone(&config), service_table, config_dir.clone());
    let manager = state.manager.clone();

    // Install the signal handlers *before* the listener is announced: a
    // SIGTERM arriving right after `listening` must not fall back to the
    // default action and skip the session cleanup below.
    let mut signals = Signals::install()?;

    let addr = api::bind_address(&config.server.addr)?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|error| format!("failed to bind {addr}: {error}"))?;
    tracing::info!(%addr, "listening");

    // SPEC §8: the startup service scan runs in the background so the API is
    // reachable while it tunes. It is spawned *after* the listener is bound and
    // before the server starts accepting, and is stopped through the
    // process-wide shutdown watch on shutdown — the same watch an API-triggered
    // scan observes, so neither is held for `timeout × channels`.
    let shutdown = state.shutdown_watch();
    let mut scan_task = tokio::spawn(scan::startup_scan(
        manager.clone(),
        Arc::clone(&config),
        config_dir.clone(),
        shutdown,
    ));

    let app = webui::app(state.clone());
    let result = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        signals.wait().await;
        // Stop the scans first: the tuner cleanup below must not race a scan
        // that is still acquiring, and the scan task must not outlive it.
        state.signal_shutdown();
        manager.shutdown().await;
        tracing::info!("all tuner sessions stopped");
    })
    .await;

    // The scan task exits on the watch, but a scan interrupted mid-tune can
    // take one readiness poll: bound the wait instead of leaking the task. A
    // server error means the graceful-shutdown path never ran, so stop the
    // scan immediately instead of waiting.
    if result.is_err() {
        scan_task.abort();
    } else {
        match tokio::time::timeout(Duration::from_secs(5), &mut scan_task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "startup scan task failed"),
            Err(_) => {
                tracing::warn!("startup scan did not stop within 5s");
                scan_task.abort();
            }
        }
    }

    result.map_err(|error| format!("server error: {error}"))
}

/// SIGINT/SIGTERM handlers, installed synchronously so no signal can be missed
/// between "listening" and the first poll of the serve future.
struct Signals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl Signals {
    fn install() -> Result<Self, String> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let interrupt = signal(SignalKind::interrupt())
                .map_err(|error| format!("failed to install the SIGINT handler: {error}"))?;
            let terminate = signal(SignalKind::terminate())
                .map_err(|error| format!("failed to install the SIGTERM handler: {error}"))?;
            Ok(Self {
                interrupt,
                terminate,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn wait(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => tracing::info!("SIGINT received; shutting down"),
                _ = self.terminate.recv() => tracing::info!("SIGTERM received; shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("SIGINT received; shutting down");
            } else {
                std::future::pending::<()>().await;
            }
        }
    }
}
