//! Smith server: the harness plane. The `smith serve` daemon hosts agent
//! loops, session logs and approval routing; thin clients (the TUI, CI
//! issuers) attach over the smith-protocol HTTP API.
//!
//! Security posture: loopback bind by default, bearer token in
//! `~/.smith/serve-token`, every request audited via tracing. Model keys
//! live only in the daemon's environment.

pub mod api;
pub mod config;
pub mod controller;
pub mod factory;
pub mod forge;
pub mod gate;
pub mod journal;
pub mod orchestrator;
pub mod workers;

pub use config::{Config, OrchestrateSection, ServeSection, WorkerSection};
pub use controller::{ApiError, Controller, WorkerDefaults};
pub use factory::{
    ConfigFactory, SessionFactory, SessionParts, build_provider, connect_mcp, environment_notes,
    set_clearance_mode, start_executor,
};
pub use gate::{GateSlot, WireGate};
pub use journal::Journal;
pub use workers::{Fleet, WorkerExecutor, WorkerRegistry, workers_registry_path};

use std::path::Path;

use anyhow::Context;
use smith_protocol::client::token_file;

/// Generate (or reuse) the daemon's bearer token, written to
/// `~/.smith/serve-token`.
pub fn ensure_token() -> anyhow::Result<String> {
    let path = token_file()?;
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if !existing.is_empty() {
            return Ok(existing.to_string());
        }
    }
    let token = uuid::Uuid::new_v4().simple().to_string();
    std::fs::write(&path, &token).with_context(|| format!("writing {}", path.display()))?;
    Ok(token)
}

/// Run the daemon: bind, install the clearance poller, serve until
/// interrupted. Session containers are left running on shutdown (same
/// contract as a TUI exit; `smith clean` reaps them).
pub async fn serve(config: &Config, bind_override: Option<String>) -> anyhow::Result<()> {
    let bind = bind_override.unwrap_or_else(|| config.serve.bind.clone());
    // Validate the TLS pair eagerly (fail fast on bad files) — the api
    // serves it.
    let tls = match (&config.serve.tls_cert, &config.serve.tls_key) {
        (Some(cert), Some(key)) => {
            let acceptor = api::load_tls_acceptor(Path::new(cert), Path::new(key))?;
            tracing::info!("TLS configured on the serve api");
            Some(acceptor)
        }
        (None, None) => None,
        _ => anyhow::bail!("serve.tls_cert and serve.tls_key must be set together"),
    };
    // A non-loopback bind needs TLS actually being served (it is), or an
    // explicit allow_plaintext_remote (S-05).
    api::ensure_bind_policy(&bind, tls.is_some(), config.serve.allow_plaintext_remote)?;
    let token = ensure_token()?;
    // A corrupt worker registry aborts startup loudly (S-19).
    let controller = Controller::new(config)?;
    // M16: rebuild the session table from the state dir before serving —
    // recovered sessions re-attach transparently (workers never notice).
    let recovered = controller.recover().await;
    if recovered > 0 {
        println!("recovered {recovered} hosted session(s)");
    }
    controller::spawn_clearance_poller(controller.clone());
    controller::spawn_fleet_sweeper(controller.clone());

    let listener = api::bind(&bind).await?;
    tracing::info!("smith serve listening on {bind}");
    println!(
        "smith serve: listening on {bind} (token: {})",
        token_file()?.display()
    );
    // Ctrl+C stops the accept loop; in-flight connections finish under
    // their own timeouts.
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("interrupted — shutting down the serve api");
            shutdown.cancel();
        }
    });
    api::accept_loop(
        listener,
        controller,
        token,
        api::ApiOptions {
            tls,
            max_git_body_bytes: (config.serve.max_git_body_mb.max(1) as usize) * 1024 * 1024,
            shutdown,
            ..Default::default()
        },
    )
    .await
}
