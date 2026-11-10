//! Smith server: the harness plane. The `smith serve` daemon hosts agent
//! loops, session logs and approval routing; thin clients (the TUI, CI
//! issuers) attach over the smith-protocol HTTP API.
//!
//! Security posture: loopback bind by default, bearer token in
//! `~/.smith/serve-token`, every request audited via tracing. Model keys
//! live only in the daemon's environment.

pub mod config;
pub mod controller;
pub mod factory;
pub mod gate;
pub mod http;
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

/// Run the daemon: bind, install the clearance poller, serve forever.
/// Session containers are left running on shutdown (same contract as a TUI
/// exit; `smith clean` reaps them).
pub async fn serve(config: &Config, bind_override: Option<String>) -> anyhow::Result<()> {
    let bind = bind_override.unwrap_or_else(|| config.serve.bind.clone());
    let token = ensure_token()?;
    let controller = Controller::new(config);
    controller::spawn_clearance_poller(controller.clone());
    controller::spawn_fleet_sweeper(controller.clone());

    let listener = http::bind(&bind).await?;
    tracing::info!("smith serve listening on {bind}");
    println!(
        "smith serve: listening on {bind} (token: {})",
        token_file()?.display()
    );
    http::accept_loop(listener, controller, token).await
}
