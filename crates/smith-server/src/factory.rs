//! Session assembly: building the provider, sandbox executor, MCP tools and
//! logger for a workspace. The production path is config-driven (podman +
//! Ollama/OpenAI + MCP); tests swap in a fake [`SessionFactory`].

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;

use smith_core::{Agent, ApprovalGate, LlmProvider, SessionLogger, ToolExecutor};
use smith_sandbox::executor::LocalExecutor;

use crate::config::Config;

/// Build the provider for the configured backend.
pub fn build_provider(config: &Config) -> anyhow::Result<Arc<dyn LlmProvider>> {
    match config.provider.as_str() {
        "ollama" => Ok(Arc::new(smith_providers::OllamaProvider::new(
            config.ollama.base_url.clone(),
            config.model.clone(),
        ))),
        "openai" => {
            let key = std::env::var("OPENAI_API_KEY").context(
                "OPENAI_API_KEY environment variable is not set (required for the openai provider)",
            )?;
            Ok(Arc::new(smith_providers::OpenAiProvider::new(
                config.openai.base_url.clone(),
                key,
                config.model.clone(),
            )))
        }
        other => anyhow::bail!("unknown provider `{other}`; expected `ollama` or `openai`"),
    }
}

/// Start a sandbox session and executor for a workspace.
pub async fn start_executor(
    config: &Config,
    workspace: &Path,
) -> anyhow::Result<(Arc<smith_sandbox::Sandbox>, LocalExecutor)> {
    let sandbox = Arc::new(smith_sandbox::Sandbox::new(config.sandbox_config()?));
    sandbox.ensure_ready().await?;
    sandbox.reap_stopped_sessions().await?;
    let session = sandbox.start_session(workspace).await?;
    Ok((sandbox.clone(), LocalExecutor::new(sandbox, session)))
}

/// Load layered MCP config (user + workspace) and connect; never fatal.
pub async fn connect_mcp(config: &Config, workspace_canon: &Path) -> smith_mcp::McpSession {
    let mut mcp_config = config.mcp.clone();
    match smith_mcp::McpConfig::load_workspace(workspace_canon) {
        Ok(workspace_config) => mcp_config = mcp_config.merged(workspace_config),
        Err(e) => tracing::warn!("could not load workspace MCP config: {e:#}"),
    }
    smith_mcp::McpSession::connect(&mcp_config).await
}

/// One-line environment summary injected into the system prompt.
pub fn environment_notes(config: &Config) -> String {
    let egress = match config
        .sandbox_config()
        .map(|s| (s.network, s.egress_enforce))
    {
        Ok((smith_sandbox::NetworkMode::Proxy, true)) => {
            "egress: allowlist proxy, packet-enforced".to_string()
        }
        Ok((smith_sandbox::NetworkMode::Proxy, false)) => {
            "egress: allowlist proxy (cooperative only)".to_string()
        }
        Ok((smith_sandbox::NetworkMode::Disabled, _)) => "network: disabled".to_string(),
        Ok((smith_sandbox::NetworkMode::Open, _)) => "network: unrestricted".to_string(),
        Err(e) => return format!("network: unknown ({e:#})"),
    };
    format!("Network mode: {egress}")
}

/// Set the sidecar clearance mode for a session: `ask` degrades to `deny`
/// when nothing is attached to answer the modal; `allow` is honoured.
/// Best-effort: a missing sidecar/token just keeps the sidecar default.
pub fn set_clearance_mode(config: &Config, interactive: bool) {
    let effective = match config.sandbox.egress_clearance.as_str() {
        "allow" => "allow",
        "deny" => "deny",
        _ if interactive => "ask",
        _ => {
            tracing::info!("[clearance] ask degraded to deny for this non-interactive session");
            "deny"
        }
    };
    if let Some(token) = smith_egress::client::read_token()
        && let Err(e) = smith_egress::client::set_mode(
            smith_egress::client::CLEARANCE_CONTROL_PORT,
            &token,
            effective,
        )
    {
        tracing::debug!("clearance mode not set: {e:#}");
    }
}

/// The assembled pieces of a hosted session.
pub struct SessionParts {
    pub agent: Agent,
    pub executor: Arc<dyn ToolExecutor>,
    /// Sandbox handle keeping the container's owner alive (production).
    pub sandbox: Option<Arc<smith_sandbox::Sandbox>>,
    pub container_label: Option<String>,
    pub mcp_status: Vec<String>,
    pub provider_name: String,
    pub model_name: String,
    pub workspace_name: String,
}

/// Builds the pieces of a hosted session. The production factory goes
/// through podman + providers + MCP; tests inject fakes.
#[async_trait]
pub trait SessionFactory: Send + Sync {
    /// Assemble a session for the (daemon-side) workspace. The gate is
    /// provided by the controller so approvals route over the wire.
    async fn create(
        &self,
        workspace: &Path,
        provider: Option<&str>,
        model: Option<&str>,
        gate: Arc<dyn ApprovalGate>,
    ) -> anyhow::Result<SessionParts>;

    /// Build a fresh provider for mid-session `/model`/`/provider` switches.
    fn build_provider(
        &self,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> anyhow::Result<Arc<dyn LlmProvider>>;
}

/// The production factory: config-driven, exactly what `smith run` does.
pub struct ConfigFactory {
    pub config: Config,
}

impl ConfigFactory {
    /// Config with the requested provider/model overrides applied.
    fn config_for(&self, provider: Option<&str>, model: Option<&str>) -> Config {
        let mut cfg = self.config.clone();
        if let Some(p) = provider {
            cfg.provider = p.to_string();
        }
        if let Some(m) = model {
            cfg.model = m.to_string();
        }
        cfg
    }
}

#[async_trait]
impl SessionFactory for ConfigFactory {
    async fn create(
        &self,
        workspace: &Path,
        provider: Option<&str>,
        model: Option<&str>,
        gate: Arc<dyn ApprovalGate>,
    ) -> anyhow::Result<SessionParts> {
        let config = self.config_for(provider, model);
        let (sandbox, executor) = start_executor(&config, workspace).await?;
        set_clearance_mode(&config, true);
        let canon = executor.session().workspace_host.clone();
        let container_label = executor
            .session()
            .name
            .trim_start_matches("smith-")
            .chars()
            .take(12)
            .collect::<String>();

        let logger = SessionLogger::new(&canon, &config.provider, &config.model)?;
        let mut mcp_session = connect_mcp(&config, &canon).await;
        let mcp_status = mcp_session.status_lines();
        let mut tools = smith_core::builtin_tools();
        tools.extend(mcp_session.take_tools());
        // Stdio MCP servers exit when the daemon's process (and stdio) ends;
        // no explicit shutdown handshake is needed for a daemon session.

        let provider_impl = build_provider(&config)?;
        let executor: Arc<dyn ToolExecutor> = Arc::new(executor);
        let mut agent = Agent::new(
            provider_impl,
            executor.clone(),
            tools,
            gate,
            Some(Arc::new(logger)),
        );
        agent.project_instructions = smith_core::instructions::load_project_instructions(&canon);
        agent.environment_notes = Some(environment_notes(&config));

        Ok(SessionParts {
            agent,
            executor,
            sandbox: Some(sandbox),
            container_label: Some(container_label),
            mcp_status,
            provider_name: config.provider,
            model_name: config.model,
            workspace_name: canon
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "workspace".into()),
        })
    }

    fn build_provider(
        &self,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> anyhow::Result<Arc<dyn LlmProvider>> {
        build_provider(&self.config_for(provider, model))
    }
}
