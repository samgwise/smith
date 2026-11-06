//! Configuration: `~/.smith/config.toml` layered over built-in defaults.
//!
//! Provider API keys never live in the config file; they come from the
//! environment (`OPENAI_API_KEY`).

use std::path::Path;

use anyhow::{Context, bail};
use serde::Deserialize;
use smith_sandbox::{NetworkMode, SandboxConfig};

pub const DEFAULT_MODEL_OLLAMA: &str = "qwen3.8:27b";
pub const DEFAULT_MODEL_OPENAI: &str = "gpt-4o-mini";

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Active provider: `ollama` or `openai`.
    pub provider: String,
    /// Model for the active provider.
    pub model: String,
    pub ollama: OllamaConfig,
    pub openai: OpenAiConfig,
    pub sandbox: SandboxSection,
    pub verify: VerifyConfig,
    pub mcp: smith_mcp::McpConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct OllamaConfig {
    pub base_url: String,
}

/// Headless verification gate: run a command after the agent's turn and feed
/// failures back for fixing.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct VerifyConfig {
    /// Command run in the sandbox after headless turns (empty = disabled).
    pub command: String,
    /// Maximum fix-and-retry rounds when the command fails.
    pub max_rounds: usize,
}

impl Default for VerifyConfig {
    fn default() -> Self {
        Self {
            command: String::new(),
            max_rounds: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct OpenAiConfig {
    pub base_url: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct SandboxSection {
    /// `enabled` or `disabled`.
    pub network: String,
    pub image: String,
    pub memory: String,
    pub cpus: String,
    pub pids_limit: u32,
    pub exec_timeout_secs: u64,
    pub translate_paths: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            provider: "ollama".into(),
            model: DEFAULT_MODEL_OLLAMA.into(),
            ollama: OllamaConfig::default(),
            openai: OpenAiConfig::default(),
            sandbox: SandboxSection::default(),
            verify: VerifyConfig::default(),
            mcp: Default::default(),
        }
    }
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            base_url: smith_providers::ollama::DEFAULT_BASE_URL.into(),
        }
    }
}

impl Default for OpenAiConfig {
    fn default() -> Self {
        Self {
            base_url: smith_providers::openai::DEFAULT_BASE_URL.into(),
        }
    }
}

impl Default for SandboxSection {
    fn default() -> Self {
        let d = SandboxConfig::default();
        Self {
            network: "enabled".into(),
            image: d.image,
            memory: d.memory,
            cpus: d.cpus,
            pids_limit: d.pids_limit,
            exec_timeout_secs: d.exec_timeout.as_secs(),
            translate_paths: d.translate_paths,
        }
    }
}

impl Config {
    /// Load the config file if present, else defaults.
    pub fn load() -> anyhow::Result<Self> {
        let path = config_path()?;
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let mut cfg: Config =
                toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
            cfg.normalise();
            Ok(cfg)
        } else {
            Ok(Self::default())
        }
    }

    /// Pair provider-appropriate defaults (e.g. an OpenAI model when the
    /// provider is switched and the model was left at the ollama default).
    pub fn normalise(&mut self) {
        if self.provider == "openai" && self.model == DEFAULT_MODEL_OLLAMA {
            self.model = DEFAULT_MODEL_OPENAI.into();
        }
    }

    /// Resolve the sandbox configuration section.
    pub fn sandbox_config(&self) -> anyhow::Result<SandboxConfig> {
        let s = &self.sandbox;
        let network = match s.network.as_str() {
            "enabled" => NetworkMode::Enabled,
            "disabled" => NetworkMode::Disabled,
            other => bail!("sandbox.network must be `enabled` or `disabled`, got `{other}`"),
        };
        Ok(SandboxConfig {
            image: s.image.clone(),
            network,
            memory: s.memory.clone(),
            cpus: s.cpus.clone(),
            pids_limit: s.pids_limit,
            exec_timeout: std::time::Duration::from_secs(s.exec_timeout_secs.max(1)),
            translate_paths: s.translate_paths,
        })
    }
}

pub fn config_path() -> anyhow::Result<std::path::PathBuf> {
    Ok(smith_core::session::smith_dir()?.join("config.toml"))
}

/// Write a commented starter config the first time (does not overwrite).
pub fn ensure_example_config() -> anyhow::Result<()> {
    let path = config_path()?;
    if path.exists() {
        return Ok(());
    }
    let dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        &path,
        format!(
            "# smith configuration\n\
             # provider = \"ollama\" | \"openai\"\n\
             provider = \"ollama\"\n\
             model = \"{DEFAULT_MODEL_OLLAMA}\"\n\
             \n\
             [ollama]\n\
             base_url = \"http://localhost:11434\"\n\
             \n\
             [openai]\n\
             base_url = \"https://api.openai.com/v1\"\n\
             # key comes from the OPENAI_API_KEY environment variable\n\
             \n\
             [sandbox]\n\
             network = \"enabled\"   # \"disabled\" maps to --network=none\n\
             image = \"docker.io/library/ubuntu:24.04\"\n\
             memory = \"4g\"\n\
             cpus = \"2\"\n\
             pids_limit = 256\n\
             exec_timeout_secs = 120\n\
             translate_paths = false\n\
             \n\
             [verify]\n\
             # headless gate: command run after each task; failures are fed\
               back to the agent for fixing (max_rounds times)\n\
             command = \"\"           # e.g. \"cargo test -q\"\n\
             max_rounds = 2\n\
             \n\
             # MCP servers (stdio); workspace-level servers go in .smith/mcp.toml\n\
             # [mcp.servers.github]\n\
             # command = \"npx\"\n\
             # args = [\"-y\", \"@modelcontextprotocol/server-github\"]\n\
             # env = {{ GITHUB_TOKEN = \"${{GITHUB_TOKEN}}\" }}\n\
             # approval = \"ask\"    # or \"allow\"\n\
             # timeout_secs = 60\n"
        ),
    )?;
    Ok(())
}
