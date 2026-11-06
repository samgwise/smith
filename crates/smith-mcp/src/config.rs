//! MCP server configuration: user config `[mcp.servers.<name>]` plus
//! workspace-scoped `.smith/mcp.toml` (workspace wins on name collision).

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

/// How tools from a server are approved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalMode {
    /// Route every call through the approval gate (default).
    #[default]
    Ask,
    /// Pre-approve all tools from this server.
    Allow,
}

#[derive(Clone, Debug, Deserialize)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment for the server process. Values support `${VAR}` expansion
    /// from the host environment; the child gets only these (plus a minimal
    /// PATH passthrough) — never the full host environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub approval: ApprovalMode,
    /// Timeout applied to each tool call.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_timeout() -> u64 {
    60
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: BTreeMap<String, McpServerConfig>,
}

impl McpConfig {
    /// Merge workspace servers over user servers (workspace wins per name).
    pub fn merged(mut self, workspace: McpConfig) -> Self {
        for (name, server) in workspace.servers {
            self.servers.insert(name, server);
        }
        self
    }

    /// Load a workspace-level `.smith/mcp.toml`, if present.
    pub fn load_workspace(workspace: &Path) -> anyhow::Result<McpConfig> {
        let path = workspace.join(".smith").join("mcp.toml");
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let cfg: McpConfig =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        Ok(cfg)
    }
}

/// Expand `${VAR}` references from the host environment. A variable that is
/// unset leaves the entry out (with a warning) rather than passing an empty
/// placeholder that looks like a valid credential.
pub fn expand_env(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (key, value) in env {
        let mut expanded = String::new();
        let mut missing = false;
        let mut rest = value.as_str();
        while let Some(start) = rest.find("${") {
            expanded.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            match after.find('}') {
                Some(end) => {
                    let var = &after[..end];
                    match std::env::var(var) {
                        Ok(v) => expanded.push_str(&v),
                        Err(_) => {
                            tracing::warn!(
                                "env variable {var} referenced in MCP config is not set"
                            );
                            // Fail closed: an entry with any unset reference
                            // is dropped entirely, not passed half-filled.
                            missing = true;
                        }
                    }
                    rest = &after[end + 1..];
                }
                None => {
                    // Unterminated reference; keep it verbatim.
                    expanded.push_str("${");
                    rest = after;
                }
            }
        }
        expanded.push_str(rest);
        if !missing {
            out.insert(key.clone(), expanded);
        }
    }
    out
}

/// Sanitise a server or tool name into `[a-z0-9_]`.
pub fn sanitise(name: &str) -> String {
    let clean: String = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let trimmed = clean.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "_".to_string()
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_server_config() {
        let raw = r#"
[servers.github]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "${GITHUB_TOKEN}" }
approval = "allow"
timeout_secs = 90

[servers.files]
command = "some-server"
"#;
        let cfg: McpConfig = toml::from_str(raw).unwrap();
        assert_eq!(cfg.servers.len(), 2);
        let gh = &cfg.servers["github"];
        assert_eq!(gh.command, "npx");
        assert_eq!(gh.approval, ApprovalMode::Allow);
        assert_eq!(gh.timeout_secs, 90);
        assert_eq!(cfg.servers["files"].approval, ApprovalMode::Ask);
        assert_eq!(cfg.servers["files"].timeout_secs, 60);
    }

    #[test]
    fn workspace_layer_overrides_user_per_name() {
        let user: McpConfig = toml::from_str(
            r#"
[servers.a]
command = "user-a"
[servers.b]
command = "user-b"
"#,
        )
        .unwrap();
        let workspace: McpConfig = toml::from_str(
            r#"
[servers.b]
command = "workspace-b"
"#,
        )
        .unwrap();
        let merged = user.merged(workspace);
        assert_eq!(merged.servers["a"].command, "user-a");
        assert_eq!(merged.servers["b"].command, "workspace-b");
    }

    #[test]
    fn env_expansion_uses_host_environment() {
        // SAFETY: test-only process env mutation; other tests in this crate
        // do not read the environment.
        unsafe { std::env::set_var("SMITH_MCP_TEST_TOKEN", "abc123") };
        let mut env = BTreeMap::new();
        env.insert(
            "TOKEN".to_string(),
            "${SMITH_MCP_TEST_TOKEN}-suffix".to_string(),
        );
        env.insert(
            "MISSING".to_string(),
            "${SMITH_MCP_TEST_UNSET_VAR}".to_string(),
        );
        env.insert("PLAIN".to_string(), "literal".to_string());
        let expanded = expand_env(&env);
        assert_eq!(expanded["TOKEN"], "abc123-suffix");
        assert_eq!(expanded["PLAIN"], "literal");
        // Unset variables drop the entry entirely.
        assert!(!expanded.contains_key("MISSING"));
    }

    #[test]
    fn name_sanitisation() {
        assert_eq!(sanitise("GitHub Repo"), "github_repo");
        assert_eq!(sanitise("db.query-v2"), "db_query_v2");
        assert_eq!(sanitise("---"), "_");
    }
}
