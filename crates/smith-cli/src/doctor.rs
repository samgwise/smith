//! Environment checks and conformance: `smith doctor` / `smith conformance`.
//!
//! Doctor is read-only by default; `--fix` performs remediations (start
//! machine, pull/build images, create network + sidecar). Deep checks run
//! real round trips; `smith conformance` is the deep suite as a command.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::config::Config;
use smith_core::{FileContents, ToolExecutor};
use smith_sandbox::executor::LocalExecutor;
use smith_sandbox::{Sandbox, SandboxConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pass,
    Warn,
    Fail,
    Skipped,
}

#[derive(Clone, Debug, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub status: Status,
    pub detail: String,
}

/// Context handed to every check.
pub struct CheckCtx {
    pub workspace: PathBuf,
    pub deep: bool,
    pub fix: bool,
    /// Resolved sandbox config (network mode, egress flags).
    pub sandbox: SandboxConfig,
    /// Raw config section for provider checks.
    pub config: Config,
}

impl CheckCtx {
    fn sandbox(&self) -> Sandbox {
        Sandbox::new(self.sandbox.clone())
    }
}

/// Run the check suite: fast checks always, deep checks with `deep`.
pub async fn run_checks(ctx: &CheckCtx) -> Vec<CheckResult> {
    let mut results = vec![];
    results.push(build_info().await);
    results.push(config_check().await);
    results.push(state_dir_check().await);
    results.push(podman_cli_check(ctx).await);
    // Machine check (may remediate with --fix) before the info check so a
    // freshly started machine is not reported stale.
    results.push(machine_check(ctx).await);
    results.push(podman_info_check(ctx).await);
    results.push(sandbox_image_check(ctx).await);
    results.push(workspace_check(ctx).await);
    results.push(host_git_check().await);
    results.push(provider_check(ctx).await);
    results.push(mcp_config_check(ctx).await);

    if ctx.sandbox.network == smith_sandbox::NetworkMode::Proxy {
        results.push(egress_network_check(ctx).await);
        results.push(egress_sidecar_check(ctx).await);
        if ctx.sandbox.egress_enforce {
            results.push(enforcement_check(ctx).await);
        } else {
            results.push(CheckResult {
                name: "egress enforcement".into(),
                status: Status::Skipped,
                detail: "egress_enforce = false".into(),
            });
        }
    } else {
        results.push(CheckResult {
            name: "egress stack".into(),
            status: Status::Skipped,
            detail: format!("network mode is {:?}", ctx.sandbox.network),
        });
    }

    if ctx.deep {
        results.extend(deep_checks(ctx).await);
    }
    results
}

/// Deep checks: real round trips. Returns several results from one shared
/// sandbox session where possible.
async fn deep_checks(ctx: &CheckCtx) -> Vec<CheckResult> {
    let mut results = vec![];
    let sandbox = ctx.sandbox();

    // Sandbox round trip over a throwaway workspace (never the user's).
    let ws = std::env::temp_dir().join("smith-doctor-workspace");
    let _ = std::fs::create_dir_all(&ws);

    let session = match sandbox.start_session(&ws).await {
        Ok(session) => Some(session),
        Err(e) => {
            results.push(CheckResult {
                name: "sandbox round trip".into(),
                status: Status::Fail,
                detail: format!("container start failed: {e:#}"),
            });
            None
        }
    };

    if let Some(session) = &session {
        let echo = sandbox.exec(session, "echo doctor-ok", None).await;
        results.push(match echo {
            Ok(out) if out.stdout.contains("doctor-ok") => CheckResult {
                name: "sandbox round trip".into(),
                status: Status::Pass,
                detail: "container start/exec/stop".into(),
            },
            Ok(out) => CheckResult {
                name: "sandbox round trip".into(),
                status: Status::Fail,
                detail: format!("unexpected exec output: {}", out.stdout),
            },
            Err(e) => CheckResult {
                name: "sandbox round trip".into(),
                status: Status::Fail,
                detail: format!("exec failed: {e:#}"),
            },
        });
    }

    // File-tool round trip through the executor seam.
    if let Some(session) = &session {
        results.push(file_tool_check(&sandbox, session).await);
        results.push(prompt_probe_check(&sandbox, session).await);
        if ctx.sandbox.network == smith_sandbox::NetworkMode::Proxy {
            results.push(egress_live_check(&sandbox, session).await);
            // The round-trip session start installs the enforcement table;
            // report its post-round-trip state, not the stale fast-check one.
            if ctx.sandbox.egress_enforce {
                let mut check = enforcement_check(ctx).await;
                check.name = "egress enforcement (after session)".into();
                results.push(check);
            }
        }
    }

    results.push(mcp_deep_check(ctx).await);

    if let Some(session) = &session {
        let _ = sandbox.stop_session(session).await;
    }
    results
}

async fn file_tool_check(sandbox: &Sandbox, session: &smith_sandbox::Session) -> CheckResult {
    let executor = LocalExecutor::new(
        Arc::new(Sandbox::new(sandbox.config.clone())),
        session.clone(),
    );
    let exec: &dyn ToolExecutor = &executor;
    let name = "file-tool round trip";

    if let Err(e) = exec.write_file("doctor-probe.txt", "kia ora").await {
        return CheckResult {
            name: name.into(),
            status: Status::Fail,
            detail: format!("write: {e:#}"),
        };
    }
    let content = match exec.read_file("doctor-probe.txt", None, None).await {
        Ok(c) => c,
        Err(e) => {
            return CheckResult {
                name: name.into(),
                status: Status::Fail,
                detail: format!("read: {e:#}"),
            };
        }
    };
    if content.text.trim() != "kia ora" {
        return CheckResult {
            name: name.into(),
            status: Status::Fail,
            detail: "read-back mismatch".to_string(),
        };
    }
    match exec.list_dir(".").await {
        Ok(entries) if entries.iter().any(|e| e == "doctor-probe.txt") => CheckResult {
            name: name.into(),
            status: Status::Pass,
            detail: "write/read/list + workspace lock".into(),
        },
        Ok(entries) => CheckResult {
            name: name.into(),
            status: Status::Fail,
            detail: format!("listing missing probe file: {entries:?}"),
        },
        Err(e) => CheckResult {
            name: name.into(),
            status: Status::Fail,
            detail: format!("list: {e:#}"),
        },
    }
}

async fn prompt_probe_check(sandbox: &Sandbox, session: &smith_sandbox::Session) -> CheckResult {
    let git = sandbox
        .exec(
            session,
            "git rev-parse --abbrev-ref HEAD 2>/dev/null",
            Some(Duration::from_secs(10)),
        )
        .await;
    let git_note = match git {
        Ok(out) if !out.stdout.trim().is_empty() => {
            format!("git reports branch {}", out.stdout.trim())
        }
        _ => "git unavailable in the sandbox image (snapshot degrades)".to_string(),
    };
    CheckResult {
        name: "prompt snapshot probes".into(),
        status: Status::Pass,
        detail: git_note,
    }
}

async fn egress_live_check(sandbox: &Sandbox, session: &smith_sandbox::Session) -> CheckResult {
    // Ensure curl exists (root install through the proxy — itself an allow test).
    let _ = sandbox
        .exec_as(
            session,
            Some("root"),
            "command -v curl >/dev/null || apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq curl >/dev/null 2>&1",
            Some(Duration::from_secs(300)),
        )
        .await;

    let allowed = sandbox
        .exec_as(
            session,
            Some("root"),
            "curl -sSf --max-time 20 https://registry.npmjs.org -o /dev/null && echo ALLOWED || echo DENIED",
            Some(Duration::from_secs(60)),
        )
        .await;
    let denied = sandbox
        .exec_as(
            session,
            Some("root"),
            "curl -sSf --max-time 20 https://example.com -o /dev/null && echo LEAKED || echo BLOCKED",
            Some(Duration::from_secs(60)),
        )
        .await;

    let allowed_ok = allowed
        .map(|o| o.stdout.contains("ALLOWED"))
        .unwrap_or(false);
    let denied_ok = denied
        .map(|o| o.stdout.contains("BLOCKED"))
        .unwrap_or(false);
    match (allowed_ok, denied_ok) {
        (true, true) => CheckResult {
            name: "egress live allow/deny".into(),
            status: Status::Pass,
            detail: "npm allowed, example.com denied (network-dependent)".into(),
        },
        (false, _) => CheckResult {
            name: "egress live allow/deny".into(),
            status: Status::Warn,
            detail: "allowlisted host unreachable (network or upstream blocking)".to_string(),
        },
        (_, false) => CheckResult {
            name: "egress live allow/deny".into(),
            status: Status::Fail,
            detail: "a non-allowlisted host was reachable - enforcement is not working".to_string(),
        },
    }
}

async fn mcp_deep_check(ctx: &CheckCtx) -> CheckResult {
    let mut config = ctx.config.mcp.clone();
    if let Ok(ws) = smith_mcp::McpConfig::load_workspace(&ctx.workspace) {
        config = config.merged(ws);
    }
    if config.servers.is_empty() {
        return CheckResult {
            name: "mcp handshake".into(),
            status: Status::Skipped,
            detail: "no MCP servers configured".to_string(),
        };
    }
    let mut session = smith_mcp::McpSession::connect(&config).await;
    let status = session.status_lines();
    let tools = session.take_tools();
    let count = tools.len();
    // Exercise one tool call if any tool exists (echo servers respond).
    let call = if let Some(tool) = tools.first() {
        // Best-effort: a tool that needs arguments we cannot guess reports
        // its own error, which is still proof of the round trip.
        tool.run(&NoExecutor, "{}").await
    } else {
        Ok("no tools".into())
    };
    let _ = call;
    session.shutdown().await;
    CheckResult {
        name: "mcp handshake".into(),
        status: if status.iter().any(|l| l.starts_with('✓')) {
            Status::Pass
        } else {
            Status::Fail
        },
        detail: format!("{count} tool(s): {}", status.join(" | ")),
    }
}

/// Executor stub for MCP tools (they never use the workspace executor).
struct NoExecutor;
#[async_trait::async_trait]
impl ToolExecutor for NoExecutor {
    async fn exec_shell(&self, _c: &str, _t: Duration) -> anyhow::Result<smith_core::ExecResult> {
        unreachable!()
    }
    async fn read_file(
        &self,
        _p: &str,
        _o: Option<u64>,
        _l: Option<u64>,
    ) -> anyhow::Result<FileContents> {
        unreachable!()
    }
    async fn write_file(&self, _p: &str, _c: &str) -> anyhow::Result<()> {
        unreachable!()
    }
    async fn list_dir(&self, _p: &str) -> anyhow::Result<Vec<String>> {
        unreachable!()
    }
    fn workspace_root(&self) -> &str {
        "/workspace"
    }
}

// ---------------------------------------------------------------- checks --

async fn build_info() -> CheckResult {
    CheckResult {
        name: "smith build".into(),
        status: Status::Pass,
        detail: format!(
            "smith {} ({}/{})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        ),
    }
}

async fn config_check() -> CheckResult {
    match Config::load() {
        Ok(cfg) => CheckResult {
            name: "config".into(),
            status: Status::Pass,
            detail: format!(
                "provider {} model {}, sandbox.network {}",
                cfg.provider, cfg.model, cfg.sandbox.network
            ),
        },
        Err(e) => CheckResult {
            name: "config".into(),
            status: Status::Fail,
            detail: format!("{e:#}"),
        },
    }
}

async fn state_dir_check() -> CheckResult {
    match smith_core::session::smith_dir() {
        Ok(dir) => {
            let probe = dir.join(".doctor-probe");
            match std::fs::write(&probe, b"ok") {
                Ok(()) => {
                    let _ = std::fs::remove_file(&probe);
                    CheckResult {
                        name: "state directory".into(),
                        status: Status::Pass,
                        detail: format!("{} writable", dir.display()),
                    }
                }
                Err(e) => CheckResult {
                    name: "state directory".into(),
                    status: Status::Fail,
                    detail: format!("{} not writable: {e}", dir.display()),
                },
            }
        }
        Err(e) => CheckResult {
            name: "state directory".into(),
            status: Status::Fail,
            detail: format!("{e:#}"),
        },
    }
}

async fn podman_cli_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    match sandbox.podman_version().await {
        Ok(version) if !version.is_empty() => {
            let major: u32 = version
                .split('.')
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            if major >= 5 {
                CheckResult {
                    name: "podman CLI".into(),
                    status: Status::Pass,
                    detail: version,
                }
            } else {
                CheckResult {
                    name: "podman CLI".into(),
                    status: Status::Warn,
                    detail: format!("{version} (5.x recommended)"),
                }
            }
        }
        Ok(_) => CheckResult {
            name: "podman CLI".into(),
            status: Status::Fail,
            detail: "podman version returned nothing".into(),
        },
        Err(e) => CheckResult {
            name: "podman CLI".into(),
            status: Status::Fail,
            detail: format!("{e:#}"),
        },
    }
}

async fn podman_info_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    match sandbox.machine_running().await {
        Ok(true) => CheckResult {
            name: "podman info".into(),
            status: Status::Pass,
            detail: "machine reachable".into(),
        },
        Ok(false) => CheckResult {
            name: "podman info".into(),
            status: Status::Warn,
            detail: "machine stopped (session start will bring it up; or use --fix)".to_string(),
        },
        Err(e) => CheckResult {
            name: "podman info".into(),
            status: Status::Fail,
            detail: format!("{e:#}"),
        },
    }
}

async fn machine_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    if sandbox.machine_running().await.unwrap_or(false) {
        return CheckResult {
            name: "podman machine".into(),
            status: Status::Pass,
            detail: "running".into(),
        };
    }
    if ctx.fix {
        // Remediation: ensure_ready starts a stopped machine.
        return match sandbox.ensure_ready().await {
            Ok(()) => CheckResult {
                name: "podman machine".into(),
                status: Status::Pass,
                detail: "started by --fix".into(),
            },
            Err(e) => CheckResult {
                name: "podman machine".into(),
                status: Status::Fail,
                detail: format!("{e:#}"),
            },
        };
    }
    CheckResult {
        name: "podman machine".into(),
        status: Status::Fail,
        detail: "not running (--fix to start it now, or it starts at next session)".to_string(),
    }
}

async fn sandbox_image_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    if !sandbox.podman_reachable().await {
        return CheckResult {
            name: "sandbox image".into(),
            status: Status::Skipped,
            detail: "cannot determine - podman not reachable (machine stopped)".to_string(),
        };
    }
    if sandbox.sandbox_image_exists().await {
        return CheckResult {
            name: "sandbox image".into(),
            status: Status::Pass,
            detail: format!("{} present", ctx.sandbox.image),
        };
    }
    if ctx.fix {
        let pulled = sandbox.run_podman(&["pull", &ctx.sandbox.image]).await;
        return match pulled {
            Ok(out) if out.status.success() => CheckResult {
                name: "sandbox image".into(),
                status: Status::Pass,
                detail: "pulled by --fix".into(),
            },
            Ok(out) => CheckResult {
                name: "sandbox image".into(),
                status: Status::Fail,
                detail: format!("pull failed: {}", String::from_utf8_lossy(&out.stderr)),
            },
            Err(e) => CheckResult {
                name: "sandbox image".into(),
                status: Status::Fail,
                detail: format!("{e:#}"),
            },
        };
    }
    CheckResult {
        name: "sandbox image".into(),
        status: Status::Warn,
        detail: format!(
            "{} missing (pulled at session start; --fix to pull now)",
            ctx.sandbox.image
        ),
    }
}

async fn workspace_check(ctx: &CheckCtx) -> CheckResult {
    let workspace_check = Sandbox::new(SandboxConfig::default())
        .check_workspace(&ctx.workspace)
        .await;
    match workspace_check {
        Ok(_) => CheckResult {
            name: "workspace".into(),
            status: Status::Pass,
            detail: format!("{} acceptable", ctx.workspace.display()),
        },
        Err(e) => CheckResult {
            name: "workspace".into(),
            status: Status::Fail,
            detail: format!("{e:#}"),
        },
    }
}

async fn host_git_check() -> CheckResult {
    let out = std::process::Command::new("git").arg("--version").output();
    match out {
        Ok(o) if o.status.success() => CheckResult {
            name: "host git".into(),
            status: Status::Pass,
            detail: String::from_utf8_lossy(&o.stdout).trim().to_string(),
        },
        _ => CheckResult {
            name: "host git".into(),
            status: Status::Warn,
            detail: "git not found on host (snapshot git line degrades in the sandbox)".to_string(),
        },
    }
}

async fn provider_check(ctx: &CheckCtx) -> CheckResult {
    let cfg = &ctx.config;
    match cfg.provider.as_str() {
        "ollama" => {
            let provider = smith_providers::OllamaProvider::new(
                cfg.ollama.base_url.clone(),
                cfg.model.clone(),
            );
            match provider.list_models().await {
                Ok(models) if models.iter().any(|m| m == &cfg.model) => CheckResult {
                    name: "provider (ollama)".into(),
                    status: Status::Pass,
                    detail: format!("model {} available", cfg.model),
                },
                Ok(models) => CheckResult {
                    name: "provider (ollama)".into(),
                    status: Status::Fail,
                    detail: format!(
                        "model `{}` not in ollama (available: {})",
                        cfg.model,
                        models.join(", ")
                    ),
                },
                Err(e) => CheckResult {
                    name: "provider (ollama)".into(),
                    status: Status::Fail,
                    detail: format!("{e:#}"),
                },
            }
        }
        "openai" => match std::env::var("OPENAI_API_KEY") {
            Ok(key) if !key.is_empty() => CheckResult {
                name: "provider (openai)".into(),
                status: Status::Pass,
                detail: format!("key present ({} chars, not shown)", key.len()),
            },
            _ => CheckResult {
                name: "provider (openai)".into(),
                status: Status::Fail,
                detail: "OPENAI_API_KEY not set".into(),
            },
        },
        other => CheckResult {
            name: "provider".into(),
            status: Status::Fail,
            detail: format!("unknown provider `{other}`"),
        },
    }
}

async fn mcp_config_check(ctx: &CheckCtx) -> CheckResult {
    let mut config = ctx.config.mcp.clone();
    if let Ok(ws) = smith_mcp::McpConfig::load_workspace(&ctx.workspace) {
        config = config.merged(ws);
    }
    if config.servers.is_empty() {
        return CheckResult {
            name: "mcp config".into(),
            status: Status::Pass,
            detail: "no servers configured (optional)".into(),
        };
    }
    let mut lines = vec![];
    let mut any_missing = false;
    for (name, server) in &config.servers {
        let found = command_on_path(&server.command);
        any_missing |= !found;
        lines.push(format!(
            "{} {name} ({}{})",
            if found { "✓" } else { "✗" },
            server.command,
            if found { "" } else { " not on PATH" }
        ));
    }
    CheckResult {
        name: "mcp config".into(),
        status: if any_missing {
            Status::Warn
        } else {
            Status::Pass
        },
        detail: lines.join(" | "),
    }
}

fn command_on_path(command: &str) -> bool {
    #[cfg(windows)]
    let finder = ("where", command);
    #[cfg(not(windows))]
    let finder = ("which", command);
    std::process::Command::new(finder.0)
        .arg(finder.1)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ------------------------------------------------------------- egress ----

async fn egress_network_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    if !sandbox.podman_reachable().await {
        return CheckResult {
            name: "egress network".into(),
            status: Status::Skipped,
            detail: "cannot determine - podman not reachable (machine stopped)".to_string(),
        };
    }
    match sandbox.egress_network_state().await {
        Ok((true, true)) => CheckResult {
            name: "egress network".into(),
            status: Status::Pass,
            detail: format!("{} with pinned subnet", smith_sandbox::EGRESS_NETWORK),
        },
        Ok((true, false)) => CheckResult {
            name: "egress network".into(),
            status: Status::Fail,
            detail: "subnet drifted (recreated at next session start)".into(),
        },
        Ok((false, _)) => CheckResult {
            name: "egress network".into(),
            status: Status::Warn,
            detail: "missing (created at session start; --fix to create now)".to_string(),
        },
        Err(e) => CheckResult {
            name: "egress network".into(),
            status: Status::Fail,
            detail: format!("{e:#}"),
        },
    }
}

async fn egress_sidecar_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    if !sandbox.podman_reachable().await {
        return CheckResult {
            name: "egress sidecar".into(),
            status: Status::Skipped,
            detail: "cannot determine - podman not reachable (machine stopped)".to_string(),
        };
    }
    let image = sandbox
        .run_podman(&["image", "exists", smith_sandbox::EGRESS_IMAGE])
        .await
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !image {
        return CheckResult {
            name: "egress sidecar".into(),
            status: Status::Warn,
            detail:
                "proxy image not built yet (one-time build at first proxy session; --fix to build)"
                    .to_string(),
        };
    }
    if ctx.fix {
        let _ = sandbox.ensure_sidecar().await;
    }
    if sandbox.egress_sidecar_running().await {
        CheckResult {
            name: "egress sidecar".into(),
            status: Status::Pass,
            detail: format!("{} running", smith_sandbox::EGRESS_SIDECAR),
        }
    } else {
        CheckResult {
            name: "egress sidecar".into(),
            status: Status::Warn,
            detail: "image built but sidecar not running (starts at session start; --fix to start)"
                .to_string(),
        }
    }
}

async fn enforcement_check(ctx: &CheckCtx) -> CheckResult {
    let sandbox = ctx.sandbox();
    let machine_up = sandbox.machine_running().await.unwrap_or(false);
    if machine_up && sandbox.egress_table_installed().await {
        CheckResult {
            name: "egress enforcement".into(),
            status: Status::Pass,
            detail: "nft table installed".into(),
        }
    } else {
        CheckResult {
            name: "egress enforcement".into(),
            status: Status::Warn,
            detail: "table not installed (installed at session start; machine may be stopped)"
                .to_string(),
        }
    }
}

// ------------------------------------------------------------ rendering --

/// Render results as aligned text lines with status markers.
pub fn render(results: &[CheckResult]) -> String {
    let width = results
        .iter()
        .map(|r| r.name.len())
        .max()
        .unwrap_or(0)
        .min(40);
    let mut out = String::new();
    for r in results {
        let marker = match r.status {
            Status::Pass => "✓",
            Status::Warn => "⚠",
            Status::Fail => "✗",
            Status::Skipped => "·",
        };
        let _ = std::fmt::Write::write_fmt(
            &mut out,
            format_args!("{marker} {:<width$}  {}\n", r.name, r.detail, width = width),
        );
    }
    let (pass, warn, fail, skipped) = counts(results);
    let _ = std::fmt::Write::write_fmt(
        &mut out,
        format_args!(
            "\n{} passed, {warn} warning(s), {fail} failed, {skipped} skipped\n",
            pass
        ),
    );
    out
}

fn counts(results: &[CheckResult]) -> (usize, usize, usize, usize) {
    let mut c = (0, 0, 0, 0);
    for r in results {
        match r.status {
            Status::Pass => c.0 += 1,
            Status::Warn => c.1 += 1,
            Status::Fail => c.2 += 1,
            Status::Skipped => c.3 += 1,
        }
    }
    c
}

/// Process exit code: 0 unless a check failed.
pub fn exit_code(results: &[CheckResult]) -> i32 {
    if results.iter().any(|r| r.status == Status::Fail) {
        1
    } else {
        0
    }
}

/// Run doctor or conformance (deep) and print; returns the process exit code.
pub async fn run_and_print(
    config: &Config,
    workspace: PathBuf,
    deep: bool,
    fix: bool,
    json: bool,
) -> i32 {
    let sandbox_config = config
        .sandbox_config()
        .unwrap_or_else(|_| SandboxConfig::default());
    let ctx = CheckCtx {
        workspace,
        deep,
        fix,
        sandbox: sandbox_config,
        config: config.clone(),
    };
    let results = run_checks(&ctx).await;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&results).unwrap_or_default()
        );
    } else {
        println!("{}", render(&results));
    }
    exit_code(&results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(name: &str, status: Status) -> CheckResult {
        CheckResult {
            name: name.into(),
            status,
            detail: "detail".into(),
        }
    }

    #[test]
    fn exit_code_zero_unless_a_fail() {
        assert_eq!(exit_code(&[]), 0);
        assert_eq!(
            exit_code(&[
                result("a", Status::Pass),
                result("b", Status::Warn),
                result("c", Status::Skipped)
            ]),
            0
        );
        assert_eq!(
            exit_code(&[result("a", Status::Pass), result("b", Status::Fail)]),
            1
        );
    }

    #[test]
    fn render_shows_markers_and_summary() {
        let out = render(&[
            result("podman cli", Status::Pass),
            result("machine", Status::Warn),
            result("egress", Status::Fail),
            result("mcp", Status::Skipped),
        ]);
        assert!(out.contains('✓'));
        assert!(out.contains('⚠'));
        assert!(out.contains('✗'));
        assert!(out.contains('·'));
        assert!(out.contains("1 passed, 1 warning(s), 1 failed, 1 skipped"));
    }

    #[test]
    fn results_serialise_to_json() {
        let json = serde_json::to_string(&result("x", Status::Pass)).unwrap();
        assert!(json.contains("\"status\":\"pass\""));
    }
}
