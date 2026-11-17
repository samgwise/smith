//! Per-workspace MCP trust (S-01): a workspace's `.smith/mcp.toml` can run
//! host commands at session start, so the workspace's MCP layer is only
//! loaded when the user trusts the workspace.
//!
//! The trust store lives at `~/.smith/trust.json`, keyed by the workspace's
//! logical identity ([`crate::session::workspace_identity`]). The TUI asks
//! once and remembers; headless runs opt in with `--trust-workspace` (also
//! remembered); `smith serve` consults the store only — it never prompts.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

/// The path of the trust store: `~/.smith/trust.json`. The
/// `SMITH_TRUST_STORE` override keeps tests out of the real home directory
/// (the same pattern as the worker's work dir).
fn store_path() -> anyhow::Result<PathBuf> {
    if let Ok(dir) = std::env::var("SMITH_TRUST_STORE")
        && !dir.trim().is_empty()
    {
        return Ok(PathBuf::from(dir));
    }
    Ok(crate::session::smith_dir()?.join("trust.json"))
}

/// One trusted workspace's record. The path is informational (the identity
/// is the key) — a human reading `~/.smith/trust.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TrustRecord {
    path: String,
    trusted_ms: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct TrustStore {
    workspaces: HashMap<String, TrustRecord>,
}

fn load() -> TrustStore {
    std::fs::read_to_string(store_path().unwrap_or_else(|_| PathBuf::from("")))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn persist(store: &TrustStore) -> anyhow::Result<()> {
    let path = store_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(store)?)
        .with_context(|| format!("writing {}", path.display()))
}

/// Whether the workspace's MCP layer may load. `explicit` is the
/// `--trust-workspace` opt-in: it grants AND records the trust.
pub fn resolve(explicit: bool, workspace_canon: &Path) -> bool {
    if explicit {
        let _ = remember(workspace_canon);
        return true;
    }
    is_trusted(workspace_canon)
}

/// Whether the store already records trust for the workspace.
pub fn is_trusted(workspace_canon: &Path) -> bool {
    load()
        .workspaces
        .contains_key(&crate::session::workspace_identity(workspace_canon))
}

/// Record trust for the workspace (`smith trust <workspace>`).
pub fn remember(workspace_canon: &Path) -> anyhow::Result<()> {
    let mut store = load();
    store.workspaces.insert(
        crate::session::workspace_identity(workspace_canon),
        TrustRecord {
            path: workspace_canon.display().to_string(),
            trusted_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        },
    );
    persist(&store)
}

/// Remove the workspace's recorded trust (`smith trust --revoke`).
pub fn revoke(workspace_canon: &Path) -> anyhow::Result<()> {
    let mut store = load();
    store
        .workspaces
        .remove(&crate::session::workspace_identity(workspace_canon));
    persist(&store)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SMITH_TRUST_STORE` is process-global: the tests that redirect it
    /// must not run concurrently.
    static STORE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The trust store round-trips per workspace identity, and an
    /// untrusted workspace resolves false.
    #[test]
    fn trust_store_round_trip() {
        let _store_guard = STORE_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var(
                "SMITH_TRUST_STORE",
                tmp.path().join("trust.json").display().to_string(),
            );
        }
        let ws_a = tmp.path().join("a");
        let ws_b = tmp.path().join("b");
        std::fs::create_dir_all(&ws_a).unwrap();
        std::fs::create_dir_all(&ws_b).unwrap();

        assert!(!is_trusted(&ws_a), "a fresh workspace is untrusted");
        assert!(
            !resolve(false, &ws_a),
            "resolve without opt-in consults the store only"
        );
        assert!(resolve(true, &ws_a), "--trust-workspace grants");
        assert!(is_trusted(&ws_a), "the opt-in is remembered");
        assert!(!is_trusted(&ws_b), "other workspaces stay untrusted");

        remember(&ws_a).unwrap();
        assert!(is_trusted(&ws_a));

        // The store on disk is the documented shape (identity → record).
        let raw = std::fs::read_to_string(tmp.path().join("trust.json")).unwrap();
        assert!(raw.contains(&crate::session::workspace_identity(&ws_a)));
    }

    /// Revocation withdraws the recorded trust (`smith trust --revoke`).
    #[test]
    fn revoke_removes_trust() {
        let _store_guard = STORE_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var(
                "SMITH_TRUST_STORE",
                tmp.path().join("trust.json").display().to_string(),
            );
        }
        let ws = tmp.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();

        remember(&ws).unwrap();
        assert!(is_trusted(&ws));
        revoke(&ws).unwrap();
        assert!(!is_trusted(&ws), "revoked trust must not persist");
        // Revoking again is a no-op.
        revoke(&ws).unwrap();
        assert!(!is_trusted(&ws));
    }
}
