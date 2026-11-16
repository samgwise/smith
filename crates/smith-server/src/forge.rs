//! The forge: controller-hosted git repos over the smart HTTP protocol
//! (M17) — the token broker collapsed into a function.
//!
//! Git stays the protocol oracle: every request pipes to a
//! `git upload-pack|receive-pack --stateless-rpc` subprocess on the daemon
//! host — the controller only streams bytes and checks tokens. Auth is
//! per-session, per-repo, read/write-scoped; tokens are minted at dispatch
//! and persisted **hashed** (`.gitcred` sidecars, the `workers.json`
//! pattern) so recovered sessions keep pushing with tokens already
//! delivered to workers.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Context as _;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::process::Command;

fn token_hash(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// One token's grant: per-session, per-repo, scoped.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GitCred {
    pub session_id: String,
    pub token_hash: String,
    pub repo: String,
    pub read: bool,
    pub write: bool,
    /// Opt-in (auto-push): may update any ref in the repo, not just the
    /// session's own checkpoint and the integration branches. Recovered
    /// `.gitcred` files from before this field default to false.
    #[serde(default)]
    pub write_all: bool,
    /// Wall-clock expiry in ms since the epoch; 0 = never (recovered
    /// `.gitcred` files from before this field default to never). Expired
    /// grants stop authenticating (S-14 part 2).
    #[serde(default)]
    pub expires_ms: u64,
}

/// Default minted-token lifetime (S-14 part 2): one week.
pub const DEFAULT_TOKEN_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Hosted repos + the token table. Repos live as bare repos under
/// `<state_dir>/repos/<name>.git`; tokens under `<state_dir>/<session>.gitcred`.
pub struct Forge {
    repos_dir: PathBuf,
    state_dir: PathBuf,
    tokens: Mutex<HashMap<String /* token hash */, GitCred>>,
    /// Minted-token lifetime in ms; 0 disables expiry.
    token_ttl_ms: u64,
}

impl Forge {
    pub fn new(state_dir: &Path) -> Self {
        Self::with_ttl(state_dir, DEFAULT_TOKEN_TTL_MS)
    }

    /// A forge whose minted tokens expire after `token_ttl_ms` (0 = never).
    pub fn with_ttl(state_dir: &Path, token_ttl_ms: u64) -> Self {
        Self {
            repos_dir: state_dir.join("repos"),
            state_dir: state_dir.to_path_buf(),
            tokens: Mutex::new(HashMap::new()),
            token_ttl_ms,
        }
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn is_expired(cred: &GitCred) -> bool {
        cred.expires_ms != 0 && Self::now_ms() >= cred.expires_ms
    }

    pub fn repos_dir(&self) -> &Path {
        &self.repos_dir
    }

    /// Mint a per-session token for `repo`. Hashed at rest; the raw token
    /// is returned once (delivered to the worker in `WorkspaceSource`).
    /// `write_all` lifts the ref policy (auto-push updates base refs).
    pub fn mint(
        &self,
        session_id: &str,
        repo: &str,
        read: bool,
        write: bool,
        write_all: bool,
    ) -> anyhow::Result<String> {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let expires_ms = if self.token_ttl_ms == 0 {
            0
        } else {
            Self::now_ms() + self.token_ttl_ms
        };
        let cred = GitCred {
            session_id: session_id.to_string(),
            token_hash: token_hash(&token),
            repo: repo.to_string(),
            read,
            write,
            write_all,
            expires_ms,
        };
        let path = self.state_dir.join(format!("{session_id}.gitcred"));
        smith_core::secret::write_private(&path, serde_json::to_string_pretty(&cred)?.as_bytes())
            .with_context(|| format!("writing {}", path.display()))?;
        self.tokens
            .lock()
            .expect("forge tokens poisoned")
            .insert(cred.token_hash.clone(), cred);
        Ok(token)
    }

    /// Reload persisted token grants (daemon restart). Returns the count.
    pub fn recover(&self) -> usize {
        let mut n = 0;
        if let Ok(entries) = std::fs::read_dir(&self.state_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_none_or(|e| e != "gitcred") {
                    continue;
                }
                match std::fs::read_to_string(&path)
                    .map_err(|e| anyhow::anyhow!("{e}"))
                    .and_then(|raw| serde_json::from_str::<GitCred>(&raw).map_err(|e| e.into()))
                {
                    Ok(cred) => {
                        if Self::is_expired(&cred) {
                            tracing::info!(
                                "expired .gitcred {} not recovered (S-14 part 2)",
                                path.display()
                            );
                            let _ = std::fs::remove_file(&path);
                            continue;
                        }
                        self.tokens
                            .lock()
                            .expect("forge tokens poisoned")
                            .insert(cred.token_hash.clone(), cred);
                        n += 1;
                    }
                    Err(e) => tracing::warn!("unreadable .gitcred {}: {e}", path.display()),
                }
            }
        }
        n
    }

    /// Authenticate a token → its grant. Expired grants refuse (S-14 part 2).
    pub fn authenticate(&self, token: &str) -> Option<GitCred> {
        let tokens = self.tokens.lock().expect("forge tokens poisoned");
        let cred = tokens.get(&token_hash(token))?;
        if Self::is_expired(cred) {
            return None;
        }
        Some(cred.clone())
    }

    /// Revoke every grant for a session (lease release / session end): the
    /// in-memory grants drop and the `.gitcred` sidecar is removed so a
    /// restart cannot recover them (S-14 part 2). Returns the removed count.
    pub fn revoke_session(&self, session_id: &str) -> usize {
        let mut tokens = self.tokens.lock().expect("forge tokens poisoned");
        let dead: Vec<String> = tokens
            .iter()
            .filter(|(_, c)| c.session_id == session_id)
            .map(|(h, _)| h.clone())
            .collect();
        for h in &dead {
            tokens.remove(h);
        }
        drop(tokens);
        let sidecar = self.state_dir.join(format!("{session_id}.gitcred"));
        if sidecar.exists() {
            let _ = std::fs::remove_file(&sidecar);
        }
        dead.len()
    }

    /// The path of a hosted repo; `None` when the name is unsafe.
    pub fn repo_path(&self, name: &str) -> Option<PathBuf> {
        if !valid_repo_name(name) {
            return None;
        }
        Some(self.repos_dir.join(format!("{name}.git")))
    }

    /// Push-to-create: `git init --bare` when the repo doesn't exist yet.
    /// Either way the repo's ref policy is (re)installed: the update hook
    /// and the receive policies (S-14).
    pub async fn ensure_repo(&self, name: &str) -> anyhow::Result<PathBuf> {
        let path = self.repo_path(name).context("invalid repository name")?;
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            // Pin the default branch: a bare repo's HEAD follows the host's
            // `init.defaultBranch` otherwise, so a clone of a repo that only
            // has `main` can check out nothing (CI runners default to
            // `master`).
            let out = Command::new("git")
                .args(["init", "--bare", "-b", "main"])
                .arg(&path)
                .output()
                .await
                .context("git init failed")?;
            anyhow::ensure!(
                out.status.success(),
                "git init --bare failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            tracing::info!("forge: created repo `{name}` (push-to-create)");
        }
        self.ensure_policy(&path).await?;
        Ok(path)
    }

    /// Install (idempotently) the update hook and the receive policies.
    /// Runs on every receive-pack path so repos created before the policy
    /// existed are retrofitted.
    async fn ensure_policy(&self, path: &Path) -> anyhow::Result<()> {
        let hooks = path.join("hooks");
        std::fs::create_dir_all(&hooks)?;
        let hook = hooks.join("update");
        let needs_write = std::fs::read_to_string(&hook)
            .map(|existing| existing != UPDATE_HOOK)
            .unwrap_or(true);
        if needs_write {
            std::fs::write(&hook, UPDATE_HOOK)
                .with_context(|| format!("writing {}", hook.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755))?;
            }
        }
        for (key, value) in [
            ("receive.denyDeletes", "true"),
            ("receive.denyNonFastForwards", "true"),
        ] {
            let out = Command::new("git")
                .arg("--git-dir")
                .arg(path)
                .args(["config", key, value])
                .output()
                .await?;
            anyhow::ensure!(
                out.status.success(),
                "git config {key} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    /// Hosted repo names (the `<name>` of each `<name>.git` directory).
    pub fn list_repos(&self) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(&self.repos_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| {
                        let p = e.path();
                        p.is_dir() && p.extension().is_some_and(|x| x == "git")
                    })
                    .filter_map(|e| {
                        e.path()
                            .file_stem()
                            .map(|s| s.to_string_lossy().into_owned())
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }
}

/// The `update` hook installed in every hosted repo. A session token may
/// update only its own checkpoint branch (`refs/heads/smith/<session>`,
/// via SMITH_GIT_SESSION) and the shared integration branches; the daemon
/// token (no session) and `write_all` tokens may update anything.
const UPDATE_HOOK: &str = "#!/bin/sh\n\
# smith forge ref policy (installed by smith; do not edit).\n\
ref=\"$1\"\n\
if [ -n \"${SMITH_GIT_WRITE_ALL:-}\" ]; then exit 0; fi\n\
session=\"${SMITH_GIT_SESSION:-}\"\n\
if [ -z \"$session\" ]; then exit 0; fi\n\
case \"$ref\" in\n\
  \"refs/heads/smith/$session\") exit 0 ;;\n\
  refs/heads/smith/integration/*) exit 0 ;;\n\
esac\n\
echo \"smith forge: session tokens may only update refs/heads/smith/<session> \
and smith/integration/*\" >&2\n\
exit 1\n";

/// Extract a git token from axum request headers: Bearer, or Basic with the
/// token as the password (git's shape). Used by the api's forge middleware.
pub fn token_from_axum_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(axum::http::header::AUTHORIZATION) {
        let value = v.to_str().ok()?;
        if let Some(b) = value.strip_prefix("Bearer ") {
            let b = b.trim();
            return (!b.is_empty()).then(|| b.to_string());
        }
        if let Some(basic) = value.strip_prefix("Basic ") {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(basic.trim())
                .ok()?;
            let s = String::from_utf8(decoded).ok()?;
            let (_user, pass) = s.split_once(':')?;
            return (!pass.is_empty()).then(|| pass.to_string());
        }
    }
    None
}

/// Repo names: alphanumeric start, then alphanumerics plus `.`, `_`, `-`.
/// No traversal, no whitespace, bounded length.
fn valid_repo_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 100
        && !name.contains("..")
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_names_are_validated() {
        assert!(valid_repo_name("demo"));
        assert!(valid_repo_name("my-repo.v2"));
        assert!(!valid_repo_name(""));
        assert!(!valid_repo_name(".hidden"));
        assert!(!valid_repo_name("a/b"));
        assert!(!valid_repo_name("a..b"));
        assert!(!valid_repo_name("has space"));
        assert!(!valid_repo_name(&"x".repeat(101)));
    }

    #[test]
    fn basic_and_bearer_tokens_extract_from_axum_headers() {
        use axum::http::HeaderMap;
        let mk = |auth: &str| {
            let mut h = HeaderMap::new();
            h.insert(
                axum::http::header::AUTHORIZATION,
                auth.parse().expect("header value"),
            );
            h
        };
        // git sends Basic with the token as the password.
        let basic = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("smith-user:tok-123")
        );
        assert_eq!(
            token_from_axum_headers(&mk(&basic)).as_deref(),
            Some("tok-123")
        );
        // Bearer for curl.
        assert_eq!(
            token_from_axum_headers(&mk("Bearer tok-123")).as_deref(),
            Some("tok-123")
        );
        assert_eq!(token_from_axum_headers(&mk("Digest xyz")), None);
    }

    /// S-14 part 2: an expired grant refuses to authenticate. (A ttl of 0
    /// means "never expires" — use a tiny ttl and let the clock pass it.)
    #[test]
    fn authenticate_refuses_expired_grants() {
        let tmp = tempfile::tempdir().unwrap();
        let forge = Forge::with_ttl(tmp.path(), 500); // half a second
        let token = forge.mint("s-expired", "demo", true, true, false).unwrap();
        assert!(forge.authenticate(&token).is_some(), "valid within the ttl");
        std::thread::sleep(std::time::Duration::from_millis(600));
        assert!(
            forge.authenticate(&token).is_none(),
            "an expired grant must not authenticate"
        );
    }

    /// S-14 part 2: recovery drops expired grants instead of trusting them.
    #[test]
    fn recover_skips_expired_grants() {
        let tmp = tempfile::tempdir().unwrap();
        // A grant minted a week ago with a one-week TTL: expired on arrival.
        let expired = GitCred {
            session_id: "s-old".into(),
            token_hash: token_hash("stale-token"),
            repo: "demo".into(),
            read: true,
            write: true,
            write_all: false,
            expires_ms: Forge::now_ms() - 1,
        };
        let path = tmp.path().join("s-old.gitcred");
        std::fs::write(&path, serde_json::to_string_pretty(&expired).unwrap()).unwrap();

        let forge = Forge::new(tmp.path());
        assert_eq!(forge.recover(), 0, "expired grants must not be recovered");
        assert!(forge.authenticate("stale-token").is_none());
        // The dead sidecar was cleaned up.
        assert!(!path.exists());
    }

    /// S-14 part 2: revoking a session kills its grant and the `.gitcred`
    /// sidecar, so a restart cannot recover the token.
    #[test]
    fn revoke_session_kills_the_grant() {
        let tmp = tempfile::tempdir().unwrap();
        let forge = Forge::new(tmp.path());
        let token = forge.mint("s1", "demo", true, true, false).unwrap();
        assert!(forge.authenticate(&token).is_some());

        assert_eq!(forge.revoke_session("s1"), 1);
        assert!(forge.authenticate(&token).is_none(), "revoked = no auth");
        assert!(!tmp.path().join("s1.gitcred").exists(), "sidecar removed");
        // Revoking again is a no-op.
        assert_eq!(forge.revoke_session("s1"), 0);
    }

    #[tokio::test]
    async fn mint_authenticate_and_recover() {
        let tmp = tempfile::tempdir().unwrap();
        let forge = Forge::new(tmp.path());
        let token = forge.mint("sess-1", "demo", true, true, false).unwrap();

        let grant = forge.authenticate(&token).expect("token authenticates");
        assert_eq!(grant.repo, "demo");
        assert!(grant.read && grant.write);
        assert!(forge.authenticate("nope").is_none());

        // The sidecar is on disk; a fresh forge (restart) recovers it.
        assert!(tmp.path().join("sess-1.gitcred").exists());
        let forge2 = Forge::new(tmp.path());
        assert_eq!(forge2.recover(), 1);
        assert!(forge2.authenticate(&token).is_some());
    }
}
