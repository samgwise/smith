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
use std::process::Stdio;
use std::sync::Mutex;

use anyhow::Context as _;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use smith_protocol::http::Request;

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
}

/// Hosted repos + the token table. Repos live as bare repos under
/// `<state_dir>/repos/<name>.git`; tokens under `<state_dir>/<session>.gitcred`.
pub struct Forge {
    repos_dir: PathBuf,
    state_dir: PathBuf,
    tokens: Mutex<HashMap<String /* token hash */, GitCred>>,
}

impl Forge {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            repos_dir: state_dir.join("repos"),
            state_dir: state_dir.to_path_buf(),
            tokens: Mutex::new(HashMap::new()),
        }
    }

    pub fn repos_dir(&self) -> &Path {
        &self.repos_dir
    }

    /// Mint a per-session token for `repo`. Hashed at rest; the raw token
    /// is returned once (delivered to the worker in `WorkspaceSource`).
    pub fn mint(
        &self,
        session_id: &str,
        repo: &str,
        read: bool,
        write: bool,
    ) -> anyhow::Result<String> {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let cred = GitCred {
            session_id: session_id.to_string(),
            token_hash: token_hash(&token),
            repo: repo.to_string(),
            read,
            write,
        };
        let path = self.state_dir.join(format!("{session_id}.gitcred"));
        std::fs::write(&path, serde_json::to_string_pretty(&cred)?)
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

    /// Resolve a request's git token: Basic (git speaks Basic; the token
    /// is the password) or Bearer (curl-friendly).
    pub fn token_from_request(req: &Request) -> Option<String> {
        if let Some(b) = req.bearer_token() {
            return Some(b.to_string());
        }
        let basic = req
            .header("authorization")?
            .strip_prefix("Basic ")?
            .trim()
            .to_string();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(basic)
            .ok()?;
        let s = String::from_utf8(decoded).ok()?;
        let (_user, pass) = s.split_once(':')?;
        (!pass.is_empty()).then(|| pass.to_string())
    }

    /// Authenticate a token → its grant.
    pub fn authenticate(&self, token: &str) -> Option<GitCred> {
        self.tokens
            .lock()
            .expect("forge tokens poisoned")
            .get(&token_hash(token))
            .cloned()
    }

    /// The path of a hosted repo; `None` when the name is unsafe.
    pub fn repo_path(&self, name: &str) -> Option<PathBuf> {
        if !valid_repo_name(name) {
            return None;
        }
        Some(self.repos_dir.join(format!("{name}.git")))
    }

    /// Push-to-create: `git init --bare` when the repo doesn't exist yet.
    pub async fn ensure_repo(&self, name: &str) -> anyhow::Result<PathBuf> {
        let path = self.repo_path(name).context("invalid repository name")?;
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            let out = Command::new("git")
                .arg("init")
                .arg("--bare")
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
        Ok(path)
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

/// Run one smart-HTTP git subprocess: request body → stdin, stdout back.
/// Git does all the protocol parsing; the daemon streams bytes.
pub async fn run_git_svc(
    repo_path: &Path,
    svc: &str,
    advertise: bool,
    body: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let mut cmd = Command::new("git");
    match (svc, advertise) {
        ("upload-pack", true) => {
            cmd.args(["upload-pack", "--stateless-rpc", "--advertise-refs"]);
        }
        ("upload-pack", false) => {
            cmd.args(["upload-pack", "--stateless-rpc"]);
        }
        ("receive-pack", true) => {
            cmd.args(["receive-pack", "--stateless-rpc", "--advertise-refs"]);
        }
        ("receive-pack", false) => {
            cmd.args(["receive-pack", "--stateless-rpc"]);
        }
        _ => anyhow::bail!("unsupported git service `{svc}` (advertise={advertise})"),
    }
    cmd.arg(repo_path);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning git (is it installed?)")?;
    {
        // Feed the request body, then close stdin — git's stateless RPC
        // answers once its input ends. receive-pack reads continuously,
        // so feeding cannot deadlock its (small) status output.
        let mut stdin = child.stdin.take().context("git stdin")?;
        stdin.write_all(&body).await?;
        stdin.flush().await?;
    }
    let out = child
        .wait_with_output()
        .await
        .context("git subprocess failed")?;
    anyhow::ensure!(
        out.status.success(),
        "git {svc} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(out.stdout)
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
    fn tokens_round_trip_through_basic_and_bearer() {
        use smith_protocol::http::Request;
        let mk = |auth: &str| Request {
            method: "GET".into(),
            path: "/x".into(),
            query: String::new(),
            headers: vec![("authorization".into(), auth.into())],
            body: vec![],
        };
        // git sends Basic with the token as the password.
        let basic = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("smith-user:tok-123")
        );
        assert_eq!(
            Forge::token_from_request(&mk(&basic)).as_deref(),
            Some("tok-123")
        );
        // Bearer for curl.
        assert_eq!(
            Forge::token_from_request(&mk("Bearer tok-123")).as_deref(),
            Some("tok-123")
        );
        assert_eq!(Forge::token_from_request(&mk("Digest xyz")), None);
    }

    #[tokio::test]
    async fn mint_authenticate_and_recover() {
        let tmp = tempfile::tempdir().unwrap();
        let forge = Forge::new(tmp.path());
        let token = forge.mint("sess-1", "demo", true, true).unwrap();

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
