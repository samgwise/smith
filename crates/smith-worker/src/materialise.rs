//! Worker-side workspace materialisation: bare-clone cache per repo URL,
//! a git worktree per session, and per-turn checkpoint commits.
//!
//! Credential scoping: the token is read from the configured env var and
//! only ever injected into the git invocation URL (in memory, per process);
//! the cache's origin URL stays clean and nothing token-shaped is written
//! to disk.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};

use smith_protocol::WorkspaceSource;

/// fnv-64 of a string (cache keys + identity labels), hex — matches
/// smith-core's workspace identity scheme.
fn fnv64(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Root for worker work: `~/.smith-worker/work` (overridable via
/// `SMITH_WORKER_WORK_DIR` — tests use it to stay out of the home dir).
pub fn work_dir() -> anyhow::Result<PathBuf> {
    if let Ok(dir) = std::env::var("SMITH_WORKER_WORK_DIR")
        && !dir.trim().is_empty()
    {
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {dir}"))?;
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .context("cannot resolve home directory")?;
    let dir = Path::new(&home).join(".smith-worker").join("work");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// Git command. The token (if any) reaches git ONLY through a per-invocation
/// credential-helper env — never in process arguments, never on disk.
fn git(args: &[String], cwd: Option<&Path>, token: Option<&str>) -> anyhow::Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(args);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    if let Some(token) = token {
        cmd.env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "credential.helper")
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("!f() {{ echo \"username=smith-token\"; echo \"password={token}\"; }}; f"),
            );
    }
    let out = cmd
        .output()
        .context("failed to run git (is it installed?)")?;
    if !out.status.success() {
        bail!(
            "git {} failed: {}{}",
            args.first().map(String::as_str).unwrap_or(""),
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Materialise (or reuse) a session workspace; returns the host-side
/// directory to bind-mount as `/workspace`.
pub fn materialise(session_id: &str, source: &Option<WorkspaceSource>) -> anyhow::Result<PathBuf> {
    let Some(source) = source else {
        // Scratch workspace: a plain directory (nothing to checkpoint).
        let dir = work_dir()?.join(format!("scratch-{session_id}"));
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        return Ok(dir);
    };

    let base = work_dir()?;
    let cache = base.join(format!("cache-{}", fnv64(&source.git_url)));

    // The token for THIS invocation: from the env var named in the source.
    let token = source
        .token_env
        .as_ref()
        .and_then(|name| std::env::var(name).ok())
        .filter(|t| !t.is_empty());

    // Bare clone cache: clone once, fetch afterwards. The token rides the
    // per-invocation credential-helper env; the cache keeps a clean URL.
    if cache.exists() {
        git(
            &[
                "fetch".into(),
                "--prune".into(),
                "origin".into(),
                "+refs/heads/*:refs/heads/*".into(),
                "+refs/tags/*:refs/tags/*".into(),
            ],
            Some(&cache),
            token.as_deref(),
        )?;
    } else {
        git(
            &[
                "clone".into(),
                "--bare".into(),
                source.git_url.clone(),
                cache.display().to_string(),
            ],
            None,
            token.as_deref(),
        )?;
    }

    // Fresh worktree per session (a rerun replaces it).
    let worktree = base.join(format!("ws-{session_id}"));
    if worktree.exists() {
        // Detach any stale worktree registration, then delete.
        let _ = git(
            &[
                "worktree".into(),
                "remove".into(),
                "--force".into(),
                worktree.display().to_string(),
            ],
            Some(&cache),
            None,
        );
        let _ = std::fs::remove_dir_all(&worktree);
    }
    // Bare clones keep fetched heads as local refs (no origin/ prefix).
    git(
        &[
            "worktree".into(),
            "add".into(),
            "--detach".into(),
            worktree.display().to_string(),
            source.git_ref.clone(),
        ],
        Some(&cache),
        None,
    )?;
    Ok(worktree)
}

/// Checkpoint the session's worktree: commit everything on a
/// `smith/<session>` branch and push best-effort. Returns (commit, pushed).
pub fn checkpoint(
    worktree: &Path,
    source: &Option<WorkspaceSource>,
    session_id: &str,
) -> anyhow::Result<(String, bool)> {
    let branch = format!("smith/{session_id}");
    git(&["add".into(), "-A".into()], Some(worktree), None)?;
    let committed = git(
        &[
            "-c".into(),
            "user.name=smith-worker".into(),
            "-c".into(),
            "user.email=smith@worker".into(),
            "commit".into(),
            "-m".into(),
            format!("smith checkpoint: session {session_id}"),
        ],
        Some(worktree),
        None,
    );
    if committed.is_err() {
        // "nothing to commit" is the normal no-op case.
        return Ok(("none".into(), false));
    }
    let commit = git(
        &["rev-parse".into(), "--short".into(), "HEAD".into()],
        Some(worktree),
        None,
    )?
    .trim()
    .to_string();

    let Some(source) = source else {
        return Ok((commit, false)); // scratch workspace: nothing to push
    };
    let token = source
        .token_env
        .as_ref()
        .and_then(|name| std::env::var(name).ok())
        .filter(|t| !t.is_empty());
    // The worktree is detached: push HEAD to the checkpoint branch ref
    // (which we never need to materialise locally).
    let pushed = git(
        &[
            "push".into(),
            "origin".into(),
            format!("HEAD:refs/heads/{branch}"),
        ],
        Some(worktree),
        token.as_deref(),
    )
    .is_ok();
    Ok((commit, pushed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_and_checkpoint_round_trip() {
        // A local file:// remote exercises the full materialise/checkpoint
        // flow without a network. The work dir is redirected to the tempdir
        // so the test never touches ~/.smith-worker.
        let tmp = tempfile::tempdir().unwrap();
        // Edition 2024: set_var is unsafe (process-global). Single-threaded
        // env use here; the work dir is redirected away from the home dir.
        unsafe {
            std::env::set_var("SMITH_WORKER_WORK_DIR", tmp.path().join("work"));
        }
        let origin = tmp.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();

        let run = |args: &[&str], cwd: Option<&Path>| {
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            git(&args, cwd, None).unwrap()
        };
        // Seed a repo with one commit on main.
        run(&["init", "-b", "main"], Some(&origin));
        std::fs::write(origin.join("README.md"), "seed\n").unwrap();
        run(&["add", "-A"], Some(&origin));
        run(
            &[
                "-c",
                "user.email=s@x",
                "-c",
                "user.name=s",
                "commit",
                "-m",
                "seed",
            ],
            Some(&origin),
        );

        let source = WorkspaceSource {
            git_url: format!(
                "file:///{}",
                origin.display().to_string().replace('\\', "/")
            ),
            git_ref: "main".into(),
            token_env: None,
        };

        // Bare-clone cache + worktree under the work dir.
        let base = work_dir().unwrap();
        let cache = base.join(format!("cache-{}", fnv64(&source.git_url)));
        let _ = std::fs::remove_dir_all(&cache);
        let worktree = materialise("test-session", &Some(source.clone())).unwrap();
        assert!(worktree.join("README.md").exists());

        // Change + checkpoint: a commit on the checkpoint branch.
        std::fs::write(worktree.join("change.txt"), "agent work\n").unwrap();
        let (commit, pushed) =
            checkpoint(&worktree, &Some(source.clone()), "test-session").unwrap();
        assert_ne!(commit, "none");
        // file:// remote: the push actually lands.
        assert!(pushed);

        // The branch exists on the remote.
        let branches = run(&["branch", "-a"], Some(&origin));
        assert!(branches.contains("smith/test-session"), "{branches}");
    }
}
