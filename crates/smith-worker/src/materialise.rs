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

// ------------------------------------------------------- merge gate --

/// A merge in progress: the temp worktree at the integration branch plus
/// the branch's pre-merge commit (the abort target). The caller runs the
/// gate command (if any) and then either [`commit_gate`] or [`abort_gate`].
#[derive(Debug)]
pub struct GateWorkspace {
    pub worktree: PathBuf,
    /// The integration branch's commit before the merge.
    pub integration_sha: String,
    /// The merge's output (for the planner's tool result).
    pub output: String,
}

/// The pure-git half of a merge gate. `Failed` is a planner-actionable
/// outcome (conflict, or no checkpoint branch to merge), not an internal
/// error — infra failures (no git, no cache) stay anyhow errors.
#[derive(Debug)]
pub enum GateStep {
    Merged(GateWorkspace),
    Failed { conflict: bool, output: String },
}

/// The bare-clone cache directory for a source (shared by every session of
/// one repo URL on this worker).
pub fn cache_dir(source: &WorkspaceSource) -> anyhow::Result<PathBuf> {
    Ok(work_dir()?.join(format!("cache-{}", fnv64(&source.git_url))))
}

fn token_of(source: &WorkspaceSource) -> Option<String> {
    source
        .token_env
        .as_ref()
        .and_then(|name| std::env::var(name).ok())
        .filter(|t| !t.is_empty())
}

/// Merge the session's checkpoint branch into the integration branch.
///
/// The cache is fetched first (the checkpoint push and other workers'
/// integration commits land as heads), the integration branch is seeded
/// from the session's materialisation ref when missing, and the merge runs
/// `--no-ff` in a temp worktree for reviewability. Nothing here is
/// destructive: a conflict leaves the integration branch untouched.
pub fn merge_for_gate(
    source: &Option<WorkspaceSource>,
    session_id: &str,
    integration_ref: &str,
) -> anyhow::Result<GateStep> {
    let Some(source) = source else {
        anyhow::bail!(
            "merge gate needs a git-backed workspace (scratch sessions have no branches)"
        );
    };
    let cache = cache_dir(source)?;
    let token = token_of(source);
    git(
        &[
            "fetch".into(),
            "--prune".into(),
            "origin".into(),
            "+refs/heads/*:refs/heads/*".into(),
        ],
        Some(&cache),
        token.as_deref(),
    )?;

    let child_branch = format!("smith/{session_id}");
    if git(
        &["rev-parse".into(), "--verify".into(), child_branch.clone()],
        Some(&cache),
        None,
    )
    .is_err()
    {
        return Ok(GateStep::Failed {
            conflict: false,
            output: format!(
                "checkpoint branch {child_branch} not found — the child has no committed work"
            ),
        });
    }

    // Seed the integration branch at the repo's base ref on first gate.
    let have_integration = git(
        &[
            "rev-parse".into(),
            "--verify".into(),
            integration_ref.into(),
        ],
        Some(&cache),
        None,
    )
    .is_ok();
    if !have_integration {
        git(
            &[
                "branch".into(),
                integration_ref.into(),
                source.git_ref.clone(),
            ],
            Some(&cache),
            None,
        )?;
    }
    let integration_sha = git(
        &[
            "rev-parse".into(),
            "--verify".into(),
            format!("{integration_ref}^{{commit}}"),
        ],
        Some(&cache),
        None,
    )?
    .trim()
    .to_string();

    // Temp worktree at the integration branch (prune first: a crashed
    // previous gate may have left it registered).
    let gate_wt = work_dir()?.join(format!("gate-{session_id}"));
    let _ = git(&["worktree".into(), "prune".into()], Some(&cache), None);
    if gate_wt.exists() {
        let _ = std::fs::remove_dir_all(&gate_wt);
    }
    git(
        &[
            "worktree".into(),
            "add".into(),
            gate_wt.display().to_string(),
            integration_ref.into(),
        ],
        Some(&cache),
        None,
    )?;

    let merge_msg = format!("smith merge gate: {session_id}");
    match git(
        &[
            "-c".into(),
            "user.name=smith-worker".into(),
            "-c".into(),
            "user.email=smith@worker".into(),
            "merge".into(),
            "--no-ff".into(),
            "-m".into(),
            merge_msg,
            child_branch,
        ],
        Some(&gate_wt),
        None,
    ) {
        Ok(output) => Ok(GateStep::Merged(GateWorkspace {
            worktree: gate_wt,
            integration_sha,
            output,
        })),
        Err(e) => {
            // Drop the conflicted worktree; the branch ref never advanced.
            let _ = git(
                &[
                    "worktree".into(),
                    "remove".into(),
                    "--force".into(),
                    gate_wt.display().to_string(),
                ],
                Some(&cache),
                None,
            );
            let _ = std::fs::remove_dir_all(&gate_wt);
            Ok(GateStep::Failed {
                conflict: true,
                output: e.to_string(),
            })
        }
    }
}

/// Gate passed: push the integration branch to the origin (best-effort —
/// the local branch has advanced regardless) and drop the temp worktree.
/// Returns whether the push landed.
pub fn commit_gate(
    source: &Option<WorkspaceSource>,
    gate: &GateWorkspace,
    integration_ref: &str,
) -> anyhow::Result<bool> {
    let pushed = match source {
        Some(source) => git(
            &[
                "push".into(),
                "origin".into(),
                format!("{integration_ref}:refs/heads/{integration_ref}"),
            ],
            Some(&gate.worktree),
            token_of(source).as_deref(),
        )
        .is_ok(),
        None => false,
    };
    remove_gate_worktree(source, gate)?;
    Ok(pushed)
}

/// Push one local ref of the cache onto a branch on the origin (M15
/// convergence: the integration branch onto the base ref, opt-in only).
pub fn push_branch(
    source: &Option<WorkspaceSource>,
    from_ref: &str,
    to_ref: &str,
) -> anyhow::Result<String> {
    let Some(source) = source else {
        bail!("push needs a git-backed workspace (scratch sessions have no remote)");
    };
    let cache = cache_dir(source)?;
    git(
        &[
            "push".into(),
            "origin".into(),
            format!("{from_ref}:refs/heads/{to_ref}"),
        ],
        Some(&cache),
        token_of(source).as_deref(),
    )
}

/// Gate failed: roll the integration branch back to its pre-merge commit
/// and drop the temp worktree. The branch is as the gate found it.
pub fn abort_gate(source: &Option<WorkspaceSource>, gate: &GateWorkspace) -> anyhow::Result<()> {
    git(
        &[
            "reset".into(),
            "--hard".into(),
            gate.integration_sha.clone(),
        ],
        Some(&gate.worktree),
        None,
    )?;
    remove_gate_worktree(source, gate)
}

fn remove_gate_worktree(
    source: &Option<WorkspaceSource>,
    gate: &GateWorkspace,
) -> anyhow::Result<()> {
    if let Some(source) = source {
        let cache = cache_dir(source)?;
        let _ = git(
            &[
                "worktree".into(),
                "remove".into(),
                "--force".into(),
                gate.worktree.display().to_string(),
            ],
            Some(&cache),
            None,
        );
    }
    if gate.worktree.exists() {
        std::fs::remove_dir_all(&gate.worktree)?;
    }
    Ok(())
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

        // ---- merge gate (M15): clean merge, conflict, abort ----
        // Same scratch of the env var (single-threaded env use); a file://
        // remote exercises the fetch/seed/merge/push cycle without a network.
        let integration = "smith/integration/parent1";

        // Two children edit the same file from the same base — whichever
        // gates second must conflict.
        for (sid, text) in [
            ("gate-child-1", "child one\n"),
            ("gate-child-2", "child two\n"),
        ] {
            let wt = materialise(sid, &Some(source.clone())).unwrap();
            std::fs::write(wt.join("README.md"), text).unwrap();
            let (commit, pushed) = checkpoint(&wt, &Some(source.clone()), sid).unwrap();
            assert_ne!(commit, "none", "{sid}");
            assert!(pushed, "{sid}");
        }

        // Clean merge: child 1 gates first; the integration branch lands on
        // the remote with the child's work merged in (--no-ff).
        let step = merge_for_gate(&Some(source.clone()), "gate-child-1", integration).unwrap();
        let GateStep::Merged(gate) = step else {
            panic!("expected a clean merge, got {step:?}")
        };
        let pushed = commit_gate(&Some(source.clone()), &gate, integration).unwrap();
        assert!(pushed, "file:// push must land");
        let shown = run(
            &["show", &format!("{integration}:README.md")],
            Some(&origin),
        );
        assert_eq!(shown.trim(), "child one");
        assert!(
            !gate.worktree.exists(),
            "temp gate worktree must be removed"
        );
        let merge_commit_type = run(
            &["cat-file", "-t", &format!("refs/heads/{integration}")],
            Some(&origin),
        );
        assert_eq!(merge_commit_type.trim(), "commit");

        // Conflict: child 2's competing edit surfaces to the planner; the
        // integration branch is untouched.
        let before = run(&["rev-parse", integration], Some(&origin));
        let step = merge_for_gate(&Some(source.clone()), "gate-child-2", integration).unwrap();
        match step {
            GateStep::Failed {
                conflict: true,
                output,
            } => assert!(output.contains("CONFLICT"), "{output}"),
            other => panic!("expected a conflict, got {other:?}"),
        }
        let after = run(&["rev-parse", integration], Some(&origin));
        assert_eq!(before, after, "failed gate must not advance the branch");

        // Abort: a clean merge whose gate fails rolls the branch back.
        let wt3 = materialise("gate-child-3", &Some(source.clone())).unwrap();
        std::fs::write(wt3.join("extra.txt"), "child three\n").unwrap();
        checkpoint(&wt3, &Some(source.clone()), "gate-child-3").unwrap();
        let step = merge_for_gate(&Some(source.clone()), "gate-child-3", integration).unwrap();
        let GateStep::Merged(gate3) = step else {
            panic!("expected a clean merge for child 3")
        };
        let before = run(&["rev-parse", integration], Some(&origin));
        abort_gate(&Some(source.clone()), &gate3).unwrap();
        let after = run(&["rev-parse", integration], Some(&origin));
        assert_eq!(before, after, "aborted gate must not advance the branch");
        assert!(
            !gate3.worktree.exists(),
            "temp gate worktree must be removed"
        );
    }
}
