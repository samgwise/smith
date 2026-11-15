//! Worker-side workspace materialisation: bare-clone cache per repo URL,
//! a git worktree per session, and per-turn checkpoint commits.
//!
//! Credential scoping: the token rides the per-invocation credential-helper
//! env, scoped with git's `credential.<url>.helper` to the origin the
//! controller bound it to (`credential_origin`), and the helper snippet reads
//! it from `SMITH_CRED_PASSWORD` — never spliced into a shell snippet, never
//! offered to another host, never written to disk.

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

/// The per-invocation credential env for one origin/token pair. The helper
/// is scoped to `origin` with git's `credential.<url>.helper` — git only
/// offers the token there, never to another host that asks (S-02) — and the
/// helper snippet reads the token from `SMITH_CRED_PASSWORD` so no
/// token-shaped text is ever spliced into a shell snippet (S-32).
fn credential_env(origin: &str, token: &str) -> Vec<(String, String)> {
    vec![
        ("GIT_CONFIG_COUNT".to_string(), "1".to_string()),
        (
            "GIT_CONFIG_KEY_0".to_string(),
            format!("credential.{origin}.helper"),
        ),
        (
            "GIT_CONFIG_VALUE_0".to_string(),
            "!f() { echo \"username=smith-token\"; echo \"password=$SMITH_CRED_PASSWORD\"; }; f"
                .to_string(),
        ),
        ("SMITH_CRED_PASSWORD".to_string(), token.to_string()),
    ]
}

/// The credential scope for a source: `(origin, token)` where the origin is
/// the one the controller bound the token to, falling back to the source
/// URL's own origin. `None` = no credential helper at all.
fn credential_scope(source: &WorkspaceSource) -> Option<(String, String)> {
    let token = source
        .token
        .clone()
        .filter(|t| !t.trim().is_empty())
        .or_else(|| {
            source
                .token_env
                .as_ref()
                .and_then(|name| std::env::var(name).ok())
                .filter(|t| !t.is_empty())
        })?;
    let origin = source
        .credential_origin
        .clone()
        .or_else(|| smith_protocol::url_origin(&source.git_url))
        .filter(|o| !o.is_empty())?;
    Some((origin, token))
}

/// A git ref must not start with `-` (option injection) or contain
/// whitespace (S-29).
fn validate_git_ref(git_ref: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!git_ref.is_empty(), "empty git ref");
    anyhow::ensure!(
        !git_ref.starts_with('-'),
        "git ref must not start with `-`: {git_ref}"
    );
    anyhow::ensure!(
        !git_ref.chars().any(char::is_whitespace),
        "git ref must not contain whitespace: {git_ref}"
    );
    Ok(())
}

/// Git command. The token (if any) reaches git ONLY through the scoped
/// per-invocation credential env — never in process arguments, never on
/// disk. The worker is headless: credential prompting is off, so a failed
/// auth fails fast instead of stalling the session.
fn git(
    args: &[String],
    cwd: Option<&Path>,
    credentials: Option<&(String, String)>,
) -> anyhow::Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(args);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "echo");
    if let Some((origin, token)) = credentials {
        for (key, value) in credential_env(origin, token) {
            cmd.env(key, value);
        }
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
    validate_git_ref(&source.git_ref)?;

    let base = work_dir()?;
    let cache = base.join(format!("cache-{}", fnv64(&source.git_url)));

    // The credential scope for THIS invocation: the token plus the origin
    // it is bound to (S-02).
    let credentials = credential_scope(source);

    // Bare clone cache: clone once, fetch afterwards. The token rides the
    // per-invocation scoped credential-helper env; the cache keeps a clean
    // URL.
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
            credentials.as_ref(),
        )?;
    } else {
        git(
            &[
                "clone".into(),
                "--bare".into(),
                "--".into(),
                source.git_url.clone(),
                cache.display().to_string(),
            ],
            None,
            credentials.as_ref(),
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
            "--".into(),
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
    // The worktree is detached: push HEAD to the checkpoint branch ref
    // (which we never need to materialise locally).
    let pushed = git(
        &[
            "push".into(),
            "origin".into(),
            "--".into(),
            format!("HEAD:refs/heads/{branch}"),
        ],
        Some(worktree),
        credential_scope(source).as_ref(),
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
    validate_git_ref(&source.git_ref)?;
    let cache = cache_dir(source)?;
    let credentials = credential_scope(source);
    git(
        &[
            "fetch".into(),
            "--prune".into(),
            "origin".into(),
            "+refs/heads/*:refs/heads/*".into(),
        ],
        Some(&cache),
        credentials.as_ref(),
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
                "--".into(),
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
            "--".into(),
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
            "--".into(),
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
                "--".into(),
                format!("{integration_ref}:refs/heads/{integration_ref}"),
            ],
            Some(&gate.worktree),
            credential_scope(source).as_ref(),
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
            "--".into(),
            format!("{from_ref}:refs/heads/{to_ref}"),
        ],
        Some(&cache),
        credential_scope(source).as_ref(),
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
    use base64::Engine as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// S-32 red: a token containing shell metacharacters must never be
    /// spliced into the helper snippet — the helper reads it from an env var.
    #[test]
    fn credential_snippet_does_not_contain_the_token() {
        let token = "sekrit-$(pwned)-tok";
        let env = credential_env("https://git.example.com", token);
        let value = &env
            .iter()
            .find(|(k, _)| k == "GIT_CONFIG_VALUE_0")
            .expect("helper config value")
            .1;
        assert!(
            !value.contains(token),
            "the token must not be spliced into the helper snippet: {value}"
        );
        assert!(
            value.contains("$SMITH_CRED_PASSWORD"),
            "the helper must read the token from SMITH_CRED_PASSWORD"
        );
        assert!(
            env.iter()
                .any(|(k, v)| k == "SMITH_CRED_PASSWORD" && v == token),
            "the token rides in its own env var"
        );
    }

    /// S-02 red: a capture server answering 401 for everything must receive
    /// NO token during a clone — the credential helper only answers the
    /// origin the token is bound to, not every host that asks.
    #[tokio::test]
    async fn capture_server_receives_no_token() {
        const TOKEN: &str = "sekrit-capture-token";
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let server = std::thread::spawn({
            let seen = seen.clone();
            let stop = stop.clone();
            move || {
                for stream in listener.incoming() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut stream = stream.unwrap();
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                        .ok();
                    let mut got = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        match std::io::Read::read(&mut stream, &mut chunk) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                got.extend_from_slice(&chunk[..n]);
                                if got.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }
                    let text = String::from_utf8_lossy(&got);
                    for line in text.lines() {
                        if line.to_ascii_lowercase().starts_with("authorization:") {
                            seen.lock().unwrap().push(line.to_string());
                        }
                    }
                    let _ = std::io::Write::write_all(
                        &mut stream,
                        b"HTTP/1.1 401 Unauthorized\r\n\
                          WWW-Authenticate: Basic realm=\"x\"\r\nContent-Length: 0\r\n\r\n",
                    );
                }
            }
        });

        // Scratch the work dir so the test never touches ~/.smith-worker.
        let tmp = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("SMITH_WORKER_WORK_DIR", tmp.path().join("work"));
            // The worker's configured git token, as the real env var.
            std::env::set_var("SMITH_TEST_CAPTURE_TOKEN", TOKEN);
        }
        // An attacker-named repo URL carrying the controller's token_env —
        // the S-02 shape. The token is bound to a DIFFERENT origin (the
        // controller's configured one).
        let source = WorkspaceSource {
            git_url: format!("http://127.0.0.1:{port}/evil.git"),
            git_ref: "main".into(),
            token_env: Some("SMITH_TEST_CAPTURE_TOKEN".into()),
            token: None,
            credential_origin: Some("https://git.example.com".into()),
        };
        // The clone itself fails (401 forever); the assertion is on what the
        // capture server saw.
        let _ = materialise("capture-test", &Some(source));

        stop.store(true, Ordering::Relaxed);
        let _ = std::net::TcpStream::connect(("127.0.0.1", port));
        server.join().unwrap();

        let expected =
            base64::engine::general_purpose::STANDARD.encode(format!("smith-token:{TOKEN}"));
        let seen = seen.lock().unwrap().clone();
        assert!(
            !seen
                .iter()
                .any(|h| h.contains(&expected) || h.contains(TOKEN)),
            "the token must not reach a foreign host; captured: {seen:?}"
        );
    }

    /// S-29: a git ref beginning with `-` (or containing whitespace) is
    /// refused before git runs.
    #[test]
    fn git_ref_validation_refuses_option_injection() {
        assert!(validate_git_ref("main").is_ok());
        assert!(validate_git_ref("smith/integration/p1").is_ok());
        assert!(validate_git_ref("-spam").is_err());
        assert!(validate_git_ref("main -oUnknown").is_err());
        assert!(validate_git_ref("").is_err());
    }

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
            token: None,
            credential_origin: None,
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
