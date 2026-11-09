//! Live smoke test for `smith serve`: create a session for a scratch
//! workspace, run a real turn over SSE, approve anything from a second
//! client, replay from zero, switch providers, and cancel a turn.
//!
//! Usage: `cargo run -p smith-cli --example serve_smoke [-- --url 127.0.0.1:9240]`
//! (the daemon must be running: `smith serve`).

use std::time::Duration;

use smith_core::CoreEvent;
use smith_protocol::{ServeClient, WireEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut url = "127.0.0.1:9240".to_string();
    let mut model: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--url" => url = args.next().unwrap_or(url),
            "--model" => model = args.next(),
            _ => {}
        }
    }
    let url = url.trim_start_matches("http://").to_string();
    let (host, port) = match url.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse()?),
        _ => (url.clone(), smith_protocol::DEFAULT_SERVE_PORT),
    };
    let token = smith_protocol::client::read_token_file()
        .context("no serve token — start `smith serve` once first")?;
    let client = ServeClient::new(&host, port, token.clone());
    client.health().await?;
    println!("[1] daemon health ok on {host}:{port}");

    // Scratch workspace (daemon-side path; same machine for the smoke).
    let ws = std::env::temp_dir().join(format!("smith-smoke-{}", std::process::id()));
    std::fs::create_dir_all(&ws)?;
    std::fs::write(ws.join("hello.txt"), "kia ora from the sandbox\n")?;
    let session_id = client
        .create_session(ws.to_str().unwrap(), None, model.as_deref())
        .await?;
    println!("[2] session created: {}", &session_id[..8]);

    // Subscribe first, then prompt: the turn streams live.
    let mut events = client.events(&session_id, 0).await?;
    client
        .start_turn(
            &session_id,
            "Read hello.txt (use the read_file tool) and reply with exactly what it says, then stop.",
        )
        .await?;

    let mut seen_user = false;
    let mut tool_ran = false;
    let mut approved_from_b = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    loop {
        let env = tokio::time::timeout_at(deadline, events.next())
            .await
            .context("timed out waiting for the turn to finish")??;
        let Some(env) = env else {
            anyhow::bail!("event stream ended early");
        };
        let finished = match &env.event {
            WireEvent::Core { core } => match core {
                CoreEvent::UserMessage { .. } => {
                    seen_user = true;
                    println!("  < user prompt echoed to viewers");
                    false
                }
                CoreEvent::ToolStarted { name, detail, .. } => {
                    println!("  < tool {name}: {detail}");
                    false
                }
                CoreEvent::ToolFinished { ok, summary, .. } => {
                    tool_ran = true;
                    println!(
                        "  < tool finished ok={ok}: {}",
                        summary.chars().take(60).collect::<String>()
                    );
                    false
                }
                CoreEvent::ApprovalRequested { call_id, detail } => {
                    // The B-client flow: a second connection decides.
                    println!("  < approval requested: {detail}");
                    let b = ServeClient::new(&host, port, token.clone());
                    b.approval(
                        &session_id,
                        Some(call_id),
                        smith_core::ApprovalDecision::AllowAlways,
                    )
                    .await?;
                    approved_from_b = true;
                    println!("  < client B approved (allow always)");
                    false
                }
                CoreEvent::TurnCompleted => {
                    println!("  < turn completed");
                    true
                }
                CoreEvent::TurnCancelled => {
                    println!("  < turn cancelled");
                    true
                }
                CoreEvent::Error { message } => {
                    println!("  < error: {message}");
                    true
                }
                _ => false,
            },
            WireEvent::Status(s) => {
                println!("  < status busy={} model={}", s.busy, s.model);
                false
            }
            _ => false,
        };
        if finished {
            break;
        }
    }
    println!(
        "[3] turn done: user_echo={seen_user} tool_ran={tool_ran} approved_from_b={approved_from_b}"
    );

    // Replay from zero on a fresh connection: the whole transcript again.
    let mut replay = client.events(&session_id, 0).await?;
    let mut n = 0u64;
    loop {
        let env = tokio::time::timeout(Duration::from_secs(15), replay.next())
            .await
            .context("replay timed out")??;
        let Some(env) = env else { break };
        n += 1;
        if matches!(
            env.event,
            WireEvent::Core {
                core: CoreEvent::TurnCompleted
            }
        ) {
            break;
        }
    }
    println!("[4] replay: {n} envelopes to the first completion");

    // Provider switch RPC (daemon-side handle; no keys on this machine's env).
    client
        .set_provider(&session_id, Some("ollama"), Some("qwen3:0.6b"))
        .await?;
    let status = client.status(&session_id).await?;
    println!(
        "[5] provider switch -> {} : {}",
        status.provider, status.model
    );

    // Cancel flow: a fresh turn, cancelled immediately.
    let mut tail = client.events(&session_id, 0).await?;
    client
        .start_turn(
            &session_id,
            "Write a haiku about sandboxing into haiku.txt.",
        )
        .await?;
    client.cancel(&session_id).await?;
    let cancelled = loop {
        let env = tokio::time::timeout(Duration::from_secs(30), tail.next())
            .await
            .context("cancel flow timed out")??;
        let Some(env) = env else { break false };
        if matches!(
            env.event,
            WireEvent::Core {
                core: CoreEvent::TurnCancelled
            }
        ) {
            break true;
        }
        if matches!(
            env.event,
            WireEvent::Core {
                core: CoreEvent::TurnCompleted
            }
        ) {
            break false; // finished before the cancel landed — still valid
        }
    };
    println!("[6] cancel flow observed (cancelled={cancelled})");

    let diff = client.diff(&session_id).await?;
    println!("[7] diff: {} chars", diff.len());

    println!("SMOKE OK (session container left running; `smith clean` removes it)");
    Ok(())
}

use anyhow::Context as _;
