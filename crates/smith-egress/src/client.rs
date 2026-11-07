//! Minimal client for the proxy's clearance control API (raw HTTP over
//! loopback TCP — no HTTP framework, matching the proxy's own hand-rolled
//! server). Used by the TUI poller and headless runs.

use anyhow::Context;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

/// Clearance control API port (the sidecar publishes it on host loopback).
pub const CLEARANCE_CONTROL_PORT: u16 = 9239;

/// Read the clearance token from `~/.smith/egress-token` (written by
/// whoever started the sidecar). `None` = no clearance configured.
pub fn read_token() -> Option<String> {
    // std-only home resolution: this crate is built standalone in the
    // container and must not grow host-crate dependencies.
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .ok()?;
    let path = PathBuf::from(home).join(".smith").join("egress-token");
    std::fs::read_to_string(path)
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

fn request(
    port: u16,
    method: &str,
    path: &str,
    token: &str,
    body: Option<&str>,
) -> anyhow::Result<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .with_context(|| format!("connecting to the clearance API on :{port}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .context("set read timeout")?;
    let auth = format!("Authorization: Bearer {token}\r\n");
    let length = body
        .map(|b| format!("Content-Length: {}\r\n", b.len()))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth}{length}Connection: close\r\n\r\n{}",
        body.unwrap_or("")
    );
    stream.write_all(request.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status: u16 = text
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .context("malformed control response")?;
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Ok((status, body))
}

/// Currently pending clearance requests: `(id, host)`.
pub fn get_pending(port: u16, token: &str) -> anyhow::Result<Vec<(u64, String)>> {
    let (status, body) = request(port, "GET", "/pending", token, None)?;
    anyhow::ensure!(status == 200, "clearance API returned {status}");
    let parsed: serde_json::Value = serde_json::from_str(&body).context("bad pending JSON")?;
    Ok(parsed
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    Some((v.get("id")?.as_u64()?, v.get("host")?.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default())
}

/// Answer a pending request: `allow`, `always` or `deny`.
pub fn post_decide(port: u16, token: &str, id: u64, decision: &str) -> anyhow::Result<()> {
    let (status, body) = request(
        port,
        "POST",
        "/decide",
        token,
        Some(&serde_json::json!({ "id": id, "decision": decision }).to_string()),
    )?;
    anyhow::ensure!(status == 200, "decide returned {status}: {body}");
    Ok(())
}

/// Set the clearance mode: `ask`, `deny` or `allow`.
pub fn set_mode(port: u16, token: &str, mode: &str) -> anyhow::Result<()> {
    let (status, body) = request(
        port,
        "POST",
        "/mode",
        token,
        Some(&serde_json::json!({ "mode": mode }).to_string()),
    )?;
    anyhow::ensure!(status == 200, "mode set returned {status}: {body}");
    Ok(())
}
