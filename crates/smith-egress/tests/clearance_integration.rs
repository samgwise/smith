//! Hermetic clearance tests: spawn the real proxy binary on localhost and
//! drive the control API. No podman, no network.

use std::process::Child;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOKEN: &str = "test-token-123";

/// Tiny TCP echo server; answers one read per connection.
async fn echo_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                if let Ok(n) = stream.read(&mut buf).await
                    && n > 0
                {
                    let _ = stream.write_all(&buf[..n]).await;
                }
            });
        }
    });
    port
}

/// Kills the proxy when the test ends — including on panic — so failed
/// tests cannot leave orphaned listeners that hang later runs.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

/// Spawn the proxy with an empty allowlist (everything needs clearance).
fn spawn_proxy(mode: &str, data_port: u16, control_port: u16) -> ChildGuard {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_smith-egress-proxy"));
    cmd.env("EGRESS_ALLOWLIST", "none")
        .env("EGRESS_CLEARANCE", mode)
        .env("EGRESS_PORT", data_port.to_string())
        .env("EGRESS_CONTROL_PORT", control_port.to_string())
        .env("CLEARANCE_TOKEN", TOKEN)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let child = ChildGuard(cmd.spawn().expect("spawn smith-egress-proxy"));
    // Wait for both listeners to open before driving the flow.
    while !port_open(data_port) || !port_open(control_port) {
        std::thread::sleep(Duration::from_millis(25));
    }
    child
}

fn port_open(port: u16) -> bool {
    std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
}

/// Minimal one-request HTTP client for the control API.
async fn control(
    port: u16,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let length = body
        .map(|b| format!("Content-Length: {}\r\n", b.len()))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth}{length}Connection: close\r\n\r\n{}",
        body.unwrap_or("")
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status: u16 = text
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

/// Open a CONNECT to the proxy and return the stream once the tunnel is
/// established (fails on 403).
async fn connect_via_proxy(proxy_port: u16, target: &str) -> Result<TcpStream, u16> {
    let mut stream = TcpStream::connect(("127.0.0.1", proxy_port)).await.unwrap();
    let request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 512];
    let n = stream.read(&mut buf).await.unwrap();
    let head = String::from_utf8_lossy(&buf[..n]).into_owned();
    if head.starts_with("HTTP/1.1 200") {
        Ok(stream)
    } else {
        Err(head
            .lines()
            .next()
            .and_then(|l| l.split(' ').nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }
}

#[tokio::test]
async fn ask_mode_holds_then_tunnels_on_allow() {
    let echo_port = echo_server().await;
    let (data_port, control_port) = (19310, 19241);
    let _child = spawn_proxy("ask", data_port, control_port);

    // CONNECT before any decision: held, no response yet.
    let target = format!("127.0.0.1:{echo_port}");
    let holder = tokio::spawn(async move { connect_via_proxy(data_port, &target).await });

    // Give the proxy a moment to register the pending request.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (status, pending) = control(control_port, "GET", "/pending", Some(TOKEN), None).await;
    assert_eq!(status, 200, "{pending}");
    assert!(pending.contains("127.0.0.1"), "pending: {pending}");
    let id: u64 = serde_json::from_str::<serde_json::Value>(&pending)
        .unwrap()
        .get(0)
        .and_then(|v| v.get("id"))
        .and_then(serde_json::Value::as_u64)
        .expect("pending entry with id");

    let (status, body) = control(
        control_port,
        "POST",
        "/decide",
        Some(TOKEN),
        Some(&format!(r#"{{"id":{id},"decision":"allow"}}"#)),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("true"), "applied: {body}");

    // The held tunnel completes and reaches the echo server.
    let mut stream = tokio::time::timeout(Duration::from_secs(5), holder)
        .await
        .expect("held connect resolved")
        .unwrap()
        .expect("tunnel established");
    stream.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}

#[tokio::test]
async fn deny_mode_rejects_immediately() {
    let echo_port = echo_server().await;
    let (data_port, control_port) = (19311, 19242);
    let _child = spawn_proxy("deny", data_port, control_port);

    let status = connect_via_proxy(data_port, &format!("127.0.0.1:{echo_port}"))
        .await
        .unwrap_err();
    assert_eq!(status, 403);
}

#[tokio::test]
async fn control_api_requires_the_token() {
    let (data_port, control_port) = (19312, 19243);
    let _child = spawn_proxy("deny", data_port, control_port);

    let (status, _) = control(control_port, "GET", "/pending", None, None).await;
    assert_eq!(status, 401);
    let (status, _) = control(control_port, "GET", "/pending", Some("wrong"), None).await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn always_decision_covers_the_host_without_reprompting() {
    let echo_port = echo_server().await;
    let (data_port, control_port) = (19313, 19244);
    let _child = spawn_proxy("ask", data_port, control_port);
    let target = format!("127.0.0.1:{echo_port}");

    // First connect: held; answer "always".
    let holder = tokio::spawn({
        let target = target.clone();
        async move { connect_via_proxy(data_port, &target).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_, pending) = control(control_port, "GET", "/pending", Some(TOKEN), None).await;
    let id: u64 = serde_json::from_str::<serde_json::Value>(&pending)
        .unwrap()
        .get(0)
        .and_then(|v| v.get("id"))
        .and_then(serde_json::Value::as_u64)
        .expect("pending id");
    control(
        control_port,
        "POST",
        "/decide",
        Some(TOKEN),
        Some(&format!(r#"{{"id":{id},"decision":"always"}}"#)),
    )
    .await;
    let tunnel = tokio::time::timeout(Duration::from_secs(5), holder)
        .await
        .expect("first tunnel resolved")
        .unwrap();
    assert!(tunnel.is_ok(), "tunnel failed with {:#?}", tunnel.err());

    // Second connect to the same host: tunnel opens with no new pending.
    let mut stream = tokio::time::timeout(
        Duration::from_secs(3),
        connect_via_proxy(data_port, &target),
    )
    .await
    .expect("second connect resolved without clearance wait")
    .expect("tunnel established");
    stream.write_all(b"pong").await.unwrap();
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
}
