//! Hermetic integration test: the bundled `mcp-echo` server binary is
//! spawned over stdio, listed, and called. No network, no external runtime.

use std::collections::BTreeMap;

use smith_mcp::{ApprovalMode, McpConfig, McpServerConfig, McpSession};

fn echo_server_config() -> McpConfig {
    let mut servers = BTreeMap::new();
    servers.insert(
        "echo".to_string(),
        McpServerConfig {
            command: env!("CARGO_BIN_EXE_mcp-echo").to_string(),
            args: vec![],
            env: BTreeMap::new(),
            approval: ApprovalMode::Ask,
            timeout_secs: 30,
        },
    );
    McpConfig { servers }
}

#[tokio::test]
async fn connect_list_and_call_the_mock_server() {
    let mut session = McpSession::connect(&echo_server_config()).await;

    // Server connected and its tools are bridged under the smith name.
    let status = session.status_lines();
    assert_eq!(status.len(), 1, "expected exactly one connected server");
    assert!(status[0].starts_with("✓ echo"), "status: {status:?}");

    let mut tools = session.take_tools();
    assert_eq!(tools.len(), 1);
    let tool = tools.remove(0);

    let def = tool.def();
    assert_eq!(def.name, "mcp_echo_echo");
    assert!(def.description.contains("MCP tool from server `echo`"));
    assert!(
        def.parameters.get("properties").is_some(),
        "schema: {def:?}"
    );
    assert!(
        tool.needs_approval("{}"),
        "ask-mode server must require approval"
    );

    // Call it through the Tool seam.
    let output = tool
        .run(test_executor(), r#"{"message":"kia ora"}"#)
        .await
        .unwrap();
    assert_eq!(output, "echo: kia ora");

    session.shutdown().await;
}

#[tokio::test]
async fn failing_server_is_reported_not_fatal() {
    let mut servers = BTreeMap::new();
    servers.insert(
        "broken".to_string(),
        McpServerConfig {
            // Something that exists but is not an MCP server.
            command: std::env::var("OS")
                .map(|_| "cmd".to_string())
                .unwrap_or("sh".into()),
            args: vec!["/c".into(), "exit".into()],
            env: BTreeMap::new(),
            approval: ApprovalMode::Ask,
            timeout_secs: 5,
        },
    );
    let config = McpConfig { servers };
    let mut session = McpSession::connect(&config).await;

    assert!(session.take_tools().is_empty());
    let status = session.status_lines();
    assert_eq!(status.len(), 1);
    assert!(status[0].starts_with("✗ broken"), "status: {status:?}");
    session.shutdown().await;
}

/// Minimal executor stub: MCP tools never use the workspace executor.
fn test_executor() -> &'static dyn smith_core::ToolExecutor {
    struct Stub;
    #[async_trait::async_trait]
    impl smith_core::ToolExecutor for Stub {
        async fn exec_shell(
            &self,
            _command: &str,
            _timeout: std::time::Duration,
        ) -> anyhow::Result<smith_core::ExecResult> {
            unreachable!()
        }
        async fn read_file(
            &self,
            _path: &str,
            _offset: Option<u64>,
            _limit: Option<u64>,
        ) -> anyhow::Result<smith_core::FileContents> {
            unreachable!()
        }
        async fn write_file(&self, _path: &str, _contents: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn list_dir(&self, _path: &str) -> anyhow::Result<Vec<String>> {
            unreachable!()
        }
        fn workspace_root(&self) -> &str {
            "/workspace"
        }
    }
    use std::sync::OnceLock;
    static STUB: OnceLock<Stub> = OnceLock::new();
    STUB.get_or_init(|| Stub)
}
