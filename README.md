# smith

A Rust coding agent that works inside a Podman sandbox: TUI front-end,
Ollama (local) or OpenAI (API key) providers, and a hard rule — arbitrary
code only ever runs in the container, never on your host.

## Layout

| Crate | Role |
|---|---|
| `smith-core` | Agent loop, `LlmProvider`/`ToolExecutor`/`Tool` traits, JSONL session log |
| `smith-providers` | Ollama (native `/api/chat`) and OpenAI (chat completions) streaming clients |
| `smith-sandbox` | Podman machine/container lifecycle, exec with timeouts |
| `smith-tui` | Ratatui interface: transcript, tool activity, approval modal |
| `smith-cli` | `smith` binary: TUI, headless runs, sandbox utilities |

## Security model (v1)

1. Provider keys stay host-side and are never injected into the container.
2. Shell commands run only via `podman exec` inside the session container
   (non-root user, memory/CPU/pids limits, wall-clock timeout).
3. File tools are locked to the canonicalised workspace directory.
4. Smith refuses to mount your home directory or sensitive paths
   (`~/.ssh`, `~/.aws`, …).
5. Every tool call is logged to the session JSONL file.

In the TUI, shell commands ask first: `y` once, `a` for the session,
`n` to refuse. Headless `smith run` auto-approves (sandbox is the boundary).

## Requirements

- Rust (stable) — `cargo build --release`
- Podman; on Windows the podman machine is started automatically
- Ollama running for local models, or `OPENAI_API_KEY` for OpenAI

## Usage

```sh
# Interactive TUI (container stays up; clean later)
smith

# One-shot task, auto-approved
smith run "explain what this project does"
smith run --workspace C:\path\to\repo "fix the failing test"

# Override provider/model for one run
smith run --provider openai "summarise the diff"

# Housekeeping
smith clean      # remove all smith session containers
smith sessions   # list session logs in ~/.smith/sessions
```

Config lives at `~/.smith/config.toml` (created on first run with comments):

```toml
provider = "ollama"       # or "openai"
model = "qwen3.8:27b"

[sandbox]
network = "enabled"       # "disabled" runs the container with --network=none
```

## Development

```sh
cargo build
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```
