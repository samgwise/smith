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
`n` to refuse. Risky commands (`git push`, `git reset`, `rm`, `curl`, `wget`,
`sudo`, `dd`, `chmod 777`) always re-prompt — "a" binds to the exact command,
never the prefix. Headless `smith run` auto-approves (the sandbox is the
boundary), and repeated denials escalate: after three, the agent is told to
stop retrying and wait.

## Egress control

`sandbox.network` (default `proxy`) controls what the agent container can reach:

- `proxy` — all HTTP(S) traffic routes through an allowlist-filtering proxy
  sidecar (`smith-egress-proxy`, built once inside the podman machine from
  the bundled Containerfile). HTTPS is filtered from the `CONNECT` hostname
  without decryption. Default allowlist: crates.io, registry.npmjs.org,
  pypi.org, ubuntu/debian repos, github.com + release downloads.
  Override with `egress_allowlist`.
- `none` — no network at all (`--network=none`).
- `open` — unrestricted (the old default; `enabled` accepted as an alias).

Known limitation: proxy env vars are cooperative. A program that ignores them
and dials IP literals directly bypasses the filter. Closing that gap is the
next milestone: an nftables default-deny ruleset inside the podman machine
scoping container egress to the proxy + DNS only. `smith clean` also removes
the proxy sidecar.

## Project instructions

If the workspace root contains `AGENTS.md` (or `CLAUDE.md`), its contents are
loaded once per session into the system prompt. The file is read at session
start only, keeping the prompt prefix byte-stable so provider prompt caches
keep hitting.

## MCP servers

smith connects to stdio MCP servers and exposes their tools to the agent as
`mcp_<server>_<tool>`. Global servers live in `~/.smith/config.toml`:

```toml
[mcp.servers.github]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "${GITHUB_TOKEN}" }   # expanded from your environment
approval = "ask"                             # or "allow" to pre-approve all
                                             # tools from this server
timeout_secs = 60
```

Project-scoped servers go in `.smith/mcp.toml` at the workspace root (same
shape, `[servers.<name>]`, wins over user config on name collisions).

Security notes: servers run **host-side** with only the explicitly configured
environment (plus PATH) — never your full environment — so the sandbox is not
a boundary for them; that is why `approval = "ask"` is the default and routes
every call through the same modal as shell commands. Servers are connected at
session start and their tool list is fixed for the session (prompt-cache
friendly); a server that fails to start is reported by `/mcp` and skipped.

## Verify gate (headless)

Give headless runs a finish line in `~/.smith/config.toml`:

```toml
[verify]
command = "cargo test -q"   # run in the sandbox after each task
max_rounds = 2              # fix-and-retry rounds on failure
```

After the agent finishes a task, smith runs the command in the container; if
it fails, the output is fed back for a fixing turn (up to `max_rounds`).
Interactive TUI sessions are gate-free — you verify by eye.

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
