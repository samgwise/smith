# smith

A Rust coding agent that works inside a Podman sandbox: TUI front-end,
Ollama (local) or OpenAI (API key) providers, and a hard rule — arbitrary
code only ever runs in the container, never on your host.

## Layout

| Crate | Role |
|---|---|
| `smith-core` | Agent loop, `LlmProvider`/`ToolExecutor`/`Tool` traits, compaction, JSONL session log |
| `smith-providers` | Ollama (native `/api/chat`) and OpenAI (chat completions) streaming clients |
| `smith-mcp` | MCP client: stdio servers exposed as `mcp_<server>_<tool>` tools |
| `smith-sandbox` | Podman machine/container lifecycle, exec with timeouts, egress enforcement |
| `smith-egress` | Allowlist-filtering proxy sidecar (CONNECT filter + bundled Containerfile) |
| `smith-protocol` | The frozen serve wire contract: envelopes, SSE framing, raw client |
| `smith-server` | The `smith serve` daemon: hosted sessions, approval routing, clearance poller |
| `smith-tui` | Ratatui interface: transcript, tool activity, approval modal, slash commands |
| `smith-cli` | `smith` binary: TUI, headless runs, serve/attach, resume, housekeeping |

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
- `open` — unrestricted; `enabled` accepted as an alias.

In `proxy` mode the allowlist is **enforced**, not just cooperative: an
nftables default-deny ruleset is installed inside the podman machine
(forward hook scoped to the egress bridge only — the machine's own networking
is untouched). Only the proxy's pinned IP and the network gateway (DNS) may
leave the bridge, so a program that ignores the proxy env and dials IP
literals is dropped at the packet level. Installation is fail-closed: if the
ruleset cannot be installed, the session refuses to start (escape hatch:
`egress_enforce = false`). Machine reboots clear the rules; the next session
reinstalls them. `smith clean` removes the sidecar and the table.

### Per-destination clearance

When the container reaches a host outside the allowlist, the connection is
held and (by default) you are asked live: the same modal as tool approvals,
with `y` once / `a` for the session / `n` deny. `a` adds the host to a
session-scoped runtime allowlist — no prompt again for that host until the
sidecar restarts.

```toml
[sandbox]
egress_clearance = "ask"   # ask (TUI modal) | deny (auto-deny) | allow
```

Headless runs degrade `ask` to `deny` (nothing is attached to answer the
modal) unless you set `allow`. The clearance API on the sidecar is
localhost-published and token-authenticated (token in
`~/.smith/egress-token`); held connections time out to deny after 120 s.

## Project instructions and system prompt

The system prompt is built once per session and byte-stable afterwards (so
provider prompt caches keep hitting). It contains: identity and rules, session
date, the network posture, a **workspace snapshot** (top-level entries,
detected project type, git state and available toolchains — gathered through
the executor seam, degrading gracefully), and the project instruction file.

If the workspace root contains `AGENTS.md` (or `CLAUDE.md`), its contents are
appended under "Project instructions". The file is read at session start
only.

## Remote execution

Run the agent loop on your machine while the sandbox runs elsewhere — any
Linux host with podman and SSH access:

```sh
# One-time: register the host as a podman connection
podman system connection add gpu-box ssh://user@gpu-box.example.com/run/user/1000/podman/podman.sock

# The workspace must exist on the remote host (git clone or rsync)
smith run --connection gpu-box --remote-workspace /home/user/project "fix the failing test"
```

Or set it in config:

```toml
[sandbox]
podman_connection = "gpu-box"
remote_workspace = "/home/user/project"
```

Credentials stay on your machine; the remote host needs only podman and its
own egress stack (the proxy sidecar and nftables enforcement install on it
automatically). File tools run over the wire (`podman exec cat`/stdin pipes);
note the connection should be one where cgroups work — podman machine
installations should use the root socket connection (the `*-root` entry
podman registers by default). Every podman call is an SSH round trip, so
expect some latency; the `smith serve` daemon milestone addresses that.

## The daemon: `smith serve` / `smith attach`

The harness plane is also a place: `smith serve` runs a small daemon that
hosts agent loops, session logs, approvals and the model keys; the TUI
attaches as a thin client. Daemon and client can share a machine (loopback)
or the daemon can live on a host near the sandbox.

```sh
# Terminal 1: the daemon (loopback, bearer token auto-generated)
smith serve                 # or: smith serve --bind 127.0.0.1:9240

# Terminal 2: thin client — newest session for this workspace, or create one
smith attach

# Attach to a specific hosted session, or a daemon elsewhere
smith attach 73bd12a6
smith attach --url 10.0.0.5:9240 --token <token>
```

What changes in attach mode:

- Model keys live only in the daemon's environment — the prompting device
  never sees them.
- The turn keeps running when you detach; attach again and the full
  transcript replays from the daemon's journal. Two attached clients share
  one session live.
- Approval and egress-clearance modals round-trip over the wire: the first
  client to answer wins, others see "approved elsewhere". Unanswered
  approvals auto-deny after `approval_timeout_secs` (default 300).
- `/model`, `/provider`, `/diff`, `/sessions`, `/resume` talk to the daemon;
  the daemon builds the system prompt and owns the history.

The wire contract (envelope, routes, SSE stream, replay semantics) is the
frozen v1 protocol in [`docs/SERVE_PROTOCOL.md`](docs/SERVE_PROTOCOL.md). A
live end-to-end check against a running daemon:
`cargo run -p smith-cli --example serve_smoke`.

Default posture: loopback bind, bearer token in `~/.smith/serve-token`, no
TLS at this layer (put a reverse proxy in front when the daemon leaves the
box — the same deployment shape as linguine's `/v1`).

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

## Diagnostics

```sh
smith doctor              # fast environment checks (read-only)
smith doctor --deep       # adds live round trips (container, file tools, MCP)
smith doctor --fix        # remediate: start machine, pull/build images
smith doctor --json       # machine-readable output
smith conformance         # the deep suite as a command; exit code reflects failures
smith conformance --network proxy   # exercise the egress stack explicitly
```

Exit code is 0 unless a check fails (warnings do not fail). Deep checks use a
throwaway temp workspace — your project directory is never touched.

## Requirements

- Rust (stable) — `cargo build --release`
- Podman; on Windows the podman machine is started automatically
- Ollama running for local models, or `OPENAI_API_KEY` for OpenAI

## Usage

```sh
# Interactive TUI (container stays up; clean later)
smith

# The harness as a daemon + thin clients (see the daemon section above)
smith serve
smith attach

# One-shot task, auto-approved, gated by the verify command
smith run "explain what this project does"
smith run --workspace C:\path\to\repo --verify "cargo test -q" "fix the failing test"

# Override provider/model for one run
smith run --provider openai "summarise the diff"

# Continue a saved session (recap without a task)
smith resume           # newest session
smith resume abc123 --task "now add tests"

# Housekeeping
smith clean      # remove session containers, the proxy sidecar and its rules
smith sessions   # list session logs in ~/.smith/sessions
```

In the TUI: `/help` lists the slash commands (`/model`, `/provider`, `/diff`,
`/sessions`, `/resume`, `/mcp`, `/quit`).

Config lives at `~/.smith/config.toml` (created on first run with comments):

```toml
provider = "ollama"       # or "openai"
model = "qwen3.8:27b"

[sandbox]
network = "proxy"         # none | proxy | open
egress_enforce = true     # nftables default-deny in the podman machine

[serve]
bind = "127.0.0.1:9240"      # smith serve (token: ~/.smith/serve-token)
approval_timeout_secs = 300  # unanswered approval modals auto-deny
```

## Development

```sh
cargo build
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```
