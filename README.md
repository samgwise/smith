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
| `smith-protocol` | The frozen wire contract: envelopes, SSE framing, worker ops, raw client |
| `smith-server` | The `smith serve` daemon: hosted sessions, approval routing, fleet scheduler |
| `smith-worker` | The `smith-worker` binary: outbound mesh, leased sandboxes, git workspaces |
| `smith-tui` | Ratatui interface: transcript, tool activity, approval modal, slash commands |
| `smith-cli` | `smith` binary: TUI, headless runs, serve/attach, worker admin, housekeeping |

## Security model (v1)

1. Provider keys stay host-side and are never injected into the container.
2. Shell commands run only via `podman exec` inside the session container
   (non-root user, memory/CPU/pids limits, wall-clock timeout).
3. File tools are locked to the canonicalised workspace directory — any
   symlinked path component is refused, so a link can't fling a write
   outside the workspace.
4. Smith refuses to mount your home directory, any ancestor of it (so
   `C:\Users` and `/home` are out), and sensitive paths (`~/.ssh`, `~/.aws`, …).
5. Every tool call is logged to the session JSONL file.

In the TUI, shell commands ask first: `y` once, `a` for the session,
`n` to refuse. Risky commands (`git push`, `git reset`, `git clean`, `rm`,
`curl`, `wget`, `sudo`, `dd`, `chmod 777`) always re-prompt — "a" binds to
the exact command, never the prefix, and compound commands (`&&`, `||`, `;`,
`|`, `$()`, backticks) are checked segment by segment: approving one half of
a compound never covers the other. Writes into the workspace are approval-free
except to files whose contents execute on the host (`.git/hooks/*`,
`.git/config`, `.gitattributes`, `.envrc`) — those always prompt. Headless
`smith run` auto-approves (the sandbox is the boundary), and repeated denials
escalate: after three, the agent is told to stop retrying and wait.

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
nftables default-deny ruleset is installed inside the podman machine, scoped
to the egress bridge only (the machine's own networking is untouched). Two
chains apply. The **forward** chain filters container→internet traffic: only
the proxy's pinned IP may leave the bridge, so a program that ignores the
proxy env and dials IP literals is dropped at the packet level. The
**input** chain filters container→machine traffic: only DNS (udp/tcp 53)
from the bridge is accepted into the machine; every other gateway port is
dropped. The one residual channel is DNS through the gateway, which can
carry a little data (by design; see `docs/SECURITY_REVIEW.md` S-08) — the
rules cover the common case, they are not a hermetic seal. Installation is
fail-closed: if the ruleset cannot be installed, the session refuses to
start (escape hatch: `egress_enforce = false`). Machine reboots clear the
rules; the next session reinstalls them. `smith clean` removes the sidecar
and the table.

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
# ...or keep the token off the process argument list:
smith attach --url 10.0.0.5:9240 --token-file ~/.smith/serve-token

# Remote daemon over TLS: pin the daemon's certificate (out-of-band)
smith attach --url https://smith.example.com:9240 --cert smith.pem
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

Default posture: loopback bind, bearer token in `~/.smith/serve-token`.
For a remote daemon, serve TLS and pin the certificate on the clients:

```toml
[serve]
tls_cert = "/etc/smith/serve.pem"   # with tls_key; unlocks remote binds
allow_plaintext_remote = false       # explicit opt-in to remote plaintext
max_git_body_mb = 256                # the forge's pack-upload cap
forge_token_ttl_secs = 604800        # minted forge tokens expire (a week)
```

Copy `serve.pem` to clients and pass `--cert` (or `SMITH_SERVE_CERT`);
the pinned leaf is the client's sole trust anchor.

## The fleet: `smith-worker`

Execution can also live on its own machines. Workers enrol with a one-time
token, dial the controller **outbound** (no inbound ports, NAT-safe),
heartbeat capability, and lease sessions from the board. Workspaces
materialise from git (bare-clone cache + worktree per session); every turn
ends with a checkpoint commit pushed to `smith/<session>` — losing a worker
loses nothing but the lease.

```sh
# Controller:
smith serve
smith worker issue gpu-box        # → one-time enrolment token

# On a worker host (Linux with podman; Windows works for dev):
smith-worker enrol --url https://controller:9240 --token <token> --name gpu-box \
  --cert smith.pem                # pin the controller's certificate
smith-worker run                  # metrics on 127.0.0.1:9241/metrics

# Anywhere:
smith run --serve --worker gpu-box --repo https://... "fix the flaky test"
smith attach --worker gpu-box --repo https://...   # interactive
smith worker list                 # or: smith worker revoke gpu-box
```

See [`docs/WORKER_MESH.md`](docs/WORKER_MESH.md) for the trust model (the
controller mints keys; workers verify the controller by its TLS cert), the
lease semantics, and the streamed-exec wire format.

## Orchestration: one prompt in, many agents out

With `--orchestrate`, a session becomes a **planner**: it decomposes work
into subtasks, each executed by its own agent on its own worker lease and
git worktree. A tool-less **judge** reviews every sub-agent's report before
the planner sees it (bounded rework rounds), and worker-side **merge gates**
converge the checkpoint branches onto a reviewable integration branch
(`smith/integration/<session>`) — conflicts come back to the planner for
rework. What gets pushed without asking: each passing gate pushes the
integration branch itself to the origin (that's how the convergence is
reviewable); `auto_push = true` additionally lets it update your base refs
(e.g. `main`), which is the only thing that's opt-in.

```sh
smith serve                                   # the controller
smith attach --orchestrate --repo https://... # a planner session (interactive)
smith run --serve --orchestrate --repo https://... "refactor config + docs + tests"
```

```toml
[orchestrate]           # ~/.smith/config.toml
max_children = 8        # spawn caps are config, not model judgement
judge_rounds = 2        # 0 disables judging (plain parallel spawning)
gate_command = "cargo test -q"   # empty = merge-only
auto_push = false       # humans review the integration branch by default
```

The controller also **hosts git itself** (`smith:` repo references in
spawns; per-session scoped tokens delivered automatically — no forge to
stand up, no long-lived credentials on workers). Minted tokens **expire**
(`[serve].forge_token_ttl_secs`, default one week) and are **revoked when a
session's lease is released** — the orchestration finish invalidates the
child's token and removes its `.gitcred`. See
[`docs/CLOUD_DEPLOYMENT.md`](docs/CLOUD_DEPLOYMENT.md) and
[`docs/SERVE_PROTOCOL.md`](docs/SERVE_PROTOCOL.md) (`/git/...`).

See [`docs/ORCHESTRATION.md`](docs/ORCHESTRATION.md) for the model, the
limits, and a worked example.

## MCP servers

smith connects to stdio MCP servers and exposes their tools to the agent as
`mcp_<server>_<tool>`. Global servers live in `~/.smith/config.toml`:

```toml
[mcp.servers.github]
command = "npx"                              # resolved via PATH (PATHEXT-aware
                                             # on Windows, so npx.cmd works)
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "${GITHUB_TOKEN}" }   # expanded from your environment
approval = "ask"                             # or "allow" to pre-approve all
                                             # tools from this server
timeout_secs = 60                            # per tool call
connect_timeout_secs = 30                    # per connect step (spawn/handshake/list)
```

Project-scoped servers go in `.smith/mcp.toml` at the workspace root (same
shape, `[servers.<name>]`, wins over user config on name collisions).

**Workspace trust**: a project's `.smith/mcp.toml` runs HOST commands, so it
only loads when the workspace is trusted. The TUI asks once at session start
(showing the server names and commands) and remembers the answer;
`smith run --trust-workspace` grants headlessly; `smith trust <workspace>`
records it for daemon-hosted sessions (`smith serve` never prompts — it
consults `~/.smith/trust.json` only; `smith trust --revoke` withdraws).
Even for a trusted workspace, the project layer's `approval` and `env` are
ignored — a repo cannot pre-approve its own tools or read your environment
variables; only its server commands and args are honoured.

Security notes: servers run **host-side** with only the explicitly configured
environment (plus PATH) — never your full environment — so the sandbox is not
a boundary for them; that is why `approval = "ask"` is the default and routes
every call through the same modal as shell commands. Servers are connected at
session start and their tool list is fixed for the session (prompt-cache
friendly); a server that fails to start (including a hung handshake, which
fails after `connect_timeout_secs`) is reported by `/mcp` and skipped.

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
throwaway temp workspace — your project directory is never touched — and the
MCP handshake check only starts servers from your own `~/.smith/config.toml`:
it never runs a project's `.smith/mcp.toml`.

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

# The fleet: lease a worker, materialise from git (see the fleet section)
smith run --serve --worker gpu-box --repo https://... "task"

# Multi-agent orchestration (see the orchestration section)
smith run --serve --orchestrate --repo https://... "refactor these three modules"

# One-shot task, auto-approved, gated by the verify command
smith run "explain what this project does"
smith run --workspace C:\path\to\repo --verify "cargo test -q" "fix the failing test"

# Submit to a daemon with the token from a file (off the argv):
smith run --serve --token-file ~/.smith/serve-token "task"

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
forge_token_ttl_secs = 604800  # minted forge tokens expire (0 = never)
```

## Development

```sh
cargo build
cargo test                                   # the default suite
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check

# The container/egress tests need a real podman server (and nftables):
cargo test -p smith-sandbox --lib -- --test-threads=1 --ignored
```

CI (`.github/workflows/rust.yml`) runs lint (fmt + clippy) and the test
suite on ubuntu and windows, plus a `container` job that runs the
podman/nftables suite against a native rootless podman server on every push.
