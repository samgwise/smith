# Smith architecture

Smith is a coding agent with one organising rule: **arbitrary code runs in the
sandbox, everything else runs on the host**. This document explains why the
system is shaped the way it is; feature-level usage lives in the README.

## The trust model

The host process (the controller) owns:

- the TUI and headless front-ends,
- the agent loop,
- the LLM client and every credential (provider keys never leave the host),
- the session log,
- the podman CLI calls that manage containers.

The container owns:

- the workspace files (bind-mounted at `/workspace`),
- arbitrary code execution via `podman exec` — always as a non-root `dev`
  user, always resource-limited, always under a wall-clock timeout.

Consequences that fall out of this split:

- Ollama and OpenAI are interchangeable back-ends with no security trade-off:
  the container never needs LLM endpoints, so no network holes for them.
- File tools can run host-side (fast, simple) while shell runs container-side,
  because file IO is not arbitrary code execution — but both cross the same
  seam (below), so a remote executor can replace either.
- MCP servers run host-side, outside the container boundary; that is why MCP
  tool calls default to ask-first approval.

## The three seams

Everything hangs off traits in `smith-core`, which knows nothing about the
terminal, podman, or the SDKs:

1. **`LlmProvider`** — streaming chat with tool calls. Implemented by
   `smith-providers` (Ollama native `/api/chat`, OpenAI chat completions).
   Held behind `SharedProvider`, a swappable handle so `/model` and
   `/provider` switches are visible to the agent loop *and* the compactor.
2. **`ToolExecutor`** — the backend that tools run against: shell-in-container
   plus workspace-locked file IO today; a remote executor (SSH or daemon)
   later. Remote-readiness is the reason *all* tool calls, including file IO,
   cross this one seam and core never touches `std::fs` directly.
3. **`Tool`** — schema + behaviour + approval flag. The builtin four
   (`shell`, `read_file`, `write_file`, `list_dir`) and MCP bridges implement
   the same trait, so the agent loop, approval gate and tool pane treat them
   identically.

Two supporting traits complete the picture: **`ApprovalGate`** (where `y`/`a`/`n`
decisions come from — the TUI modal, or auto-allow in headless runs) and
**`Compactor`** (context summarisation over the budget).

The front-ends are consumers, not participants: the core is headless and
event-driven (`CoreEvent` streams over channels; the TUI is one subscriber),
and the approval modal is just a gate implementation. This is also what keeps
the loop testable — every unit test drives the real loop with a scripted
provider, a recording executor and a scripted gate.

## Sandbox and egress

`smith-sandbox` owns the podman lifecycle: machine ensure-up, one container
per session (pinned `ubuntu:24.04`, non-root, memory/CPU/pids limits, boot
script creating the `dev` user), exec with an in-container `timeout` plus a
host-side wall-clock guard, and reaping on `smith clean`.

Network modes:

- `proxy` (default): a filtering proxy sidecar (`smith-egress`) on a dedicated
  network. HTTPS is filtered from the `CONNECT` hostname — no TLS
  interception. The allowlist is **enforced** by an nftables default-deny
  forward chain installed inside the podman machine, scoped to the egress
  bridge only; only the proxy's pinned IP and the gateway DNS may leave the
  bridge. Installation is fail-closed (a failed install refuses to start the
  session) and verified post-install (an empty install must not count as
  success).
- `none`: `--network=none`. `open`: unrestricted (compatibility posture).

Design notes worth keeping: the egress proxy is a sidecar *container*, not a
host process, to stay inside the machine (no host ports, no firewall prompts)
and avoid Linux cross-compilation on the host. The `smith-egress` crate uses
a standalone manifest so the Containerfile build needs nothing from the
workspace. Machine discovery uses `LastUp`/`Currently running` (podman 5.x
list reporters have no State field), and remote commands are passed as one
quoted argument because `podman machine ssh` space-joins its arguments.

## Sessions, compaction, prompting

A session is an append-only JSONL log under `~/.smith/sessions/` recording
user messages, assistant messages (with tool calls) and tool results. Resume
rebuilds the conversation from it, appends in place, and checks a logical
workspace identity (a hash of the canonical path — no absolute host paths in
session state, so logs stay portable).

Context management: the agent loop compacts when history exceeds the budget —
older turns are summarised by the current provider at a user-message boundary
(never splitting an assistant/tool-result pair), with truncation as the
fallback. The system prompt (including `AGENTS.md`) and the tool-schema array
are byte-stable across calls on purpose: provider prompt caches only hit on
exact prefixes.

## MCP

`smith-mcp` bridges stdio MCP servers (via the official `rmcp` SDK) into the
`Tool` registry as `mcp_<server>_<tool>`. Servers are configured in layers
(user config + workspace `.smith/mcp.toml`, workspace wins), spawned with only
their explicitly configured environment (`${VAR}`-expanded; unset references
drop the entry), and their tool list is fetched once per session to keep the
request shape cache-stable. A failed server is reported and skipped.

## The harness plane: `smith serve`

M13 turned the harness into a place: `smith-server` hosts agent loops,
session logs, approval routing and the model keys in a daemon; thin clients
attach over the wire. The pieces:

- **`smith-protocol`** is the frozen wire contract (`docs/SERVE_PROTOCOL.md`):
  a versioned, sequenced envelope wrapping smith-core's `CoreEvent` plus
  protocol-level events (`status`, `clearance_requested`,
  `approval_decided`), carried by hand-rolled HTTP/1.1 with SSE streaming.
  No web framework — the egress sidecar's control API is the house pattern,
  and the message types (not the transport) are the compatibility surface.
- **`smith-server`** assembles sessions exactly like `smith run` does
  (`ConfigFactory`) behind an injectable `SessionFactory` seam, keeps a
  journal per session (sequence numbers, bounded replay ring, broadcast
  fan-out), and routes approvals through a first-decision-wins slot that
  auto-denies on timeout. Egress clearance polling moved here too: the
  sidecar is a machine-level concern, not a TUI concern. The egress client
  is blocking std I/O (it also builds standalone in the container), so it
  runs on the blocking pool.
- **`smith-tui`'s `SessionLink` seam** is what made this a small diff: the
  UI never touches the agent, history or gates — it sees `LinkEvent`s and
  calls the link. `LocalLink` preserves the colocalised behaviour exactly
  (in-process turn task, gate bridge, local clearance poller);
  `RemoteLink` is the thin client (SSE reader with reconnect + replay,
  RPCs for prompts/decisions). One UI, two topologies — the colocation
  decision stays open by construction.

Security posture of the daemon: loopback bind by default, bearer token in
`~/.smith/serve-token`, request size caps and read timeouts on the hand-
rolled parser, and — the point of the exercise — model keys exist only in
the daemon's environment, so the prompting device holds no secrets.

## The execution plane: `smith-worker`

M14 gave execution its own daemons. A worker enrols with a single-use token
(the controller mints keys; workers verify the controller by its TLS cert —
the linguine split), dials the controller **outbound** (an SSE command
stream plus POSTs back — no inbound ports, NAT-safe), heartbeats capability,
and leases sessions from the board. Ops cross the mesh as `ExecOp`s
mirroring the `ToolExecutor` trait, so the worker just implements it locally
and the controller wraps the mesh in the same trait — the agent loop is
none the wiser. Exec output streams as frozen `ExecEvent` telemetry while
the result keeps the buffered timeout/kill semantics. Workspaces materialise
from git (bare-clone cache + worktree per session, tokens via per-invocation
credential-helper env); each turn ends with a checkpoint commit pushed to
`smith/<session>` — losing a worker loses nothing but the lease. Linux
workers enforce nftables natively (a third `root_shell` transport, sudo);
the scheduler is capability-aware, least-loaded, and queues sessions when
no worker fits. Metrics (Prometheus text, hand-rolled) at `/v1/metrics`.

## History

The build proceeded in milestones (see `git log`): v1 vertical slice
(sandbox + loop + TUI), M5 resumable sessions + compaction + slash commands,
M6 project instructions + cache-stable prompting + paged reads + verify gate +
exact-action approvals, M7 MCP client, M8 egress proxy, M9 nftables
enforcement, M10 doctor + conformance, M11 per-destination clearance,
M12 remote executor (podman-remote), M13 `smith serve` daemon + thin-client
TUI (wire contract frozen in `docs/SERVE_PROTOCOL.md`). The fleet arc
continues in `docs/FLEET_ROADMAP.md`: worker daemon + scheduler (M14),
multi-agent orchestration (M15).
