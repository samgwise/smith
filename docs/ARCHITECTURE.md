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

## History

The build proceeded in milestones (see `git log`): v1 vertical slice
(sandbox + loop + TUI), M5 resumable sessions + compaction + slash commands,
M6 project instructions + cache-stable prompting + paged reads + verify gate +
exact-action approvals, M7 MCP client, M8 egress proxy, M9 nftables
enforcement. The roadmap beyond: per-destination live allow/deny prompts, a
replay/eval harness over session logs, streamable-HTTP MCP transport +
OAuth, native-Linux enforcement, and remote executors/fleets.
