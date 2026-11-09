# The smith serve protocol (v1)

The wire contract between `smith serve` and its thin clients (`smith attach`,
CI issuers). This is the **M13 compatibility freeze point** from
`docs/FLEET_ROADMAP.md`: the message shapes below are pinned by round-trip
tests (`smith-protocol`); any breaking change bumps `v` and this document.

Transport note: v1 rides hand-rolled HTTP/1.1 over TCP (the same
framework-free pattern as the egress sidecar's control API) with SSE for
streaming. The *messages* are the contract; a future WebSocket or mesh
carrier can transport the same envelopes unchanged (that is M14's option).

## Envelope

Every streamed event is wrapped:

```json
{"v":1,"seq":42,"event":{"type":"core","core":{"type":"assistant_delta","text":"hi"}}}
```

- `v` — protocol version (`PROTOCOL_VERSION`), currently `1`.
- `seq` — monotonic per-session sequence assigned by the daemon, starting at
  1. Replay is by `?since=seq`.
- `event` — a tagged [`WireEvent`](#wire-events).

## Wire events

- `core` — a smith-core `CoreEvent` verbatim (assistant deltas, tool starts/
  finishes, `user_message`, `approval_requested`, usage, `compacted`,
  `notice`, `turn_completed`, `turn_cancelled`, `error`). The `CoreEvent`
  shapes are smith-core's serde tags (`type` = `snake_case`).
- `status` — a full `SessionStatus` snapshot: `{id, workspace_id, workspace,
  provider, model, busy, container, mcp_status, created_ms}`. Published when
  the session is created, when a turn starts/ends, and on provider switches.
- `clearance_requested` — `{id, host}`: an egress clearance request from the
  sidecar, polled daemon-side and fanned out to every session.
- `approval_decided` — `{call_id, decision}`: an approval was resolved
  (delivered decisions echo the client's `call_id`; timeout-denies echo an
  empty string). Viewers close their modals on this.

## Auth

Every route requires `Authorization: Bearer <token>`. The token lives in
`~/.smith/serve-token`, generated on first `smith serve`. There are no
public endpoints.

## Routes (all under `/v1`)

RPC-style calls are one request per connection, JSON bodies, and reply with
a `Content-Length` body (`202 {"accepted":true}` for acks, errors as
`{"error": ...}` with the status):

- `GET /v1/health` → `{"ok":true,"version":"…","protocol":1}`
- `GET /v1/sessions` → `[SessionSummary]` (`id, workspace_id, workspace,
  provider, model, busy, created_ms`)
- `POST /v1/sessions` `{workspace, provider?, model?}` → `{"session_id": …}`.
  Long call: the sandbox (and sidecar) start here. Workspace paths are
  **daemon-side**.
- `POST /v1/sessions/:id/turns` `{input}` → `202`; `409` when the session is
  busy (one turn per session at a time).
- `POST /v1/sessions/:id/cancel` → cancels the in-flight turn (its agent
  emits `turn_cancelled` through the stream).
- `POST /v1/sessions/:id/approvals` `{decision: allow_once|allow_always|deny,
  call_id?}` → delivers the decision to the pending approval. Approvals are
  **per-session, single-pending** (one turn at a time implies one open
  approval); `call_id` is the client's correlation id and is echoed in
  `approval_decided`. First decision wins; `409` when nothing is pending.
  Unanswered approvals auto-**deny** after `[serve].approval_timeout_secs`
  (default 300), echoing `{"call_id":"","decision":"deny"}`.
- `POST /v1/sessions/:id/provider` `{provider?, model?}` → daemon-side
  provider switch (the daemon holds the keys; nothing key-shaped exists on
  the prompting device).
- `POST /v1/sessions/:id/clearance` `{id, decision}` → forwarded by the
  daemon to the egress sidecar's control API (`allow`/`always`/`deny`).
- `POST /v1/sessions/:id/resume` `{prefix}` → loads a saved session log from
  the daemon host's `~/.smith/sessions` into the hosted history (must match
  the session's `workspace_id`).
- `GET /v1/sessions/:id/diff` → `{"text": …}` — transcript-formatted `git diff`.
- `GET /v1/sessions/:id/status` → the `SessionStatus` JSON.

## Event stream

`GET /v1/sessions/:id/events?since=seq` → `200` with
`Content-Type: text/event-stream`, chunked transfer encoding. Each SSE event
is a single `data:` line carrying one envelope:

```
data: {"v":1,"seq":7,"event":{"type":"core","core":{"type":"turn_completed"}}}
```

Semantics:

1. The daemon subscribes the connection to the live feed **before**
   snapshotting the replay, then sends every journalled envelope with
   `seq > since`. Clients deduplicate by `seq` (an event may appear in both
   the replay and the live feed).
2. The journal is a bounded ring (10 000 envelopes). A lagged live
   subscriber is refilled from the ring by the daemon.
3. The stream ends when the daemon stops or the client disconnects; clients
   reconnect with the last seen `seq` and the replay covers the gap.

## Session and daemon lifecycle

- Sessions are in-memory in the daemon; the JSONL logs they write persist.
  A daemon restart loses hosted sessions (log-based `smith resume` still
  works); session containers are left running on daemon shutdown and reaped
  by `smith clean`.
- Bind address: `[serve].bind` in `~/.smith/config.toml`
  (default `127.0.0.1:9240`). No TLS at this layer — loopback today; put a
  TLS reverse proxy in front when the daemon leaves the box (the same
  deployment shape as linguine's `/v1`).

## Test anchors

`smith-protocol` pins the freeze: `envelope_json_is_frozen` (exact envelope
JSON), `core_event_variants_round_trip`, `wire_event_variants_round_trip`,
`request_bodies_round_trip`. `smith-server`'s integration tests drive the
routes end-to-end over loopback.
