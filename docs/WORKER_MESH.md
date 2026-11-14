# The worker mesh: `smith-worker`

M14 of the fleet arc (see `docs/FLEET_ROADMAP.md`): the execution plane
becomes its own fleet of worker daemons. Workers **dial the controller
outbound** — no inbound ports on workers, NAT-safe by construction —
heartbeat their capabilities, and lease sessions from the controller's
board. Workers hold repo credentials only; **model keys never leave the
controller**.

## Trust model (the linguine split)

- The **controller mints access keys**: `smith worker issue <name>` prints a
  single-use enrolment token; `smith-worker enrol` exchanges it once for a
  long-lived (revocable) worker credential stored at
  `~/.smith-worker/credential.json`. Tokens are random, stored **hashed**
  (SHA-256) in `~/.smith/workers.json` on the controller. No signing —
  server-side revocation is the property we need.
- The **worker verifies the controller by its TLS certificate** (the reverse
  proxy in front of `smith serve`), which also encrypts the traffic — the
  same deployment shape as linguine's `/v1`.
- Revisit trigger for signed enrolment (PASETO/Ed25519): the controller
  leaving a trusted network boundary.

## Lifecycle

```sh
# On the controller host:
smith serve                     # the harness plane
smith worker issue gpu-box      # → single-use token

# On a worker host (Linux with podman; Windows works for dev):
smith-worker enrol --url controller:9240 --token <one-time-token> --name gpu-box
smith-worker run                # metrics on 127.0.0.1:9241/metrics

# Back on any prompting device:
smith run --serve --worker gpu-box --repo https://... --ref main "fix the flaky test"
smith attach --worker gpu-box --repo https://...   # or interactive
smith worker list / smith worker revoke gpu-box
```

## Mesh

- **Controller → worker**: the worker holds an SSE stream
  (`GET /v1/worker/events`) carrying commands: `start_session`,
  `exec`, `stop_session`, `checkpoint`, and (M15) `merge_gate` +
  `push_branch`. Commands queue (bounded) while the worker reconnects.
- **Worker → controller**: POSTs — `heartbeat` (capability + load + live
  leases, every 10s), `op-result` (correlated by exec id), `exec-events`
  (batched streamed output), `checkpoint` (commit + pushed flag).
- **Worker git auth (M17)**: for controller-hosted repos
  (`WorkspaceSource.token`), the DISPATCH delivers a per-session,
  per-repo scoped token — used in memory per git invocation; no
  long-lived secret on the worker. External forges keep the `token_env`
  pattern.
- **Reconnect**: the worker retries with backoff; its next heartbeat's
  `live_leases` **reconciles** the controller's board (worker state wins).
  A worker that misses three heartbeats goes stale; its leases are lost.

## Leases

One turn at a time per session; one lease per session; sessions **stick**
to their worker. A worker-mode session waits (queued) until a live worker
matches the capability request (image match + name pin + least-loaded).
Lease loss (stale/revoked/disconnect) marks the session `lease_state: lost`
in its status stream — the work is on the checkpoint branch. Orchestration
finish releases leases explicitly (`lease_state: released`).

M15 merge gates run here too: in the session's bare cache, the worker
merges `smith/<child>` into the parent's integration branch
(`smith/integration/<parent>`, `--no-ff` in a temp worktree), runs the
configured gate command in a fresh sandbox on the merged tree, and reports
`{ok, conflict, output}` — a failed gate leaves the integration branch
untouched. A successful gate pushes the integration branch to the origin,
so gates on other workers of the same repo converge. See
`docs/ORCHESTRATION.md`.

## ExecEvent (the M14 freeze)

Streamed exec telemetry, `{lease_id, exec_id, seq, kind}` with
`kind` = `started | stdout_delta | stderr_delta | finished {exit_code,
timed_out, duration_ms}`. Telemetry only — the tool result itself travels
in the correlated `op-result` with the proven buffered timeout/kill
semantics. Shapes are pinned by tests in `smith-protocol`; append to
`docs/SERVE_PROTOCOL.md` when extending.

## Workspaces (disposal-safe)

A worker is cattle. Per session: bare-clone cache per repo URL
(`~/.smith-worker/work/cache-<hash>`) → `git worktree add` (detached) at
`ws-<session>` → bind-mounted as `/workspace`. Git tokens come from the env
var named in the source (`git_token_env`, default `SMITH_GIT_TOKEN`) and
reach git **only via a per-invocation credential-helper env** — never in
process arguments, never on disk. At each turn end the controller sends
`checkpoint`: the worker commits everything on
`smith/<session-id>` and pushes best-effort. Losing a worker loses nothing
but the lease.

## Observability

- Controller: `GET /v1/metrics` (daemon token) — workers by state, sessions
  by executor, tool calls, exec-duration and queue-wait histograms, lease
  counters, clearance/approval decisions.
- Worker: `http://127.0.0.1:9241/metrics` (loopback only).

Prometheus text format, hand-rolled; scraping is pure ops.

## Live check

```sh
cargo run -p smith-cli --example serve_smoke   # daemon-level round trip
# Full mesh: serve + worker + `smith run --serve --worker <name>` as above.
```
