# Security review tracker

Reviewed on 14 November 2026 against `main`. This was a static review, so nothing has been executed. Items marked **to verify** need a reproduction or test before they count as confirmed.

How to use this file:
- Tick `[x]` when the fix is merged and a test covers it. Add the PR or commit after the item.
- Keep IDs stable. Add new items at the end of their section rather than renumbering.
- Each item ends with its migration phase (see the migration plan) or "Independent" when it's fixed separately.

Status: 0 of 44 closed.

## High

- [ ] **S-01 · Workspace MCP config runs host commands before approval** (Independent)
  - Where: `crates/smith-server/src/factory.rs:52-60`, `crates/smith-mcp/src/config.rs:57-68`, `crates/smith-mcp/src/manager.rs:123-160`, `crates/smith-mcp/src/bridge.rs:46-49`, `crates/smith-cli/src/doctor.rs:286-321`
  - Problem: a repository's `.smith/mcp.toml` spawns its commands at session start, can pass host variables through `${VAR}`, and can set `approval = "allow"`. `smith doctor --deep` also calls a workspace tool with `{}` arguments, bypassing approval.
  - Fix: require per-workspace trust before loading the workspace layer. Ignore `approval` and `env` from it. Doctor never spawns or calls workspace MCP servers.
  - Verify: a scratch repo whose `.smith/mcp.toml` writes a marker file. No marker may appear before trust is granted.

- [ ] **S-02 · Git token sent to any host** (Independent)
  - Where: `crates/smith-server/src/controller.rs:582-587` (attaches `token_env` to every request-supplied repo), `crates/smith-worker/src/materialise.rs:46-59` (credential helper answers for any host), `crates/smith-server/src/orchestrator.rs:1108-1115` (planner-chosen `repo` and `ref`)
  - Problem: the worker's `SMITH_GIT_TOKEN` is returned to any host that issues a 401. A prompt-injected planner, or any API client, can name an attacker's URL.
  - Fix: scope the helper to the configured origin with git's `credential.<URL>.helper`. Validate `repo` and `ref`, and put `--` before user-supplied URLs and refs.
  - Verify: a local server that answers every request with 401 must receive no token during a clone.

- [ ] **S-03 · Request bodies buffered before auth, and unbounded framing reads** (Phases 1, 2, 4)
  - Where: `crates/smith-server/src/http.rs:70` (body read before auth at `:78`), `crates/smith-protocol/src/http.rs:236-268` (line reads grow without a bound before the cap check), `:214-219` (trailer lines uncounted), `:221-225` (unchecked `body.len() + size`), `:19` (256 MiB git cap, applied pre-auth)
  - Problem: an unauthenticated client can make the daemon buffer up to 256 MiB per connection, or grow memory without limit through chunk-size lines and trailers. The size arithmetic can wrap in release builds.
  - Fix: authenticate in middleware before any body is read, and let hyper handle framing with its own limits.
  - Verify: `red_s03_unauthenticated_body_is_not_read` in `crates/smith-server/tests/serve_integration.rs` (added 15 November 2026; fails today as expected — no response within 3 s while the server waits for the declared body).

- [ ] **S-04 · No connection cap and no request deadline** (Phase 1)
  - Where: `crates/smith-server/src/http.rs:38-53` (one task per connection, no limit), `crates/smith-protocol/src/http.rs:22` (timeouts apply per read, not per request)
  - Problem: any number of connections can be opened, and each can stall by sending bytes just often enough to beat the per-read timeout.
  - Fix: a connection semaphore, hyper's header-read timeout, and a per-frame upload timeout (tower-http `RequestBodyTimeoutLayer`). Phase 0 finding: hyper's header timeout also closes idle keep-alive connections, so no separate idle knob is needed.
  - Verify: `red_s04_idle_connection_is_bounded_and_cap_refuses_extras` in `crates/smith-server/tests/serve_integration.rs` (added 15 November 2026; fails today as expected — an idle connection was still open after 15 s).

- [ ] **S-05 · Plaintext transport; TLS verification documented but not built** (Phases 1, 5)
  - Where: `crates/smith-protocol/src/client.rs:74-82` (raw TCP), `crates/smith-worker/src/main.rs:55-65` (strips `http://`, no TLS), `docs/WORKER_MESH.md:18-20` and `crates/smith-server/src/workers.rs:1-7` (say the worker verifies the controller's certificate), `crates/smith-server/src/config.rs:150-152` (any bind address accepted)
  - Problem: enrolment tokens, worker credentials, per-session forge tokens, and command streams cross the network in cleartext. The documented TLS-proxy deployment can't work for workers, because the worker client can't speak TLS.
  - Fix: TLS in the controller, certificate pinning in the worker and clients (decision D-2 in the plan), and refusal of non-loopback binds unless TLS is configured or plaintext is explicitly allowed.
  - Verify: a worker refuses a controller whose certificate doesn't match the pin.

- [ ] **S-06 · Agent can write files that run on the host, without approval** (Independent)
  - Where: `crates/smith-core/src/tools.rs:98-100, 154-156, 188-190` (read, write, and list need no approval), `crates/smith-sandbox/src/executor.rs:157-167`
  - Problem: the workspace is mounted read-write, so the agent can write `.git/config` (for example `core.fsmonitor`), `.git/hooks/*`, `.envrc`, or package scripts. These run on the host when the user next uses them, which contradicts the README's "arbitrary code only runs in the container".
  - Fix: require approval for writes to tool-executed config paths, or mount `.git` read-only inside the container.
  - Verify: a `write_file` to `.git/hooks/post-commit` is refused or prompts for approval.

- [ ] **S-07 · Symlink escape on write, for local and worker file operations** (Independent)
  - Where: `crates/smith-sandbox/src/executor.rs:55-96` (`resolve` only canonicalises existing paths), `:157-167` (the write follows the link), `crates/smith-worker/src/main.rs:769-773` (worker writes use the same code)
  - Problem: for a dangling symlink, `exists()` is false, so `resolve` accepts the link path and `std::fs::write` follows it outside the workspace, with no approval. This applies to Linux and macOS hosts, where symlinks are supported.
  - Fix: confine file operations with `cap-std` directory handles, or reject links using `symlink_metadata` and create files with `create_new`.
  - Verify: a workspace symlink to a missing file outside the workspace. `write_file` must refuse.

## Medium

- [ ] **S-08 · Egress rules only filter forwarded traffic** (Independent)
  - Where: `crates/smith-sandbox/src/enforce.rs:24-35`
  - Problem: the nftables ruleset hooks only `forward`. Traffic to the bridge gateway is locally destined, so it's unfiltered on every port, and DNS through the gateway can carry data out.
  - Fix: add an `input` chain that allows only DNS from the bridge. Document the residual DNS channel.
  - Verify: from a container, a connection to the gateway on a non-DNS port fails.

- [ ] **S-09 · Clearance control port mismatch** (Independent, to verify)
  - Where: `crates/smith-sandbox/src/lib.rs:498-499` (publishes `127.0.0.1:3128` to container port 9239), `crates/smith-egress/src/client.rs:12` (host client uses 9239), used at `crates/smith-server/src/controller.rs:335` and `crates/smith-server/src/factory.rs:96`
  - Problem: host clearance calls target `127.0.0.1:9239`, which isn't published. Interactive clearance would time out to deny. That fails closed, but the feature doesn't work.
  - Fix: publish `127.0.0.1:9239:9239`, and define the port in one place.
  - Verify: `podman port smith-egress-proxy`, then `Test-NetConnection 127.0.0.1 -Port 9239`.

- [ ] **S-10 · Root command injection in remote file reads** (Independent)
  - Where: `crates/smith-sandbox/src/executor.rs:231-254` (`stat` runs as root and `cat` as `dev`; the path is interpolated inside single quotes), `:204-221` (rejects only `..`)
  - Problem: a path containing `'` breaks out of the quoting and runs commands in the container as root. The model controls the path.
  - Fix: use the `shell_quote` helper the write path already uses, or pass argv directly.
  - Verify: `read_file` with a path containing `'; id; '` must not run `id`.

- [ ] **S-11 · Approval checks can be bypassed with compound commands and prefixes** (Independent)
  - Where: `crates/smith-core/src/agent.rs:143-174` (risky list matched against the start of the whole string), `:531-567` (non-risky "always" stores the first word and matches with `starts_with`)
  - Problem: approving `git status` also approves `git clean -fdx`. Approving `cd src && make` also approves `cd x && rm -rf …`. The README says risky commands always re-prompt and that "always" binds to the exact command.
  - Fix: split on `&&`, `||`, `;`, `|` and `$()`. Check each segment's argv[0] and subcommand, and store exact commands for "always".
  - Verify: unit tests for each example above.

- [ ] **S-12 · Approval modal can hide the command; control characters reach the terminal** (Independent)
  - Where: `crates/smith-tui/src/ui.rs:176-198` (fixed 7-row modal, wrapping, no scrolling), `:72-79` (transcript lines aren't sanitised)
  - Problem: a command over about 58 characters hides the `[y]/[a]/[n]` row, and text beyond about three wrapped lines is cut off. Escape sequences in model or tool output can drive the terminal.
  - Fix: size or scroll the modal and show the full command. Strip or escape control characters.
  - Verify: a 300-character command shows its tail and the option row.

- [ ] **S-13 · Command output is buffered without limit** (Independent; prerequisite for C-01)
  - Where: `crates/smith-sandbox/src/lib.rs:628-640` (`wait_with_output`), `:1072-1090` (`read_piped`)
  - Problem: a noisy command can exhaust host or worker memory within its timeout. The 12 KB clamp happens only later, in the core.
  - Fix: cap each stream in the reader, keep the tail, and add a notice. Use the same caps for worker results (decision D-6 in the plan).
  - Verify: `yes | head -c 200000000` in the sandbox keeps memory bounded.

- [ ] **S-14 · Forge tokens are repo-wide, never expire, and can't be revoked** (Phase 4 for ref policy; Independent for lifetime)
  - Where: `crates/smith-server/src/forge.rs:63-86` (mint), `:135-141` (authenticate, no expiry), `:151-171` (`ensure_repo` leaves `receive.denyDeletes` and `receive.denyNonFastForwards` unset), `crates/smith-server/src/http.rs:446-464` (scope checks the repo only)
  - Problem: any session's token can force-push or delete any ref in its repo, including `main` and other sessions' checkpoints. Tokens stay valid until the `.gitcred` file is deleted.
  - Fix: enforce a ref-prefix policy through an `update` hook that allows only `smith/<session>`, set both receive policies, add a token TTL, and revoke at session end.
  - Verify: a session token can't update `refs/heads/main`, and an expired token is rejected.

- [ ] **S-15 · Secrets and state written with default permissions** (Independent; Phase 1 applies to new TLS key files)
  - Where: `crates/smith-server/src/lib.rs:34-45` (serve-token), `crates/smith-sandbox/src/lib.rs:1095-1110` (egress-token, also racy on first run), `crates/smith-server/src/workers.rs:107-114` (workers.json), `crates/smith-worker/src/main.rs:137-144` (worker credential), `crates/smith-server/src/forge.rs:78-80` (`.gitcred`)
  - Problem: nothing in the repo sets permissions, so on Linux and macOS these files are readable under a default umask. Writes aren't atomic, so a crash can corrupt the registry.
  - Fix: create files with owner-only permissions and write them through `tempfile`'s `persist`. On Windows, confirm the resulting ACLs.
  - Verify: `stat` on Linux shows mode 0600 for each file.

- [ ] **S-16 · `smith clean` removes unrelated containers** (Independent)
  - Where: `crates/smith-sandbox/src/lib.rs:1018-1039` (`clean_all`), `:207-229` (`reap_stopped_sessions`)
  - Problem: the `name=smith-` filter is an unanchored regex, so any container with "smith-" anywhere in its name is force-removed.
  - Fix: anchor the name (`^smith-`) and filter on a label that smith sets.
  - Verify: a container named `legacy-smith-dev` survives `smith clean`.

- [ ] **S-17 · Workspace denylist is name-based** (Independent)
  - Where: `crates/smith-sandbox/src/lib.rs:1050-1067`
  - Problem: only the exact home directory and paths with components named `.ssh`, `.aws` and similar are refused. Mounting `C:\Users` or `C:\` is allowed, which exposes `samgw\.ssh` inside the container.
  - Fix: restrict workspaces to configured roots, and refuse any ancestor of the home directory.
  - Verify: `C:\Users` is refused as a workspace.

- [ ] **C-01 · Worker results above 1 MiB are silently dropped** (Phase 2)
  - Where: `crates/smith-protocol/src/http.rs:17` (1 MiB default), `crates/smith-server/src/http.rs:65-69` (applies it to `/v1/worker/op-result`), `crates/smith-worker/src/main.rs:627-637` (failed posts are ignored), `crates/smith-sandbox/src/lib.rs:628-640` (no output cap)
  - Problem: a command or `read_file` result larger than 1 MiB is rejected with the connection dropped, so the controller waits for the 300 s operation timeout.
  - Fix: cap results on the worker (S-13, decision D-6) and set the op-result limit above that cap. Log failed posts.
  - Verify: `red_c01_two_mib_op_result_is_accepted` in `crates/smith-server/tests/serve_integration.rs` (added 15 November 2026; fails today as expected — the connection is force-closed with `os error 10054`).

- [ ] **C-02 · Git clients' gzip-compressed requests fail** (Phase 4; reproduced)
  - Where: `crates/smith-protocol/src/http.rs:155-186` (only `Transfer-Encoding` and `Content-Length` are read), `crates/smith-server/src/http.rs:627` (raw body passed to `upload-pack`)
  - Problem: git gzips RPC request bodies above 1024 bytes (`remote-curl.c:1008`), and `http-backend` inflates them (`http-backend.c:477-485`). Clones or fetches with many refs fail. REPRODUCED 15 November 2026 by `red_c02_gzipped_upload_pack_request_is_decoded` in `crates/smith-server/tests/serve_integration.rs`: a gzipped upload-pack request returns 500 with `bad line length character`.
  - Fix: handled by the migration (decision D-4 in the plan).
  - Verify: the red test goes green; `git clone` of a hosted repo with 30 branches succeeds.

## Low

- [ ] **S-18 · Non-constant-time token comparisons** (Phase 1)
  - Where: `crates/smith-server/src/http.rs:78` and `:453`, `crates/smith-egress/src/bin/smith-egress-proxy.rs:217-222`
  - Fix: compare hashes with `subtle::ConstantTimeEq`.
  - Verify: no `==` comparisons on secret values remain.

- [ ] **S-19 · Fleet registry resets silently on a corrupt file; enrolment tokens never expire** (Independent)
  - Where: `crates/smith-server/src/workers.rs:96-105` (`unwrap_or_default`), `:117-128` (no expiry)
  - Fix: fail loudly on a parse error and keep the bad file aside. Add expiry to enrolment tokens.
  - Verify: a corrupt registry stops startup with an error, and the file is kept.

- [ ] **S-20 · Clearance poller reads the egress token once and never prunes** (Independent)
  - Where: `crates/smith-server/src/controller.rs:336` (token read at construction), `:1684-1689` (poller exits if the token is missing), `:1690-1703` (`seen` grows forever)
  - Problem: on a fresh install, clearance prompts never reach the daemon.
  - Fix: re-read the token on each poll, and prune `seen`.
  - Verify: clearance requests appear after the egress token is created post-start.

- [ ] **S-21 · No timeouts on MCP startup or provider calls** (Independent)
  - Where: `crates/smith-mcp/src/manager.rs:149-160` (spawn, handshake, tool listing), `crates/smith-providers/src/*.rs` (no HTTP timeouts)
  - Problem: a hung server or model call holds the session busy indefinitely.
  - Fix: timeouts on each step, plus a turn-level timeout.
  - Verify: a hung MCP server fails session start after the timeout.

- [ ] **S-22 · Unbounded buffers** (Independent; `ChunkedReader` is removed in Phases 5 and 6)
  - Where: `crates/smith-providers/src/stream.rs:8-37`, `crates/smith-protocol/src/http.rs:439-513`, `crates/smith-core/src/instructions.rs:11-21`
  - Problem: a line without a newline, or a very large `AGENTS.md`, grows memory or the prompt without limit.
  - Fix: caps on line length and instruction size, with a notice when truncated.
  - Verify: a 10 MB line without a newline is rejected.

- [ ] **S-23 · Proxy and clearance edge cases** (Independent)
  - Where: `crates/smith-egress/src/bin/smith-egress-proxy.rs:144-147` (`rsplit_once(':')` on the full URI sends `http://host:8080/path` to port 80), `crates/smith-egress/src/clearance.rs:108-120` (no cap on pending requests)
  - Fix: parse the authority properly, and cap pending requests.
  - Verify: `http://host:8080/path` connects to port 8080.

- [ ] **S-24 · Error JSON built by string formatting; panicking byte slices** (Phases 1, 2, 4)
  - Where: `crates/smith-server/src/http.rs` (`git_service_info` and `git_rpc` build JSON with `format!` and `.replace('"', "'")`, which also leaks git stderr), `:424` (`commit[..12]` on worker input)
  - Fix: `serde_json` for every error body, and `get(..n)` for slices.
  - Verify: a git error containing quotes or backslashes produces valid JSON.

- [ ] **S-25 · Worker and client URL parsing breaks IPv6 literals** (Phase 5)
  - Where: `crates/smith-worker/src/main.rs:55-65` and `:183-194`
  - Fix: one URL parser, shared by the worker and `ServeClient`.
  - Verify: `[::1]:9240` parses for the worker and for clients.

- [ ] **S-26 · MCP `command = "npx"` doesn't resolve on Windows** (Independent)
  - Where: `crates/smith-mcp/src/manager.rs:133-154`, `README.md:228-236`
  - Problem: Rust's `Command` only appends `.exe` on Windows, so `npx` (which is `npx.cmd`) isn't found.
  - Fix: resolve program names with the `which` crate, which honours PATHEXT.
  - Verify: `npx` resolves through `which` and starts.

- [ ] **S-27 · Container hardening** (Independent)
  - Where: `crates/smith-sandbox/src/lib.rs:303-345`
  - Problem: no `--cap-drop=ALL` and no `no-new-privileges`, and the boot script runs as root.
  - Fix: drop capabilities, set `no-new-privileges`, and keep root only for the boot step.
  - Verify: `podman inspect` shows the dropped capabilities.

- [ ] **S-28 · Re-applying the nftables ruleset appends rules** (Independent, to verify)
  - Where: `crates/smith-sandbox/src/lib.rs:979-981`, `crates/smith-sandbox/src/enforce.rs:82-89` (the test comment says `nft -f` replaces the table)
  - Problem: `nft -f` merges into an existing table, so stale rules persist until reboot.
  - Fix: delete the table and reload it in one transaction.
  - Verify: after two session starts, the ruleset holds one copy of each rule.

- [ ] **S-29 · Git arguments without `--`; partial repository initialisation is never repaired** (Independent)
  - Where: `crates/smith-worker/src/materialise.rs:109-118` and `:138-148`, `crates/smith-server/src/forge.rs:151-171`
  - Fix: `--` before URLs and refs. Initialise repositories in a temporary path and rename them into place.
  - Verify: a branch name beginning with `-` is refused.

- [ ] **S-30 · Secrets on command lines** (Independent)
  - Where: `crates/smith-sandbox/src/lib.rs:505` (`CLEARANCE_TOKEN` as a podman argument), `crates/smith-cli/src/main.rs:122-124` and `:232-234` (`--token`)
  - Fix: pass secrets through a file or stdin, and offer a `--token-file` option.
  - Verify: the token doesn't appear in the process argument list.

- [ ] **S-31 · Resume can overwrite a turn's in-memory history** (Independent)
  - Where: `crates/smith-server/src/controller.rs:1225-1269`
  - Problem: the busy check isn't atomic with the history swap, so a turn that starts in between loses its in-memory messages (the JSONL log keeps them).
  - Fix: claim the busy flag atomically for the whole resume.
  - Verify: a resume during a turn returns busy, and the turn's history survives.

- [ ] **S-32 · Credential helper splices the token into a shell snippet** (Independent; pairs with S-02)
  - Where: `crates/smith-worker/src/materialise.rs:53-58`
  - Fix: have the helper read the token from an environment variable, or validate its characters.
  - Verify: a token containing `$(…)` isn't executed.

- [ ] **S-33 · Heartbeats can re-grant released sessions** (Independent, to verify)
  - Where: `crates/smith-server/src/controller.rs:1340-1367`
  - Problem: after a daemon restart, a worker's `live_leases` can re-grant sessions that were already released.
  - Fix: keep a set of released sessions, or require a lease record before re-learning.
  - Verify: after a restart, a heartbeat listing a released session doesn't re-grant it.

- [ ] **S-34 · Mount specification built by string concatenation** (Independent)
  - Where: `crates/smith-sandbox/src/lib.rs:292-301`
  - Problem: a colon in the workspace path changes the meaning of the mount.
  - Fix: pass the mount as separate arguments, or reject paths containing colons.
  - Verify: a workspace path containing `:` mounts the intended directory or is refused.

## Documentation to correct

- [ ] **D-01 · Home and sensitive path refusal is overstated** (see S-17). `README.md:28-29`
- [ ] **D-02 · TLS verification is described but not built** (see S-05). `docs/WORKER_MESH.md:18-20`. Phase 6.
- [ ] **D-03 · "Only the proxy and gateway DNS may leave the bridge"** (see S-08). `README.md:52-59`
- [ ] **D-04 · Risky commands always re-prompt; "always" binds to the exact command** (see S-11). `README.md:32-37`
- [ ] **D-05 · "Nothing is pushed without opting in"**. The merge gate pushes `smith/integration/<parent>` on every pass (`crates/smith-worker/src/main.rs:527-569`), which `docs/WORKER_MESH.md:72-74` describes. `README.md:196-198`
- [ ] **D-06 · Deep checks "never touch your project directory"** (see S-01). `README.md:273-274`
- [ ] **D-07 · `command = "npx"` example** (see S-26). `README.md:228-236`
- [ ] **D-08 · File tools are locked to the canonicalised workspace** (see S-07). `README.md:27`
