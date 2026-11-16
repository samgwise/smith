# Security review tracker

Reviewed on 14 November 2026 against `main`. This was a static review, so nothing has been executed. Items marked **to verify** need a reproduction or test before they count as confirmed. Security rounds 2 and 3 closed every item test-first (round 3 batches 1–6 closed the final eight).

How to use this file:
- Tick `[x]` when the fix is merged and a test covers it. Add the PR or commit after the item.
- Keep IDs stable. Add new items at the end of their section rather than renumbering.
- Each item ends with its migration phase (see the migration plan) or "Independent" when it's fixed separately.

Status: 44 of 44 closed. Security round 3 (batches 1–6) closed the final eight items test-first; S-08's and S-28's live verifies run with `cargo test --ignored` on a podman machine.

## High

- [x] **S-01 · Workspace MCP config runs host commands before approval** (fixed, security round 2 batch 5)
  - Where: `crates/smith-core/src/trust.rs` (the trust store), `crates/smith-server/src/factory.rs` (`connect_mcp`), `crates/smith-mcp/src/config.rs` (`merged`), `crates/smith-cli/src/main.rs` (TUI prompt, `smith trust`), `crates/smith-cli/src/doctor.rs` (`mcp_deep_check`)
  - Problem: a repository's `.smith/mcp.toml` spawns its commands at session start, can pass host variables through `${VAR}`, and can set `approval = "allow"`. `smith doctor --deep` also calls a workspace tool with `{}` arguments, bypassing approval.
  - Fix: the workspace layer loads only from a trusted workspace — trust is the TUI prompt (once, remembered), `--trust-workspace` for headless (also remembered), `smith trust <workspace>` for daemon-hosted sessions, keyed by workspace identity in `~/.smith/trust.json`. The layer's `approval` and `env` are ignored even when trusted. Doctor never spawns or calls workspace MCP servers.
  - Verify: `trust_store_round_trip` (smith-core), `workspace_layer_is_sanitised` (smith-mcp), `resolve_finds_programs_on_path`; the marker-file check is a live manual reproduction.

- [x] **S-02 · Git token sent to any host** (fixed, security round 2 batch 3)
  - Where: `crates/smith-server/src/controller.rs` (`resolve_source`, `prepare_source`), `crates/smith-worker/src/materialise.rs` (`credential_scope`, `credential_env`), `crates/smith-protocol/src/workers.rs` (`WorkspaceSource.credential_origin`)
  - Problem: the worker's `SMITH_GIT_TOKEN` is returned to any host that issues a 401. A prompt-injected planner, or any API client, can name an attacker's URL.
  - Fix: the controller binds each token to the origin it was issued for (`WorkspaceSource.credential_origin` — the configured repo's origin for env tokens, the hosted repo's origin for minted forge tokens); the worker's credential helper is `credential.<origin>.helper`, so git only offers the token there. Sources without a bound origin get no helper at all.
  - Verify: `capture_server_receives_no_token` in `crates/smith-worker/src/materialise.rs` (a loopback server answering 401 for everything receives no token).

- [x] **S-03 · Request bodies buffered before auth, and unbounded framing reads** (fixed; the legacy HTTP layer was deleted at the Phase 6 cutover)
  - Where: was `crates/smith-server/src/http.rs:70` and `crates/smith-protocol/src/http.rs` (deleted)
  - Problem: an unauthenticated client could make the daemon buffer up to 256 MiB per connection, or grow memory without limit through chunk-size lines and trailers.
  - Fix: authenticate in middleware before any body is read; hyper handles framing with its own limits.
  - Verify: `s03_unauthenticated_body_is_not_read` in `crates/smith-server/tests/api_integration.rs`.

- [x] **S-04 · No connection cap and no request deadline** (fixed at the Phase 6 cutover)
  - Where: was `crates/smith-server/src/http.rs:38-53` (deleted)
  - Problem: any number of connections could be opened, and each could stall indefinitely.
  - Fix: a connection semaphore, hyper's header-read timeout (which also bounds idle keep-alive), and a per-frame idle timeout on git uploads.
  - Verify: `s04_idle_connection_is_bounded_and_cap_refuses_extras` in `crates/smith-server/tests/serve_integration.rs` and `connection_cap_closes_extra_connections` / `idle_connections_close_within_the_header_timeout` in `crates/smith-server/tests/api_integration.rs`.

- [x] **S-05 · Plaintext transport; TLS verification documented but not built** (fixed at the Phase 6 cutover)
  - Where: `crates/smith-server/src/api.rs` (tokio-rustls server), `crates/smith-protocol/src/client.rs` (reqwest/rustls client with pinning), `crates/smith-worker/src/main.rs` (the credential carries the pinned cert)
  - Problem: enrolment tokens, worker credentials, per-session forge tokens, and command streams crossed the network in cleartext, and the documented TLS verification was never built.
  - Fix: TLS on the api server (tokio-rustls/ring); reqwest clients with scheme-aware targets (`https://` selects TLS) and certificate pinning — the controller's PEM is the sole trust anchor (`--cert` / `SMITH_SERVE_CERT` for the CLI, `--cert` at worker enrolment, stored in the credential). Non-loopback binds need TLS or an explicit `allow_plaintext_remote`.
  - Verify: `serve_client_over_pinned_tls` in `crates/smith-server/tests/api_integration.rs` (health and SSE over a pinned self-signed cert; a mismatched pin is refused).

- [x] **S-06 · Agent can write files that run on the host, without approval** (fixed, security round 2 batch 1)
  - Where: `crates/smith-core/src/tools.rs` (read, write, and list need no approval), `crates/smith-sandbox/src/executor.rs:157-167`
  - Problem: the workspace is mounted read-write, so the agent can write `.git/config` (for example `core.fsmonitor`), `.git/hooks/*`, `.envrc`, or package scripts. These run on the host when the user next uses them, which contradicts the README's "arbitrary code only runs in the container".
  - Fix: `needs_approval` receives the call's path; writes to host-executing config (`.git/hooks/*`, `.git/config`, `.git/modules/*/config`, `.gitattributes`, `.envrc`) always demand approval. Approval-gated writes (not a read-only `.git` mount — the agent legitimately commits in-container).
  - Verify: `write_file_to_git_hooks_demands_approval` and `sensitive_write_paths` in `crates/smith-core/src/tools.rs`.

- [x] **S-07 · Symlink escape on write, for local and worker file operations** (fixed, security round 2 batch 2)
  - Where: `crates/smith-sandbox/src/executor.rs` (`resolve`, `write_file`)
  - Problem: for a dangling symlink, `exists()` is false, so `resolve` accepts the link path and `std::fs::write` follows it outside the workspace, with no approval. This applies to Linux and macOS hosts, where symlinks are supported.
  - Fix: `resolve` walks the candidate path with `symlink_metadata` and refuses any symlink component. The worker's file ops route through the same `LocalExecutor`, so the fix covers the worker too.
  - Verify: `write_file_refuses_a_dangling_symlink_escape` in `crates/smith-sandbox/src/executor.rs` (`#[cfg(unix)]`).

## Medium

- [x] **S-08 · Egress rules only filter forwarded traffic** (fixed, security round 3 batch 5)
  - Where: `crates/smith-sandbox/src/enforce.rs` (`ruleset`)
  - Problem: the nftables ruleset hooks only `forward`. Traffic to the bridge gateway is locally destined, so it's unfiltered on every port, and DNS through the gateway can carry data out.
  - Fix: the ruleset gains an `input` chain scoped to the bridge: only DNS (udp/tcp 53) from the gateway is accepted into the machine; every other locally-destined port drops. The residual DNS channel to the gateway is documented in the README (it is now the only one).
  - Verify: `ruleset_contains_a_dns_only_input_chain` in `crates/smith-sandbox/src/enforce.rs`; live container-side check runs with `cargo test --ignored`.

- [x] **S-09 · Clearance control port mismatch** (fixed, security round 2 batch 4)
  - Where: `crates/smith-sandbox/src/lib.rs` (`sidecar_run_args`), `crates/smith-egress/src/client.rs:12`
  - Problem: host clearance calls target `127.0.0.1:9239`, which isn't published. Interactive clearance would time out to deny. That fails closed, but the feature doesn't work.
  - Fix: the sidecar publishes `127.0.0.1:9239:9239`, and the port is defined in one place — `smith_egress::client::CLEARANCE_CONTROL_PORT` (the sandbox's duplicate constant is gone).
  - Verify: `sidecar_publishes_the_clearance_control_port` in `crates/smith-sandbox/src/lib.rs`; live check via `podman port smith-egress-proxy`.

- [x] **S-10 · Root command injection in remote file reads** (fixed, security round 2 batch 2)
  - Where: `crates/smith-sandbox/src/executor.rs` (`remote_stat_command`, `remote_cat_command`)
  - Problem: a path containing `'` breaks out of the quoting and runs commands in the container as root. The model controls the path.
  - Fix: the remote `stat` and `cat` commands go through the `shell_quote` helper the write path already uses.
  - Verify: `remote_read_commands_quote_the_path` in `crates/smith-sandbox/src/executor.rs` (a path containing `'; id; '` cannot break out).

- [x] **S-11 · Approval checks can be bypassed with compound commands and prefixes** (fixed, security round 2 batch 1)
  - Where: `crates/smith-core/src/agent.rs` (the risky list and the approval cache)
  - Problem: approving `git status` also approves `git clean -fdx`. Approving `cd src && make` also approves `cd x && rm -rf …`. The README says risky commands always re-prompt and that "always" binds to the exact command.
  - Fix: commands split on `&&`, `||`, `;`, `|`, `&`, backticks and `$( )` parens (a light splitter; it may over-split, which only ever fails closed). Every segment is checked; `git clean` joined the risky list; risky and compound commands bind allow-always to the exact normalised command, and a risky segment is only covered by an exact stored command.
  - Verify: `git_status_allow_always_does_not_cover_git_clean`, `compound_with_risky_segment_is_never_covered_by_a_prefix_approval` and `prefix_approval_does_not_cover_a_compound_with_a_risky_tail` in `crates/smith-core/src/agent.rs`.

- [x] **S-12 · Approval modal can hide the command; control characters reach the terminal** (fixed, security round 3 batch 6)
  - Where: `crates/smith-tui/src/ui.rs` (`render_modal`, `wrap_text`, `sanitise`)
  - Problem: a command over about 58 characters hides the `[y]/[a]/[n]` row, and text beyond about three wrapped lines is cut off. Escape sequences in model or tool output can drive the terminal.
  - Fix: the modal height grows with the wrapped command (bounded by the screen) and the command's TAIL is shown with an "earlier lines" marker, so the option row is always rendered last. All transcript, tool-pane and modal text is stripped of ANSI CSI/OSC sequences and C0 controls before rendering.
  - Verify: `modal_layout_shows_the_tail_of_a_long_command`, `modal_layout_wraps_at_the_width` and `sanitise_strips_ansi_and_control_characters` in `crates/smith-tui/src/ui.rs`.

- [x] **S-13 · Command output is buffered without limit** (fixed; reader caps in security round 2 batch 1, the op-result route cap at the Phase 6 cutover)
  - Where: `crates/smith-sandbox/src/lib.rs` (`exec_inner` and `exec_streaming`)
  - Problem: a noisy command can exhaust host or worker memory within its timeout. The 12 KB clamp happens only later, in the core.
  - Fix: each exec stream is capped at 2 MiB in the reader, keeping the tail with a truncation notice (decision D-6). The same reader serves the local and the worker streaming path, so worker results are capped at the source too.
  - Verify: `read_piped_caps_a_huge_stream_keeping_the_tail` in `crates/smith-sandbox/src/lib.rs` (a 200 MB stream stays bounded).

- [x] **S-14 · Forge tokens are repo-wide, never expire, and can't be revoked** (fixed; ref policy in Phase 4, TTL and revocation in security round 3 batch 3)
  - Where: `crates/smith-server/src/forge.rs` (mint/authenticate, expiry, revoke), `crates/smith-server/src/controller.rs` (`release_session`), the api's forge routes
  - Problem: any session's token can force-push or delete any ref in its repo, including `main` and other sessions' checkpoints. Tokens stay valid until the `.gitcred` file is deleted.
  - Fix, part 1 (done, Phase 4): an `update` hook installed by `ensure_repo` scopes session tokens to `refs/heads/smith/<session>` and `smith/integration/*` (the daemon token and `write_all` tokens — auto-push opt-in — cover everything); `receive.denyDeletes` and `receive.denyNonFastForwards` are set on every hosted repo.
  - Fix, part 2 (done, round 3 batch 3): minted tokens expire after `[serve] forge_token_ttl_secs` (default one week); expired grants fail `authenticate` and are dropped at recovery; `release_session` revokes the session's grant and removes its `.gitcred`.
  - Verify, part 1: `smith_repo_scheme_mints_session_tokens` (a `main` push and a foreign-session push with a session token are hook-declined) in `crates/smith-server/tests/serve_integration.rs`. Part 2: `authenticate_refuses_expired_grants`, `recover_skips_expired_grants` and `revoke_session_kills_the_grant` in `crates/smith-server/src/forge.rs`.

- [x] **S-15 · Secrets and state written with default permissions** (fixed, security round 3 batch 1)
  - Where: `crates/smith-core/src/secret.rs` (`write_private`), routed through `crates/smith-server/src/lib.rs` (serve-token), `crates/smith-sandbox/src/lib.rs` (egress-token), `crates/smith-server/src/workers.rs` (workers.json), `crates/smith-worker/src/main.rs` (worker credential), `crates/smith-server/src/forge.rs` (`.gitcred`)
  - Problem: nothing in the repo sets permissions, so on Linux and macOS these files are readable under a default umask. Writes aren't atomic, so a crash can corrupt the registry.
  - Fix: a shared `write_private` helper — the file is created 0600 (Unix), synced, and renamed into place; a failure never leaves a half-written secret. On Windows the profile directory already scopes the ACLs.
  - Verify: `private_files_have_owner_only_permissions`, `write_round_trips_and_leaves_no_temp` and `rewrite_replaces_the_file_cleanly` in `crates/smith-core/src/secret.rs` (`stat` check is `#[cfg(unix)]`).

- [x] **S-16 · `smith clean` removes unrelated containers** (fixed, security round 2 batch 2)
  - Where: `crates/smith-sandbox/src/lib.rs` (`session_container_filters`, shared by `clean_all` and `reap_stopped_sessions`)
  - Problem: the `name=smith-` filter is an unanchored regex, so any container with "smith-" anywhere in its name is force-removed.
  - Fix: the name filter is anchored (`^smith-`) and the `smith.session=1` label — set on every session container at `podman run` — is required.
  - Verify: `session_container_filters_are_anchored_and_labelled` in `crates/smith-sandbox/src/lib.rs`.

- [x] **S-17 · Workspace denylist is name-based** (fixed, security round 2 batch 2)
  - Where: `crates/smith-sandbox/src/lib.rs` (`refuse_sensitive_workspace`)
  - Problem: only the exact home directory and paths with components named `.ssh`, `.aws` and similar are refused. Mounting `C:\Users` or `C:\` is allowed, which exposes `samgw\.ssh` inside the container.
  - Fix: any ancestor of the home directory is refused (the home's `Path::starts_with` on the canonicalised workspace), on top of the existing sensitive-component check.
  - Verify: `workspace_refusal_refuses_ancestors_of_the_home_directory` in `crates/smith-sandbox/src/lib.rs`.

- [x] **C-01 · Worker results above 1 MiB are silently dropped** (fixed at the Phase 6 cutover; the worker-side output caps are S-13)
  - Where: was the 1 MiB cap on `/v1/worker/op-result` in the legacy HTTP layer
  - Problem: a command or `read_file` result larger than 1 MiB was rejected with the connection dropped, so the controller waited for the 300 s operation timeout.
  - Fix: the op-result route cap is 32 MiB (D-6) and failed controller posts are logged by the worker.
  - Verify: `worker_flow_and_two_mib_op_result` in `crates/smith-server/tests/api_integration.rs` (a 2 MiB op-result is accepted).

- [x] **C-02 · Git clients' gzip-compressed requests fail** (fixed at the Phase 6 cutover; git http-backend inflates gzip request bodies)
  - Where: was `crates/smith-protocol/src/http.rs:155-186` and `crates/smith-server/src/http.rs:627` (deleted)
  - Problem: git gzips RPC request bodies above 1024 bytes (`remote-curl.c:1008`), and the old forge passed them through un-decoded — upload-pack failed with `bad line length character`.
  - Fix: git http-backend inflates gzip request bodies (`HTTP_CONTENT_ENCODING`); the api streams bodies to it raw.
  - Verify: `red_c02_gzipped_upload_pack_request_is_decoded` and `clone_with_many_branches_goes_through_gzip` in `crates/smith-server/tests/serve_integration.rs`.

## Low

- [x] **S-18 · Non-constant-time token comparisons** (fixed; the serve api at the cutover, the egress sidecar in security round 2 batch 4)
  - Where: `crates/smith-egress/src/bin/smith-egress-proxy.rs` (`tokens_equal`)
  - Fix: the egress sidecar compares the bearer token with a no-early-exit XOR fold (no new dependencies in the standalone container build); the serve api compares SHA-256 digests with `subtle::ConstantTimeEq` (`tokens_match` in `crates/smith-server/src/api.rs`).
  - Verify: `token_comparison_is_constant_time_and_correct` in the sidecar binary; no `==` comparisons on secret values remain.

- [x] **S-19 · Fleet registry resets silently on a corrupt file; enrolment tokens never expire** (fixed, security round 2 batch 4)
  - Where: `crates/smith-server/src/workers.rs` (`WorkerRegistry::load`, `enrol`)
  - Fix: a corrupt registry fails `load` with an error (backed up as `<path>.corrupt`), aborting `smith serve` startup loudly; enrolment tokens expire after an hour.
  - Verify: `corrupt_registry_fails_loudly_and_keeps_the_file` and `enrolment_tokens_expire` in `crates/smith-server/src/workers.rs`.

- [x] **S-20 · Clearance poller reads the egress token once and never prunes** (fixed, security round 2 batch 4)
  - Where: `crates/smith-server/src/controller.rs` (`spawn_clearance_poller`)
  - Problem: on a fresh install, clearance prompts never reach the daemon.
  - Fix: the poller re-reads the token each loop (a token created after the daemon starts still works) and prunes `seen` to a bounded window.
  - Verify: `s20_clearance_poller_picks_up_a_token_created_after_start` in `crates/smith-server/tests/serve_integration.rs`.

- [x] **S-21 · No timeouts on MCP startup or provider calls** (fixed, security round 2 batch 5)
  - Where: `crates/smith-mcp/src/manager.rs` (`connect_server`, per-server `connect_timeout_secs`), `crates/smith-providers/src/*` (dial timeout, stream idle timeout)
  - Problem: a hung server or model call holds the session busy indefinitely.
  - Fix: the MCP handshake and tool listing are bounded by `connect_timeout_secs` (default 30 s, configurable per server); provider clients dial with a 30 s connect timeout, and a model stream silent for more than 120 s fails the step — the bounded tools' per-call timeout already covers tool calls.
  - Verify: `hung_server_fails_within_the_connect_timeout` in `crates/smith-mcp/src/manager.rs` (a server that never speaks MCP fails start in 2 s).

- [x] **S-22 · Unbounded buffers** (fixed, security round 3 batch 4)
  - Where: `crates/smith-providers/src/stream.rs` (`lines`), `crates/smith-core/src/instructions.rs` (`load_project_instructions`)
  - Problem: a line without a newline, or a very large `AGENTS.md`, grows memory or the prompt without limit.
  - Fix: a provider line exceeding 1 MiB without a newline fails the stream; instruction files over 256 KiB are truncated with a visible `[smith: truncated]` marker.
  - Verify: `unterminated_line_fails_at_the_cap` in `crates/smith-providers/src/stream.rs` (a 2 MiB line fails) and `oversized_instructions_are_truncated_with_a_marker` in `crates/smith-core/src/instructions.rs`.

- [x] **S-23 · Proxy and clearance edge cases** (fixed, security round 3 batch 5)
  - Where: `crates/smith-egress/src/bin/smith-egress-proxy.rs` (`parse_authority`), `crates/smith-egress/src/clearance.rs` (`try_create_pending`)
  - Fix: the plain-HTTP port comes from the URI's authority (`http://host:8080/path` dials 8080, not 80); pending clearance requests are capped at 64 — beyond the cap the request is denied instead of growing the set without limit.
  - Verify: `parse_authority_extracts_scheme_host_and_port` (sidecar binary) and `pending_requests_are_capped` in `crates/smith-egress/src/clearance.rs`; the live `http://host:8080/path` dial runs with `cargo test --ignored`.

- [x] **S-24 · Error JSON built by string formatting; panicking byte slices** (fixed at the Phase 6 cutover)
  - Where: was `crates/smith-server/src/http.rs` (git errors built with `format!` and `.replace('"', "'")`, which also leaked git stderr) and `:424` (`commit[..12]` on worker input)
  - Fix: every error body is serde-built (`error_response`/`json_response` in `crates/smith-server/src/api.rs`), and slices use `get(..n)`.
  - Verify: `cargo clippy --workspace --all-targets -- -D warnings` passes; the git error path returns serde-built JSON.

- [x] **S-25 · Worker and client URL parsing breaks IPv6 literals** (fixed, Phase 5)
  - Where: `crates/smith-protocol/src/client.rs` (`parse_serve_url`, shared by `ServeClient` and the worker)
  - Fix: one scheme-aware URL parser handling `[::1]:port` literals, ports, and paths.
  - Verify: the `serve_url_parsing` unit test in `crates/smith-protocol/src/client.rs` (bracketed IPv6 with and without ports, schemes, and error cases).

- [x] **S-26 · MCP `command = "npx"` doesn't resolve on Windows** (fixed, security round 2 batch 5)
  - Where: `crates/smith-mcp/src/manager.rs` (`resolve_command`)
  - Problem: Rust's `Command` only appends `.exe` on Windows, so `npx` (which is `npx.cmd`) isn't found.
  - Fix: program names resolve through the `which` crate, which honours PATHEXT; an unresolvable program reports a failed server, not a spawn panic.
  - Verify: `resolve_finds_programs_on_path` in `crates/smith-mcp/src/manager.rs`.

- [x] **S-27 · Container hardening** (fixed, security round 2 batch 2)
  - Where: `crates/smith-sandbox/src/lib.rs` (`run_args`)
  - Problem: no `--cap-drop=ALL` and no `no-new-privileges`, and the boot script runs as root.
  - Fix: `--cap-drop=ALL`, then `--cap-add` of exactly the five caps the root boot step needs (CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID — useradd + apt), and `--security-opt no-new-privileges` so the agent user cannot regain any of it.
  - Verify: `run_args_harden_the_container` in `crates/smith-sandbox/src/lib.rs`; live check via `podman inspect`.

- [x] **S-28 · Re-applying the nftables ruleset appends rules** (fixed, security round 3 batch 5)
  - Where: `crates/smith-sandbox/src/enforce.rs` (`ruleset`)
  - Problem: `nft -f` merges into an existing table, so stale rules persist until reboot.
  - Fix: the payload is an idempotent reload in one transaction — declare the table, `delete table`, then recreate it with exactly these rules.
  - Verify: `ruleset_reloads_idempotently` in `crates/smith-sandbox/src/enforce.rs`; the live apply-twice rule count runs with `cargo test --ignored`.

- [x] **S-29 · Git arguments without `--`; partial repository initialisation is never repaired** (fixed, security round 2 batch 3)
  - Where: `crates/smith-worker/src/materialise.rs` (all git invocations)
  - Fix: `--` before user-supplied URLs and refs (clone, worktree add, push, branch, merge), `git_ref` validated at the entry points (no leading dash, no whitespace), and the worker is headless (credential prompting off, so a failed auth fails fast instead of stalling the session).
  - Verify: `git_ref_validation_refuses_option_injection` in `crates/smith-worker/src/materialise.rs`; the materialise round-trip exercises the `--` forms with real git.

- [x] **S-30 · Secrets on command lines** (fixed, security round 3 batch 2)
  - Where: `crates/smith-sandbox/src/lib.rs` (`ensure_sidecar`, `sidecar_run_args`), `crates/smith-cli/src/attach.rs` (`resolve_token`)
  - Fix: the sidecar's environment (including `CLEARANCE_TOKEN`) rides a private server-side env file passed with `--env-file` and deleted once the sidecar is up; the CLI offers `--token-file` for `smith run --serve` and `smith attach` (keeping the token off the argv; `--token` remains for scripts).
  - Verify: `sidecar_args_never_carry_the_token` in `crates/smith-sandbox/src/lib.rs` and `resolve_token_prefers_the_file_form_and_rejects_both` in `crates/smith-cli/src/attach.rs`.

- [x] **S-31 · Resume can overwrite a turn's in-memory history** (fixed, security round 2 batch 4)
  - Where: `crates/smith-server/src/controller.rs` (`resume`)
  - Problem: the busy check isn't atomic with the history swap, so a turn that starts in between loses its in-memory messages (the JSONL log keeps them).
  - Fix: `resume` claims the busy flag with `compare_exchange` for the whole operation — a racing turn is refused while the swap happens, and the turn's history survives.
  - Verify: `s31_resume_during_a_busy_turn_returns_busy` in `crates/smith-server/tests/serve_integration.rs`.

- [x] **S-32 · Credential helper splices the token into a shell snippet** (fixed, security round 2 batch 3)
  - Where: `crates/smith-worker/src/materialise.rs` (`credential_env`)
  - Fix: the helper snippet is a token-free constant reading the token from the `SMITH_CRED_PASSWORD` env var; the token never appears in the snippet or on an argument list.
  - Verify: `credential_snippet_does_not_contain_the_token` in `crates/smith-worker/src/materialise.rs` (a token containing `$(…)` never reaches the snippet).

- [x] **S-33 · Heartbeats can re-grant released sessions** (fixed, security round 2 batch 4)
  - Where: `crates/smith-server/src/controller.rs` (`relearn_leases`)
  - Problem: after a daemon restart, a worker's `live_leases` can re-grant sessions that were already released.
  - Fix: re-learning skips sessions whose label state is `Released` — the label is recovered from the journal after a daemon restart, so the guard holds across restarts.
  - Verify: `relearn_skips_released_sessions` in `crates/smith-server/src/controller.rs`.

- [x] **S-34 · Mount specification built by string concatenation** (fixed, security round 2 batch 2)
  - Where: `crates/smith-sandbox/src/lib.rs` (`build_mount`)
  - Problem: a colon in the workspace path changes the meaning of the mount.
  - Fix: the mount builder refuses any `:` in the source beyond the Windows drive-letter prefix (which podman expects in the source).
  - Verify: `mount_spec_refuses_colons_in_the_source` in `crates/smith-sandbox/src/lib.rs`.

## Documentation to correct

- [x] **D-01 · Home and sensitive path refusal is overstated** (see S-17; the README now states what is actually refused — home, its ancestors, sensitive components — security round 2 batch 6). `README.md:28-33`
- [x] **D-02 · TLS verification is described but not built** (see S-05; built and documented in WORKER_MESH.md at the Phase 6 cutover).
- [x] **D-03 · "Only the proxy and gateway DNS may leave the bridge"** (see S-08, still open; the README now documents the residual gateway-side channels instead of overclaiming — security round 2 batch 6). `README.md:58-71`
- [x] **D-04 · Risky commands always re-prompt; "always" binds to the exact command** (see S-11, fixed in batch 1; the README documents the segment-by-segment compound checks — security round 2 batch 6). `README.md:34-43`
- [x] **D-05 · "Nothing is pushed without opting in"** (the README now states that each passing gate pushes the integration branch, and `auto_push` only opts in base-ref updates — security round 2 batch 6). `README.md:216-222`
- [x] **D-06 · Deep checks "never touch your project directory"** (see S-01; fixed in security round 2 batch 5 — doctor no longer spawns or calls workspace MCP servers; batch 6 documents it). `README.md:314-316`
- [x] **D-07 · `command = "npx"` example** (see S-26; fixed in security round 2 batch 5 — the example resolves on Windows too, documented batch 6). `README.md:256-265`
- [x] **D-08 · File tools are locked to the canonicalised workspace** (see S-07, fixed in batch 2; the README documents the symlink refusal — security round 2 batch 6). `README.md:27-29`
