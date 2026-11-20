# Milestone 2 worker supervision

The daemon coordinates project intent and worker slots. Each slot independently tracks its observed state, PID and start identity, generation, last exit, restart count, and bounded output lines. SQLite stores configuration, project intent, recent crash reports, and durable ownership leases. A guardian owns one OS child and is the only code that signals or reaps it. The `roost-process` crate owns argv construction, macOS process inspection, signaling, and output capture. The CLI submits versioned requests over the existing IPC client.

## Lifecycle

- Create enabled profiles with explicit connections and ordered queue lists. Starting a project requests one worker per configured instance of every enabled profile. The limit is 16 instances per profile and 32 supervised instances per daemon. An explicit PHP executable path is required for each start or restart.
- A start or restart response acknowledges intent. Use `process.list` to watch `starting`, `running`, `stopping`, `backoff`, `failed`, or `stopped`. A repeated start during active supervision is a no-op. An explicit start can activate saved intent that was dormant after a daemon restart. An explicit restart replaces the project workers after their previous groups finish cleanup and resets their restart counters.
- The guardian sends SIGTERM and escalates to SIGKILL after the profile's grace period, up to 60 seconds. Manual stop cancels any scheduled retries. Worker restart policies are `never`, `on_failure`, and `always`. Eligible exits retry after 1, 2, 4, 8, 16, then 30 seconds, with a 30-second cap. The tenth rapid exit latches failure. Five minutes of stable running resets the rapid-failure count. Permanent validation or execution errors fail immediately. Known transient OS resource failures and readiness timeouts with confirmed cleanup get up to two retries after one and two seconds under `on_failure` or `always`. Failure to prove cleanup blocks replacement.
- Roost checks Composer manifests and lockfiles for Horizon before starting workers. For an application that declares Horizon, starting unmanaged workers requires `--allow-horizon`. Runtime detection of Horizon is deferred.
- Every unexpected exit has a status reason. A clean exit without an eligible restart leaves observed state `stopped` and preserves desired state `running`. A nonzero or signal exit that will not retry leaves observed state `failed`. A clean exit is excluded from crash history.

## Restarting the daemon

Start, restart, and stop save intent in the same transaction as their IPC receipt. The launch intent stores the resolved PHP path and the user's Horizon override. A receipt replay returns the original acknowledgement without changing today’s intent.

- Normal daemon shutdown asks guardians to stop and records a clean marker when ownership cleanup is confirmed. Project intent is preserved.
- If the daemon is killed, the guardian detects EOF on its control pipe and cleans its group. The next daemon reports an interrupted shutdown and waits for the previous guardian's lease before replacing a worker.
- A user stop saves stopped intent, cancels pending recovery and retries, and asks active guardians to stop. A later daemon startup honors the saved stop even if autostart is enabled.

Autostart defaults to off. Use `roost project autostart ID on` to allow the daemon to resume saved running intent on future startups, after either a clean shutdown or an interruption. A project with autostart off retains its saved intent and stays dormant until a fresh start. Toggling autostart changes startup behavior and cancels queued recovery when turned off. Active workers continue under the current intent. Retry counters are session-local. Installing a login LaunchAgent is outside this change.

## Process identity and recovery

The daemon reserves each slot in SQLite before it launches its guardian. A guardian must atomically claim the lease before starting a worker. On restart, the daemon can cancel any reservation that a guardian has yet to claim. The guardian and the worker run in separate sessions, so a foreground terminal interrupt reaches the daemon while the guardian can finish cleanup. The worker starts in its own process group.

The guardian verifies the UID, parent, process group, and OS start timestamp. An unreaped child pins the leader PID and prevents group ID reuse while the guardian signals the group. The guardian verifies that the group has no live members and reaps the child before writing a terminal lease. Child processes that deliberately leave the worker's process group fall outside this ownership guarantee.

The guardian treats closure of its daemon control pipe as a stop, including after SIGKILL of the daemon. A replacement daemon waits for durable cleanup confirmation before accepting a new start for that project. If the guardian disappears before it confirms cleanup, Roost exposes the unresolved lease as `recovery_blocked` and retains it for manual investigation. It never signals a stale PID on the strength of a stored lease. The Milestone 2 CLI cannot clear an unresolved lease.

## Output and diagnostics

The daemon drains each pipe without blocking and reads at most 64 KiB per stream per tick. After guardian exit, it keeps draining across ticks before saving a crash report. It finishes at EOF or when no buffered data remains. A two-second deadline bounds cleanup if an inherited writer keeps producing output. It retains the most recent 32 lines per stream per slot and keeps output for up to 64 slots across the daemon, favoring live workers. It buffers at most 512 input bytes per unfinished line. Longer lines are discarded through the next newline. Entries include stream, generation, timestamp, byte count, newline count, and sanitized text. Separate per-stream limits keep noisy stdout from evicting stderr. `roost log tail ID` returns live and recent lines.

`roost worker crashes ID` returns the newest 12 unexpected nonzero or signal exits per project. SQLite retains at most 128 reports in total. The daemon commits a report and releases its completed lease together. Reports include the exit code or signal, timestamp, diagnostic reason, and the redacted lines for that generation. If the daemon dies before collecting its pipes, it can recover exit metadata from a completed guardian lease with no output attached. The worker's application may also maintain its own logs.

Redaction is best effort. It suppresses whole lines with recognized credential assignment patterns, authorization and cookie headers, URL user info, common token formats, and private-key blocks. It strips common ANSI control sequences and suppresses text with unsafe control characters. Unlabeled secrets, unfamiliar formats, sensitive arguments embedded in stack traces, and some multiline secrets can still appear. Review crash output before sharing it. Capture is local to the private Roost state and same-user IPC.
