# Milestone 2 worker supervision

The daemon coordinates project intent and worker slots. Each slot independently tracks its observed state, PID and start identity, generation, last exit, restart count, and bounded output summaries. SQLite stores configuration and durable ownership leases. A guardian owns one OS child and is the only code that signals or reaps it. The `roost-process` crate owns argv construction, macOS process inspection, signaling, and output capture. The CLI submits versioned requests over the existing IPC client.

## Lifecycle

- Create enabled profiles with explicit connections and ordered queue lists. Starting a project requests one worker per configured instance of every enabled profile. The limit is 16 instances per profile and 32 supervised instances per daemon. An explicit PHP executable path is required for each start or restart.
- A start or restart response acknowledges intent. Use `process.list` to watch `starting`, `running`, `stopping`, `backoff`, `failed`, or `stopped`. A new start while the project already desires running is a no-op. An explicit restart replaces the project workers after their previous groups finish cleanup and resets their restart counters.
- The guardian sends SIGTERM and escalates to SIGKILL after the profile's grace period, up to 60 seconds. Manual stop cancels any scheduled retries. Worker restart policies are `never`, `on_failure`, and `always`. Eligible exits retry after 1, 2, 4, 8, 16, then 30 seconds, with a 30-second cap. The tenth rapid exit latches failure. Five minutes of stable running resets the rapid-failure count. A failed launch is visible as a terminal failure.
- Roost checks Composer manifests and lockfiles for Horizon before starting workers. For an application that declares Horizon, starting unmanaged workers requires `--allow-horizon`. Runtime detection of Horizon is deferred.
- Project intent is scoped to the current daemon session. After daemon recovery, the project is stopped until explicitly started again. Project and profile configuration persist.

## Process identity and recovery

The daemon reserves each slot in SQLite before it launches its guardian. A guardian must atomically claim the lease before starting a worker. On restart, the daemon can cancel any reservation that a guardian has yet to claim. The guardian and the worker run in separate sessions, so a foreground terminal interrupt reaches the daemon while the guardian can finish cleanup. The worker starts in its own process group.

The guardian verifies the UID, parent, process group, and OS start timestamp. An unreaped child pins the leader PID and prevents group ID reuse while the guardian signals the group. The guardian verifies that the group has no live members and reaps the child before writing a terminal lease. Child processes that deliberately leave the worker's process group fall outside this ownership guarantee.

The guardian treats closure of its daemon control pipe as a stop, including after SIGKILL of the daemon. A replacement daemon waits for durable cleanup confirmation before accepting a new start for that project. If the guardian disappears before it confirms cleanup, Roost exposes the unresolved lease as `recovery_blocked` and retains it for manual investigation. It never signals a stale PID on the strength of a stored lease. The Milestone 2 CLI cannot clear an unresolved lease.

## Output and diagnostics

Each slot retains up to 64 timestamped summaries with byte and newline counts by stream and generation. Summary records contain no application output text. Worker output can contain credentials, and arbitrary text redaction is unresolved. The status explains whether an unexpected nonzero exit emitted stderr and gives a fallback when stderr is empty. Output and recent exit details are session-local. Persistent crash reports are planned for Milestone 4.
