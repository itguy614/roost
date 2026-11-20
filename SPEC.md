# Roost v0.1 — Engineering Specification

Status: implementation baseline. Platform: macOS first (proposed macOS 13+, Apple Silicon and Intel). Desktop: Tauri 2 + Vue 3 + TypeScript + Pinia + Tailwind. Backend: Rust Tokio. CLI: Rust Clap. Persistence: SQLite. IPC: versioned JSON-RPC 2.0 over a private Unix domain socket. Service: per-user launchd LaunchAgent. Initial runtime provider: Laravel Herd. Future provider: Flock.

## Product goal
Manage Laravel background development processes across local applications and monorepos without terminal windows. GUI and CLI operate on one daemon-owned state. Workers survive GUI closure. Support queue workers and schedulers only in v0.1; no arbitrary commands, Horizon job metrics, Docker, or remote orchestration. Recognize Horizon later, and avoid spawning conflicting `queue:work` processes when a project is Horizon-managed.

## MVP requirements
1. Register Laravel projects by directory, validate `artisan`, resolve project-specific PHP executable through a Herd provider (with system-PHP fallback where explicitly selected).
2. Discover configured queue **connections** by inspecting Laravel's effective configuration through a bounded, sanitized project PHP subprocess. Do not confuse connections with queue names: Laravel has no complete registry of all queue names. Suggest names via best-effort static inspection (`onQueue`, `$queue`, `Queue::pushOn`, etc.) and explicit configuration; never auto-create worker profiles without user approval.
3. Define multiple queue worker profiles per project, with connection, ordered queue names, instance count, sleep, tries, timeout, restart and watch settings. Allow separate or shared queue workers. One `schedule:work` supervisor per project.
4. Group multiple Laravel projects; start, stop, restart, and inspect groups. Shared monorepo paths must explicitly declare affected projects. Avoid restarting unrelated projects.
5. Watch relevant PHP/config files and `.env`, `composer.json`, `composer.lock`, with default exclusions `.git`, `vendor`, `node_modules`, `storage/logs`, `storage/framework`, build output. Debounce 500 ms with a 2-second coalescing cap; serialize and deduplicate restarts; reconcile after watch errors and macOS sleep/wake.
6. Supervise each child process independently. Desired state and observed state must be separate. Lifecycle: stopped, starting, running, stopping, backoff, failed. Graceful SIGTERM then bounded SIGKILL of verified owned processes. On unexpected exit, configurable `never`, `on-failure`, `always` policies; exponential backoff 1,2,4,8,16,30 seconds capped, max 10 rapid failures, reset after 5 minutes stable. Manual stops must never trigger automatic restart. Code-change restart policy is separate from crash policy.
7. Capture bounded stdout/stderr per process, exit code/signal, timestamps, recent lines and crash reports. Persist crash history and provide useful fallback diagnostics when stderr is empty. Never claim a Laravel job exception is a process crash when the worker remains alive.
8. Native macOS desktop notifications for crashes and permanent recovery failure, with opt-in/out per worker profile and deduplication. Notifications should work when the main window is closed through a menu bar process/helper when running and OS permissions allow. A daemon event alone does not display a native notification; design this boundary explicitly.
9. Process metrics: CPU usage, RSS memory, uptime, peak RSS, restarts, crashes, desired/actual counts. Poll OS metrics every 2 seconds. Queue metrics: pending queue depth where supported by connection-specific adapters, poll every 10 seconds with timeouts. Unsupported/unavailable values are null/unknown, never zero. Metrics retention: raw 24h, rollups 7d. Resource thresholds and notifications; resource-triggered restarts disabled by default. Queue job throughput, duration, wait time and per-job memory are out of scope until optional instrumentation or Horizon integration.
10. GUI: dashboard, project detail, queues/workers, scheduler, groups, logs, crash reports, metrics charts, settings, menu bar controls. CLI parity for lifecycle, inspection, logs, and metrics. Login autostart is opt-in per project; GUI launch is separate from daemon login launch.
11. Configuration both in GUI/SQLite and optional portable `.roost.json`. File-owned settings are not silently overwritten by GUI; SQLite owns local overrides and runtime state. Relative paths resolve from configuration file location. Secrets and machine-specific absolute paths are not committed. Explicit revision and conflict behavior.

## Architecture
Cargo workspace crates: `roost-core` (domain/state), `roost-process` (spawn/signals/process identity), `roost-watch` (notify/globs), `roost-ipc` (protocol), `roost-store` (SQLite), `roost-providers` (PHP/Herd), `roost-daemon` (service), `roost-cli` (Clap). Desktop at `apps/desktop` with Tauri bridge; GUI must not access SQLite directly. Daemon owns lifecycle and durable state. No network listener. Unix socket private directory and restrictive permissions; validate peer credentials where possible; protocol negotiation and bounded message sizes. Do not use shell execution for PHP commands. Validate paths and arguments. No root privileges.

## Data model
- Group: UUID, name, optional root_path, watch rules, timestamps.
- Project: UUID, optional group_id, name, canonical path, provider, PHP override, desired state, autostart boolean, watch enabled/rules, config owner, revision.
- WorkerProfile: UUID, project_id, name, connection, ordered queue names, desired instances, sleep/tries/timeout, graceful stop timeout, crash restart policy, code-change restart policy, notifications, enabled.
- SchedulerProfile: unique project_id, enabled, mode `schedule_work`, restart/stop policies.
- ProcessInstance: UUID, profile ID, PID, OS process start identity, generation, observed state, timestamps, exit code/signal, restart count, reason, last error.
- Event, CrashReport, MetricSample, QueueSnapshot, ConfigRevision, Operation.

Group membership is at most one group per project in v0.1. Multiple worker profiles per project; multiple process instances per worker profile; at most one Roost-managed scheduler per project.

## IPC v1 (proposed)
Handshake `daemon.hello` (protocol version, capabilities). `daemon.status`, `project.list/get/add/update/remove/start/stop/restart`, `group.list/create/update/delete/addProject/removeProject/start/stop/restart`, `process.list/get/restart`, `queue.connections/discover/suggestions`, `worker.profile.create/update/delete/scale`, `watch.status/enable/disable`, `config.get/validate/apply`, `provider.list/detect/resolvePhp`, `log.tail/subscribe/unsubscribe`, `event.subscribe/unsubscribe/history`, `worker.crashes`, `worker.crash.get`, `worker.restartPolicy.get/update`, `worker.recovery.reset`, `metrics.process.current/history`, `metrics.project.summary`, `metrics.queue.current/history`, `metrics.subscribe`, `metrics.alerts.list`, `metrics.thresholds.update`, `operation.get/cancel`.

Use JSON-RPC 2.0 newline-delimited messages with size limits. Mutations return operation IDs, support idempotency keys, and serialize by affected resource. Events carry monotonic sequence cursors for reconnect. Explicit protocol version mismatch errors. Do not implement all commands at once: implement vertical slices.

## Process correctness
The daemon is the sole process owner. Never kill processes by command-name matching. PID reuse is possible: record and verify OS start identity and owned process group before termination. Prove orphan/reconciliation behavior on macOS in an early prototype. On daemon restart, safely terminate verified Roost-owned survivors and replace as needed; otherwise mark uncertain ownership for manual intervention. Reconcile state after wake, upgrades and crashes. Ensure scheduler stop is complete before replacement starts. Protect against simultaneous GUI/CLI commands and restart storms.

## Laravel nuances
Queue connections come from `config/queue.php` / effective Laravel config, but queue names may be dynamically assigned and cannot be fully discovered. Runtime inspection can execute project bootstrap code; limit duration, privileges and returned fields. Queue depth is adapter-specific, and polling must not expose credentials or block supervision. A worker can listen to ordered queues (`--queue=high,default`) but priorities are not automatic balancing. Use `php artisan queue:work CONNECTION --queue=...` and `php artisan schedule:work` via direct argv. Account for `retry_after` versus worker timeout. Do not start both Horizon and unmanaged queue workers on the same project without explicit user confirmation.

## Nonfunctional targets
Idle daemon target <50 MB excluding children; GUI launch target <2s; local IPC reads target <100ms; watcher reaction after settled changes target <2s. These are targets, not guarantees. Offline operation. Logs rotate and redact secrets. Respect macOS notification permission. Signed/notarized distribution eventually.

## Test strategy
Unit: validation, config precedence, glob matching, state transitions, backoff, dedupe, IPC. Integration: multiple workers/queues/connections, scheduler, long job graceful stop, crash stderr and fallback, no auto-restart when disabled, rate limits, queue discovery with dynamic names, file change targeting, monorepo shared paths, queue depth unavailable, metrics PID identity, daemon recovery, GUI/CLI concurrent commands. macOS: login autostart opt-in, GUI close, sleep/wake, process identity, LaunchAgent install/upgrade/uninstall, notification permissions, code signing. Soak: 24h with multiple projects and induced crashes.

## Delivery milestones
M1 workspace + core models + SQLite + daemon/IPC + basic CLI. M2 process supervisor, queue worker lifecycle, logs, backoff and recovery. M3 Laravel runtime provider/discovery, scheduler, watcher and monorepo groups. M4 metrics, crash reports, native notification integration. M5 Tauri/Vue GUI and menu bar. M6 macOS packaging, security and reliability hardening. Each milestone requires tests and a demo. Initial engineering estimate ~9+ weeks, subject to discovery and packaging complexity.

## Acceptance criteria
GUI and CLI share state; workers survive GUI closure; multiple queue profiles and connections are supported; queue discovery distinguishes known and unknown names; code changes restart only affected workers; project groups work; scheduler never duplicates within Roost; unexpected crashes capture diagnostics, notify and follow configured restart policy; CPU/RSS and queue depth are visible with honest availability status; no runaway restart loops; autostart opt-in; no unrelated processes killed; clean signed macOS installation passes tests.
