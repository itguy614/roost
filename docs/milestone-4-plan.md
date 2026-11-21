# Milestone 4 implementation plan

Baseline is `master` at `b396440`, the Milestone 3 groups merge. Deliver process and queue metrics, crash-report inspection, resource alerts, and native macOS notifications. Reuse the daemon's verified process identities, guardian lifecycle, bounded crash capture, and versioned IPC.

Review the [architecture and acceptance contracts](milestone-4-contracts.md) before implementation. The contracts record proposed defaults, IPC examples, and the evidence each PR needs.

## 0. Native notification feasibility spike

After contract approval, run this spike before opening the first implementation PR. Use a disposable bundled native helper, with Swift and UserNotifications as the first candidate. Verify stable app identity, permission prompting, and actual notification presentation in a logged-in macOS session without the main window. Exercise manual lifecycle ownership, singleton locking, daemon reconnect, and a helper bundle upgrade with state reuse. Record signing requirements and distinguish OS request acceptance from presentation. Review the evidence before PR 1. Full integration remains PR 4. [Apple notification authorization](https://developer.apple.com/documentation/usernotifications/asking-permission-to-use-notifications) · [Apple agent app configuration](https://developer.apple.com/library/archive/documentation/General/Reference/InfoPlistKeyReference/Articles/LaunchServicesKeys.html)

The first run and the open authorization blocker are recorded in the [spike evidence](milestone-4-notification-spike.md).

## 1. Process metrics and retention

- Add typed samples in `roost-core` and a read-only macOS sampler in `roost-process`. Verify the PID and recorded start identity around collection. Sample CPU and RSS for active workers and schedulers every two seconds. Calculate CPU from consecutive samples of the same identity and monotonic elapsed time. Document CPU as 100% per fully used logical core. Expose uptime, peak observed RSS, restart and crash counts, and desired and actual instance counts. Distinguish per-generation gauges from project and profile lifecycle totals.
- In `roost-daemon`, copy the bounded list of owned process identities under the supervisor lock, then perform OS reads and SQLite writes off that lock. Discard stale results when a slot changes generation. Use bounded batches and treat first CPU readings, vanished processes, and unavailable readings as null with a reason.
- Add an additive SQLite migration in `roost-store` for indexed raw samples and five-minute rollups. Keep raw data for 24 hours and rollups for seven days. Make rollup retries idempotent. Preserve coverage counts and gaps, and bound history queries and response pages. At the existing 32-process cap, two-second sampling can produce about 1.38 million raw process rows per day. Measure database growth, cleanup, and IPC latency before approving the storage design.
- Expose `metrics.process.current`, `metrics.process.history`, and `metrics.project.summary` through `roost-ipc` and matching `roost-cli` commands. Test PID reuse, process replacement, counter resets, missing samples, retention boundaries, and migration from schema 4.

## 2. Queue depth

- Propose a per-project opt-in for periodic queue probing because Laravel bootstrap can run project code and connect to its configured services. While opted in, poll eligible queues on a ten-second cadence using a separate concurrency budget, bounded output and timeout, and no overlapping probes for one project. Deduplicate queues from configured profiles and resolve defaults through Laravel. Static queue suggestions remain advisory.
- Extend `roost-providers` with database and Redis adapters using the verified Laravel `pendingSize` behavior where available. Keep version and driver capability checks explicit. Return only symbolic identifiers, a pending count, and availability metadata. Unsupported drivers or versions, timeouts, and unavailable backends return null. A successful empty-queue count returns zero. Exclude reserved and delayed jobs and document differences in eligibility across adapters. [Laravel database queue](https://github.com/laravel/framework/blob/12.x/src/Illuminate/Queue/DatabaseQueue.php) · [Laravel Redis queue](https://github.com/laravel/framework/blob/12.x/src/Illuminate/Queue/RedisQueue.php)
- Add queue samples and rollups to `roost-store`, and `metrics.queue.current/history` with CLI parity. Test real Laravel database and isolated Redis queues with ready, delayed, reserved, and empty cases. Cover PHP and backend failures, strict output validation and scrubbing, watcher-driven configuration invalidation, and continued daemon responsiveness during a slow probe.

## 3. Crash inspection, alerts, and durable events

- Extend current worker and scheduler crash lists with report-by-ID inspection and CLI commands. Reuse committed crash reports and bounded output, including metadata-only reports recovered from guardian leases. Keep intentional stops and Laravel job exceptions from generating crash alerts.
- Add bounded, cursor-based event history in `roost-store`, with typed worker crash, permanent recovery failure, and resource-threshold events. Commit crash events with the associated report. Use stable event IDs across retries and recovery. Add revisioned worker notification and threshold settings, `event.history`, `metrics.alerts.list`, and `metrics.thresholds.update`. Use durable cursor polling for helper reconnect and CLI inspection, and defer push subscriptions to Milestone 5 under the documented compatibility contract.
- Require sustained threshold violations and deduplicate repeated alerts until recovery or cooldown. Keep resource-triggered restarts off by default. Gate an explicit opt-in through the existing supervisor and guardian cleanup, with a cooldown, a bounded attempt count, and checks for current identity and desired state. Test manual-stop races, stale metric samples, alert deduplication, restart limits, and event replay across daemon restart.

## 4. Native macOS notification delivery

- Ship the helper as a separate IPC client with bounded cursor polling and a durable local cursor. Use stable notification identifiers and compact, privacy-safe content without stdout, stderr, or stack traces. Report permission denial and delivery errors separately from daemon event creation. Use a fake notification sink for automated tests and a real macOS session for allow, deny, revoked permission, reconnect, and duplicate delivery checks. Stable identifiers reduce duplicates across retries, and OS presentation is verified separately from request acceptance. [Apple notification request identifiers](https://developer.apple.com/documentation/usernotifications/unnotificationrequest/identifier)

## Review and acceptance

- Open one PR per implementation slice in order after the feasibility spike. Each PR requires review of its invariants, failure behavior, measured costs, IPC compatibility, and reproducible demonstration. Keep throughput instrumentation, Horizon metrics, and arbitrary command execution outside Milestone 4. Keep the Tauri dashboard and menu bar in Milestone 5, and distribution signing, notarization, and installation in Milestone 6.
- Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` for each slice. Run helper tests and a manual macOS notification check for the fourth slice. This workspace has Apple Silicon macOS, Swift, PHP, and a Redis executable. Verify Intel, minimum supported macOS, sleep/wake, and actual notification delivery separately.
- Ship a reproducible walkthrough that observes CPU and RSS, verifies database and Redis pending counts and unsupported nulls, forces a disposable worker to crash, retrieves its report, sees a deduplicated native notification, and demonstrates that a manual stop remains stopped.
