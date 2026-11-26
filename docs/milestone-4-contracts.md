# Milestone 4 architecture and acceptance contracts

These are proposed contracts for review before implementation. The [implementation plan](milestone-4-plan.md) starts with the native notification feasibility spike after contract approval. PRs 1 through 4 remain independently reviewable.

## 1. Metric identity and attribution

| Scope | Identity and meaning |
| --- | --- |
| Project | Stable project UUID. Summaries combine that project's queue workers and scheduler and report their desired, running, and unresolved counts separately. |
| Profile and slot | Project UUID, process kind, profile UUID, and slot index. This is the logical series across replacements. A retired profile UUID identifies historical data only. |
| Process generation | Logical slot, instance UUID, and generation number. Within one generation, samples must also match the guardian-recorded PID and OS start identity. A replacement can change the instance UUID or increment the generation. Neither a PID nor a generation number alone identifies a sample. |

Collect subject identities under the supervisor lock. Read OS counters off that lock, verify identity before and after the read, and discard results if the slot has since changed. Compute CPU from two monotonic counter readings for the same generation. Use 100% per fully used logical core. First samples, counter resets, and intervals spanning a detected wake or sampling gap yield unknown CPU and establish a new baseline. Uptime comes from the current process start on a verified OS time basis. Per-generation peak RSS is the largest successfully observed RSS in that generation. CPU deltas, uptime, and peaks never carry over to a replacement.

Expose restart and crash counts as lifecycle totals keyed by logical slot, profile, and project, with an explicit counting start time. Count a committed crash once even after lease recovery. A relaunch counts as a restart when the replacement generation is confirmed running. Aggregate current gauges only from the current generations. Report coverage for partial aggregates and represent missing values as null. An unresolved lease is reported separately from confirmed running instances.

## 2. Resource budgets and schema gate

Before selecting the storage schema, run a reproducible benchmark on Apple Silicon macOS with 32 confirmed active processes, including queue workers and a scheduler. Compare the merged baseline with candidate sampling enabled. Run the two-second process cadence and ten-second queue cadence with opted-in database and Redis fixtures. Include idle and loaded workers, active metrics readers, restart churn, a slow queue backend, and cleanup while samples are arriving. Record hardware, OS, dependency versions, fixture size, commands, duration, and raw results.

| Measure | Required evidence |
| --- | --- |
| Storage | Main database and journal bytes, bytes per row and per day, rows retained after 24 hours, and five-minute rollup size through seven days. At 32 processes, the nominal process load is 1,382,400 raw rows per day before queue samples and indexes. Use a real cadence run plus accelerated seeded retention tests and a sustained 24-hour soak before sign-off. |
| Cleanup | Batch duration, writer stalls and contention, retry behavior, and physical disk growth during pruning and rollup. Test the 24-hour raw and seven-day rollup boundaries and demonstrate that retrying compaction preserves the aggregates. |
| CPU and memory | Added daemon CPU over baseline, probe CPU and concurrency, peak and steady-state daemon RSS, and dropped or delayed samples. Track worker and helper costs separately. Preserve the specification's under-50 MB idle daemon target, excluding children. |
| Responsiveness | Report p50, p95, and p99 for status, process and metrics reads, and representative lifecycle RPCs during the workload and cleanup. Use the specification's under-100 ms local read target as the proposed p95 review criterion. Verify lifecycle polling continues when telemetry is slow. |

Publish the measured results in PR 1 and accept byte, CPU, and peak-memory budgets in review before freezing the schema. Candidate storage batches and retention cleanup run off the supervisor lock with bounded queues and pages. Under sustained pressure, skip telemetry work and expose coverage gaps. Queue-probe measurements may use an isolated workload harness during the schema benchmark and must be repeated with the real adapters in PR 2.

The queue slice uses working envelopes of 5% of one core for the daemon with probes disabled, 7 percentage points incremental for probes, and 12% combined at eight enabled projects. Compare zero, one, two, four, and eight projects and include daemon, PHP, Herd, and related subprocess CPU. The cumulative storage envelopes including journals are 180 MiB under steady operation and 250 MiB under pathological churn. The daemon keeps the 50 MB RSS and 100 ms local IPC p95 targets. Milestone 6 includes multi-day steady-state retention and physical storage recycling under the final workload.

## 3. Queue probe safety and semantics

Queue polling is off by default and explicitly enabled per project. Resolve PHP and the effective Laravel queue configuration through the existing provider boundary. Select only configured queues and Laravel-resolved defaults, deduplicate selectors, and discard results after disable, project revision changes, or watcher invalidation. Roost issues only count queries against supported adapters. Trusted Laravel bootstrap can still execute application code, so the read-only contract covers Roost's probe operations.

Run probes off the supervisor lock in isolated subprocesses. Use direct argv, a minimal environment, and validated symbolic connection and queue names. Cap concurrency at two across the daemon and one per project. Cap each probe at 32 selectors and 16 KiB of captured output. The two-second slot budget includes launch, reads, and process-group cleanup. Set the execution timeout early enough to leave cleanup time. Keep a slot occupied until cleanup is confirmed. A slot that exceeds its budget reduces capacity and is surfaced as a probe failure.

**Cadence and saturation.** Open a monotonic ten-second round with at most eight admissions, one batch per project. Two slots can finish four two-second waves in eight seconds, leaving two seconds of scheduling margin. This covers eight eligible projects with up to 32 selectors each every round at the stated bounds. Keep one rotating project queue and one rotating selector offset per project. Advance after dispatch even on failure, put newly eligible projects at the tail, and carry undispatched projects forward. Coalesce missed rounds after wake or delay. Stop admissions when the remaining round cannot fit the slot budget. With a stable set of `P` projects and full capacity, a project receives a batch within `ceil(P / 8)` rounds. Selectors beyond 32 rotate over successive visits. A timeout marks affected values stale immediately if a last success exists, otherwise unknown. An unrefreshed success becomes stale at 20 seconds with a capacity reason when deferred. Keep the last successful value and its timestamp separate. PR 2 must measure cadence, backend and cleanup time, and wait time under saturation.

Durable queue history keeps one periodic observation per concrete series per 30-second UTC slot and retains observed first readings, availability changes, failure-reason changes, and resolved identity changes. Rows can be periodic, transitional, or both. Rollups report coverage separately and aggregate successful periodic observations only. Retain raw observations for 24 hours and five-minute rollups for seven days. Current readings can show brief depth-only spikes that fall between durable observations.

**Read-only verification.** The initial version matrix is the exact Laravel releases 12.50.0 and 13.43.0 with stock database and Redis queue classes. In both versions, `DatabaseQueue::pendingSize` builds a filtered aggregate `SELECT COUNT` over the configured jobs table for the selected queue, unreserved rows, and jobs available by the current time. In both versions, `RedisQueue::pendingSize` issues `LLEN` for the resolved ready-list key. The Redis path leaves delayed and reserved sets untouched. The database path avoids locking or reserving a job. The probe will allowlist verified version and driver combinations and return unsupported for custom classes or unverified paths. [Laravel 12.50 database](https://github.com/laravel/framework/blob/v12.50.0/src/Illuminate/Queue/DatabaseQueue.php) · [Laravel 12.50 Redis](https://github.com/laravel/framework/blob/v12.50.0/src/Illuminate/Queue/RedisQueue.php) · [Laravel 13.43 database](https://github.com/laravel/framework/blob/v13.43.0/src/Illuminate/Queue/DatabaseQueue.php) · [Laravel 13.43 Redis](https://github.com/laravel/framework/blob/v13.43.0/src/Illuminate/Queue/RedisQueue.php)

In PR 2, prove these call paths with real Laravel fixtures. Capture the SQL and Redis commands on isolated backends and compare queue contents before and after probing. Include ready, delayed, and reserved jobs, an empty queue, and a second queue on the same connection. Redis includes due jobs once they enter the ready list. Test an unsupported driver, a missing capability, hostile output, and a hung backend. Preserve credentials inside the project runtime. A confirmed empty queue is zero. Unsupported or failed reads are null.

## 4. IPC and reconnect

Milestone 4 ships durable cursor polling through `event.history`, `metrics.process.history`, and `metrics.queue.history`. Defer `event.subscribe`, `event.unsubscribe`, `metrics.subscribe`, and its unsubscribe operation to Milestone 5. Advertise only implemented methods in the v1 handshake. Preserve existing method shapes and frame limits. Use bounded page sizes and bounded history ranges.

- Events and metric history use separate committed sequence streams. An opaque cursor binds the stream lineage, scope, filters, and last scanned sequence. Resume with the same scope and filters. Persist ordering with the records it describes, and return `next_cursor` and `has_more`. The first page captures a high-water mark that subsequent pages reuse until drained. The next poll captures a new mark. Append rollups only for finalized buckets and key their writes idempotently by series and bucket. Sequence values encoded as strings avoid loss of precision in JavaScript.
- Retention advances a durable per-stream prune watermark in the same transaction that removes records. Validate that watermark and read each page in one SQLite read snapshot. If the last scanned sequence is below the prune watermark, return a typed `cursor_expired` error before returning any records on that page, even with a pinned high-water mark. Equality is safe because the cursor is exclusive. Conservative invalidation is allowed for filtered queries. Return the retention boundary and a recovery cursor. Use separate errors for a different database lineage or mismatched scope. For events, resume at the retained boundary and surface the missed interval.
- Raw and rollup history use separate cursors and explicit resolutions. An expired raw cursor can restart on the retained raw range or request five-minute history with a new rollup cursor. There is no implicit switch between resolutions. To combine views, choose a UTC five-minute cutover at or after the raw retention floor, take finalized rollups ending at or before it, and take raw samples at or after it. Surface uncovered buckets or intervals as gaps. Test pruning between pages in all streams, the exact watermark boundary, and this cutover with partial buckets and filtered series.
- Future subscriptions use an explicitly negotiated, dedicated Unix-socket connection. A client supplies its polling cursor and filters. The server replays retained records and transitions to live updates on the same sequence stream. A subscription ACK, data frames, and unsubscribe are bounded and identifiable. Disconnect slow consumers on overflow and let them recover through cursor polling. Existing request-response clients receive push frames only after explicitly subscribing.

## 5. Native notification semantics

| Stage | Meaning and durable evidence |
| --- | --- |
| Event created | The daemon commits a stable event UUID and sequence. A crash event commits together with its crash report. A permanent recovery failure or threshold episode gets its own deduplicated event. |
| Helper received | The helper reads the committed event, persists its delivery record, and rechecks per-profile opt-in and OS authorization before submission. Its cursor advances only after a terminal outcome has been journaled. |
| OS accepted | UserNotifications accepts the request. Retry transient failures with capped backoff under the same stable OS request ID. Record denied permission, opt-out, or expiration as terminal skips. Persist the outcome before advancing the cursor. |
| Presented | macOS controls presentation, including Focus and notification settings. Report actual presentation as unknown through IPC unless a supported signal establishes it. Verify visible presentation manually during the feasibility spike and PR 4 demo. |

Delivery is best effort. A helper crash between OS acceptance and its local journal write can cause a retry. Use the same request identifier across retries and cap retries. Persist the cursor and use one helper per daemon state directory, with delivery records scoped to the daemon event-stream lineage. Expose connection freshness, permission state, last receipt, and last OS submission outcome separately. Use minimal metadata in visible notifications and exclude captured output. [Apple notification authorization](https://developer.apple.com/documentation/usernotifications/asking-permission-to-use-notifications) · [Apple notification request identifiers](https://developer.apple.com/documentation/usernotifications/unnotificationrequest/identifier)

**Lifecycle.** The logged-in user starts and stops the bundled helper in Milestone 4. The daemon owns durable events and runs independently of the helper. Hold a nonblocking exclusive lock on a private file under the canonical daemon state directory for the entire helper lifetime. A duplicate exits before prompting or sending. Reconnect with bounded backoff after daemon loss, renegotiate v1 and the event capability, then resume the saved cursor for the same lineage. A changed lineage surfaces a gap and requires a fresh retained cursor. An incompatible daemon leaves delivery paused and preserves the journal. Upgrade by stopping the old helper and replacing the bundle with the same app identity and stable signing requirement. Preserve private state, acquire the same lock, recheck authorization, and migrate the journal additively. Verify process ownership, singleton behavior, reconnect, and the local upgrade path in the spike. A local ad hoc signature can test feasibility. Distribution identity and permission continuity require separate verification. [Apple code signing requirements](https://developer.apple.com/documentation/Technotes/tn3127-inside-code-signing-requirements)

## 6. Resource alert state machine

Configure rules on worker profiles and evaluate each rule separately for every running worker slot's current generation. Profile and project aggregates are for inspection. There is one rule per profile and metric. CPU uses finite percentages with 100% per logical core and accepts values above 100%. RSS uses integer bytes. Both require `0 <= clear < breach`. Reject nonfinite CPU values, invalid RSS integers, duplicate metrics, unknown actions, and stale revisions. Resource-triggered restarts default to off.

Milestone 4.3 evaluates CPU thresholds using only fresh, correctly converted observations from the current owned process generation. Historical readings and rollups are for inspection. Alert evaluation and automatic restart decisions never consume historical readings, including data collected before the Mach timebase correction.

A meaningful rule update or deletion closes affected active episodes with `rule_updated` or `rule_removed`, preserves their historical evidence, cancels pending actions from the old revision, and resets sample streaks. The new rule requires three fresh samples collected after the change. A no-op update preserves the active episode. Edits and deletion preserve cooldown and attempt budgets for the logical slot and metric. Test a pending restart racing a rule change and an update while an episode is active.

| State or input | Transition and action |
| --- | --- |
| Clear → pending → active | Three consecutive fresh samples from the current generation at or above the breach threshold open one durable episode. A lower or missing sample clears the pending count. |
| Active → clear | Three consecutive fresh samples at or below the clear threshold close the episode. Values between thresholds leave it active and reset the clear streak. |
| Missing, stale, or unsupported | Reset consecutive sample counters. Keep an active episode visible with unknown or stale evidence. Never trigger a new episode or restart from these inputs. A wake or generation change resets the sampling baseline. Replacing the generation closes its episode with a recorded reason. |
| Repeated breach | Deduplicate by rule and episode. A proposed five-minute cooldown per logical slot and rule suppresses duplicate notification or restart actions across episodes. Retain suppressed episode history. Cooldown expiry alone triggers no action. |
| Restart | Require explicit opt-in and a fresh breach. Allow at most one attempt per episode and two attempts per logical slot in a rolling hour. Revalidate the current identity, revision, running intent, and guardian lease under the existing supervisor lock. Charge the durable attempt budget before asking the guardian to stop. An interruption after charging can consume an attempt without initiating a restart. Recovered actions must pass the same freshness and intent checks. |
| Manual stop | Committed stop intent cancels any pending threshold restart. If the guardian has already begun stopping for replacement, cleanup completes and the supervisor suppresses relaunch. A stale queued or recovered action cannot restore running intent. |

## 7. Example IPC result payloads for Milestone 5

These examples show the proposed `result` objects inside v1 responses. `fresh` has a current measured value. `unknown` identifies an eligible series without a usable value. `stale` has no current value and may include a separately labeled last known sample. `unsupported` identifies a missing driver or capability. Zero is a measured value.

### `metrics.process.current`

```json
{
  "identity": {"project_id":"00000000-0000-4000-8000-000000000001","kind":"worker","profile_id":"00000000-0000-4000-8000-000000000002","slot":0,"instance_id":"00000000-0000-4000-8000-000000000003","generation":2},
  "observed_at":"2026-11-20T20:00:00Z",
  "cpu_percent":{"state":"unknown","value":null,"reason":"first_sample"},
  "rss_bytes":{"state":"fresh","value":41943040},
  "uptime_seconds":{"state":"fresh","value":2},
  "peak_rss_bytes":{"state":"fresh","value":41943040}
}
```

### `metrics.process.history`

```json
{
  "resolution":"5m",
  "items":[{"sequence":"118","generation":2,"instance_id":"00000000-0000-4000-8000-000000000003","from":"2026-11-20T20:00:00Z","to":"2026-11-20T20:05:00Z","cpu_percent":{"state":"fresh","mean":12.5,"min":0,"max":36,"samples":113,"expected_samples":150}}],
  "next_cursor":"example-process-cursor-118",
  "has_more":false
}
```

### `metrics.queue.current`

```json
{
  "project_id":"00000000-0000-4000-8000-000000000001",
  "queues":[
    {"connection":"database","queue":"default","pending":{"state":"fresh","value":0,"observed_at":"2026-11-20T20:00:00Z"}},
    {"connection":"redis","queue":"high","pending":{"state":"stale","value":null,"reason":"timeout","last_known":{"value":4,"observed_at":"2026-11-20T19:59:40Z"}}},
    {"connection":"sqs","queue":"default","pending":{"state":"unsupported","value":null,"reason":"driver_unsupported"}}
  ]
}
```

### `metrics.queue.history`

```json
{
  "connection":"redis",
  "queue":"high",
  "resolution":"5m",
  "items":[{"sequence":"91","from":"2026-11-20T19:55:00Z","to":"2026-11-20T20:00:00Z","pending":{"state":"unknown","mean":null,"min":null,"max":null,"samples":0,"observations":3,"periodic_observations":2,"transition_observations":2,"legacy_observations":0,"expected_samples":10}}],
  "next_cursor":"example-queue-cursor-91",
  "has_more":false
}
```

### `worker.crash.get`

```json
{
  "id":"00000000-0000-4000-8000-000000000004",
  "project_id":"00000000-0000-4000-8000-000000000001",
  "profile_id":"00000000-0000-4000-8000-000000000002",
  "slot":0,
  "exit":{"at":1795204800,"code":7,"signal":null,"requested_stop":false,"forced":false,"stop_cause":null},
  "reason":"Recovered exit metadata after daemon interruption",
  "output_state":"unavailable",
  "output":[]
}
```

### `metrics.alerts.list`

```json
{
  "alerts":[{"id":"00000000-0000-4000-8000-000000000005","profile_id":"00000000-0000-4000-8000-000000000002","slot":0,"state":"active","metric":"rss_bytes","breach_at":104857600,"clear_at":94371840,"evidence":{"state":"stale","value":null},"restart":{"state":"off","attempts_in_last_hour":0}}],
  "next_cursor":null
}
```

### `notifications.helper.status`

```json
{
  "connection":"stale",
  "last_seen_at":"2026-11-20T19:59:45Z",
  "permission":"unknown",
  "last_received_event_id":"00000000-0000-4000-8000-000000000006",
  "last_submission":{"event_id":"00000000-0000-4000-8000-000000000006","state":"accepted","at":"2026-11-20T19:59:44Z"},
  "presentation":"unknown"
}
```

## 8. Failure injection and review gate

| Injected failure | Acceptance evidence |
| --- | --- |
| Late SQLite write failure | Abort a sample batch, a rollup write, and a crash report plus event transaction after earlier writes. Verify rollback, no cursor for uncommitted data, no raw deletion before rollup commit, and idempotent retry. Exercise disk pressure during a sustained run. |
| Daemon restart | Interrupt persistence and restart. Verify retained cursor order, no duplicate committed events, reset CPU baselines, preserved restart budgets, and no automatic restart of manually stopped projects. |
| Helper reconnect | Disconnect before receipt, after receipt, and after OS acceptance before local journaling. Verify bounded retries, stable IDs, cursor progression, expired-cursor recovery, and explicit permission behavior. |
| Queue timeout | Hang a probe while workers and a scheduler run. Verify bounded cleanup, a null or stale reading, no leaked secrets, and responsive lifecycle IPC. |
| PID replacement | Replace a process between collection and publication, and simulate PID reuse. Verify the replacement's first CPU sample is unknown and its uptime and peak belong to the new generation. |
| Sleep and wake | Use a fake clock to verify timer gaps and perform a real macOS sleep/wake run with active workers and a helper. Verify re-established baselines, no synthetic CPU spike, bounded poll resumption, cursor recovery, and no duplicate restarts or notifications attributable to wake. |

Each PR supplies a review record with its invariants, fault-test results, measured costs or a justified scope-specific measurement, compatibility evidence, and a reproducible demo. Architecture approval of these contracts precedes the feasibility spike. Review the spike evidence before PR 1 and the PR evidence before each merge. Milestone 4 ends with a verified end-to-end macOS demonstration and sign-off. Throughput instrumentation, Horizon metrics, arbitrary command execution, and desktop UI implementation are outside this milestone.
