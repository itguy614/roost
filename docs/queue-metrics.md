# Queue depth metrics

Queue polling is off by default. Enabling it opts a trusted local project into periodic Laravel bootstrap under the daemon user. Roost's adapter performs only a pending count. Laravel bootstrap may still run application code. Roost discards diagnostic output and sends only validated symbolic connection and queue names, counts, and fixed reason codes through IPC.

## Pending count semantics

| Driver | Count | Unavailable cases |
| --- | --- | --- |
| Database | Rows for the selected queue with no reservation and availability at or before the probe time | Missing connection or failed read |
| Redis | Entries in the resolved ready list | Delayed or reserved entries remain in their sets. A failed read is unavailable. |
| Other | Null with an unsupported reason | Custom queue adapter classes and framework versions outside the verified matrix are also unsupported. |

The allowlist is Laravel 12.50.0 and 13.43.0 and their exact stock database and Redis queue classes. Database polling uses the framework's filtered `SELECT COUNT`. Redis polling uses `LLEN` through the framework's resolved connection and key prefix. Connection setup can authenticate or select a Redis database. Zero means a backend confirmed the queue was empty. [Laravel 12.50 database](https://github.com/laravel/framework/blob/v12.50.0/src/Illuminate/Queue/DatabaseQueue.php) · [Laravel 12.50 Redis](https://github.com/laravel/framework/blob/v12.50.0/src/Illuminate/Queue/RedisQueue.php) · [Laravel 13.43 database](https://github.com/laravel/framework/blob/v13.43.0/src/Illuminate/Queue/DatabaseQueue.php) · [Laravel 13.43 Redis](https://github.com/laravel/framework/blob/v13.43.0/src/Illuminate/Queue/RedisQueue.php)

## Opt-in, current readings, and history

```sh
roost metrics queue enable "$PROJECT_ID"
roost metrics queue current "$PROJECT_ID"
roost metrics queue history "$PROJECT_ID" --from "$(($(date +%s) - 600))" --connection database --resolution raw
roost metrics queue disable "$PROJECT_ID"
```

Current readings are drawn from enabled worker profiles even when those workers are stopped. An empty queue list resolves the connection's Laravel default. Fields `requested_queue` and `queue` distinguish a profile default from a concrete queue. Default and explicit names resolving to the same queue share the backend read and historical sample within a batch. Unsupported selectors show null. Unvalidated names are omitted. A timeout reports stale immediately if a last success exists, with its own timestamp and value under `last_known`. Successful readings that have gone 20 seconds without an update report capacity staleness. Current readings rebuild after daemon restart, opt-in changes, or watcher invalidation. Stored history survives restarts and opt-out.

Probes have a two-second slot budget including Herd PHP resolution, PHP execution, and cleanup. Two projects can probe in parallel, with one in-flight probe per project and at most eight admissions in a ten-second round. The rotating project order and selector offsets continue across rounds. Each batch covers at most 32 selectors, and total stdout and stderr are capped at 16 KiB. Herd, system, and override resolutions are cached for up to 120 seconds in a 64-project LRU. Opt-out, project revision, and watcher invalidation discard the affected resolution. Changes in the selected executable, the configured launcher, or the project’s PHP selection files trigger a fresh lookup. The filesystem checks cover `.php-version`, `.herd.yml`, `herd.yml`, and `composer.json`. The fixed expiration also refreshes provider changes outside those files. A failed executable launch evicts the resolution. The backend timeout path keeps a valid PHP resolution and waits until the next scheduled probe to retry.

The daemon stops probe launches while cleanup is unconfirmed. It retains custody of the child handle and checks ownership and process-group membership before each retry. Eight nonblocking retries are spaced at least 250 ms apart. Queue current reports `polling.state = "degraded"` and a `cleanup` value of `retrying` or `blocked`. Exhausted custody remains fenced for that daemon instance.

Queue batch staging waits for SQLite outside the current-read and watcher lock. The final commit uses a zero SQLite busy timeout under the short publication lock. A failed commit rolls back the batch before any readings become visible. Opt-out uses the durable enablement token and a bounded SQLite wait. If the write cannot commit, the caller receives a retryable busy error and enabled intent stays intact. A project revision, opt-out, selector change, or watcher invalidation fences superseded batches.

A transient failure after a connection default has been resolved attaches to the last effective queue in the same observed configuration. Durable history records an unavailable reading for that concrete queue and current keeps the separately timestamped last known count. An observed recovery appends a fresh observation even within the same periodic slot. A missing connection, invalid default, or explicit configuration invalidation clears the previous default attribution. A failure before a new default is known stays on the unresolved selector.

Current polling stays on a ten-second cadence. Durable history records the first observation of each concrete series in a 30-second UTC slot, plus any observed first reading, availability change, failure-reason change, or change of resolved queue identity. A reading that meets both criteria is stored once. A depth-only change within a slot can be visible in current readings and absent from history, including brief spikes. The last observed identity and reason survive daemon restart, so a healthy restart with the same identity does not manufacture a transition. A timeout before the default has been resolved in a new daemon session belongs to the unresolved selector. Opt-out clears the observation checkpoint, and re-enabling records a new first observation.

In raw history, `coverage.periodic` marks a periodic observation. `initial`, `availability_changed`, `reason_changed`, and `identity_changed` describe why that row was also retained as a transition. In rollups, `pending.observations` counts distinct rows, `periodic_observations` counts periodic rows including failures, `samples` counts successful periodic rows, and `transition_observations` counts rows with at least one transition flag, including an initial observation. Periodic and transition counts can overlap. The mean, minimum, and maximum use only successful periodic rows. A complete five-minute bucket has up to ten periodic slots. Missing observations leave gaps.

Migration 007 labels older raw observations with `coverage.legacy`. Previously finalized ten-second rollups retain their original statistics, with `periodic_observations: null`, `legacy_observations` identifying the older rows, and `expected_samples: 30`. Rollups finalized under the new policy show `expected_samples: 10`, and any unfinalized legacy rows count only toward `observations` and `legacy_observations`.

History uses a separate sequence stream and cursor lineage from process metrics. Use optional symbolic connection and queue scope filters and a half-open Unix-seconds window of at most seven days. Raw data is retained for 24 hours. Finalized five-minute UTC buckets are retained for seven days, with counts of observations and successful samples. A missed probe can leave a gap. Hold the same scope and time bounds while paging. Retention returns `-32020` with a recovery cursor rather than silently dropping history. Start a fresh cursor when crossing resolutions. Pick a five-minute cutover at or after the raw retention floor and include only rollups ending at or before it.

## Validation and cost reproduction

Use disposable Composer installs with the exact framework versions. Composer flagged advisories on Laravel 12.50.0 during fixture setup. The exact older release is used only for this isolated compatibility test. The fixture grants SQLite read-only access at open and restricts the private Unix-socket Redis reader to `LLEN` and session selection. It does not require a Laravel application or Redis instance belonging to the developer.

```sh
cargo build --workspace --locked
python3 scripts/test-queue-metrics.py --laravel12 "$LARAVEL12_FIXTURE" --laravel13 "$LARAVEL13_FIXTURE" --alternate-php "$SECOND_PHP" --saturation
python3 scripts/benchmark-queue-metrics.py --scale --workload idle --candidate target/debug/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 90
caffeinate -i python3 scripts/benchmark-queue-metrics.py --soak --candidate target/debug/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 3600
python3 scripts/benchmark-queue-metrics.py --herd-only --laravel13 "$LARAVEL13_FIXTURE"
python3 scripts/benchmark-queue-metrics.py --storage-only
```

### Read-only, timeout, and lock-contention evidence

Both real Laravel versions produced the expected SQLite and Redis counts for ready, delayed, reserved, empty, and secondary queues. The fixture verified unchanged SQLite rows and Redis keys. Redis MONITOR showed probe `LLEN` calls alongside connection setup and the fixture's snapshot reads. The matrix also checked unsupported and missing adapters and cursor advancement.

An unresponsive Redis connection made its previous count stale while another project stayed fresh. The default queue's concrete history showed the timeout and the subsequent recovery. With SQLite deliberately blocking metric commits, the test checks queue-current, opt-out, watcher invalidation, and worker and scheduler operations. It also checks that an older batch cannot publish after watcher invalidation. The saturation fixture tests fairness with nine projects and rotation with 65 selectors in one project.

The restart fixture verifies the original cursor lineage and one periodic sample per series and slot across a healthy daemon restart. It then switches a disposable project's revision from Herd-selected PHP 8.4.28 to a real PHP 8.5.13 executable selected through the system provider, configures a missing override, and restores the Herd provider. The probe fixture records the resolved runtime's `PHP_VERSION`. The test updates the disposable SQLite project directly because there is no public provider edit method in this slice.

### Cumulative resource measurements

Working envelopes are 5% of one core for the daemon with queue polling disabled, 7 percentage points incremental for queue polling at capacity, and 12% combined. Daemon RSS has a 50 MB target and local IPC has a 100 ms p95 target. The comparison keeps 31 idle fixture workers and one scheduler active and varies queue opt-in across zero, one, two, four, and eight projects. Each opted-in project polls 32 selectors against real Laravel, SQLite, and a disposable Redis backend. Client reads run at the same cadence in each trial.

The harness measures daemon user and system CPU, CPU charged to reaped descendants including Herd and Laravel PHP, and live guardian and Redis CPU. It calibrates the Mach timebase against `getrusage`. It reports the cost of fixture Redis separately so it can be included in the comparison. The supervised applications' own workload CPU is outside the daemon and polling budget. PHP probes remain short-lived. A child that spans a measurement boundary can shift a small amount of CPU between windows.

The one-hour run exercises the same full-capacity workload. It injects a silent Redis socket every five minutes and verifies timeout, recovery, and actual Herd lookup after cache expiration. Five-minute checkpoints record CPU, sampled daemon RSS, SQLite footprint, and IPC latency. The measurement runs on Apple Silicon. The separate process CPU gauge correction is tracked in [PR #14](https://github.com/itguy614/roost/pull/14).

### Durable storage budget

The production cadence is 30 seconds plus transitions for 24 hours of raw data and seven days of five-minute rollups. The cumulative working envelopes, including SQLite journals, are 180 MiB for steady operation and 250 MiB for pathological churn. The seeded test fills 256 queue series and adds a full day of raw records and week of rollups for 32 managed processes. In the regular fixture, 16 queue selectors incur 96 total outage and recovery transitions. The flapping fixture switches every selector between success and timeout at each poll. Both fixtures exercise the retention SQL with a sampled rollback journal.

| Durable interval | Periodic slots per 5m | Queue raw rows | Database and sampled journal | Queue retention commit |
| --- | ---: | ---: | ---: | ---: |
| 10 seconds | 30 | 2,211,840 | 234.32 MiB | 27.36 ms |
| 30 seconds plus transitions | 10 | 737,328 | 163.05 MiB | 28.24 ms |
| 60 seconds plus transitions | 5 | 368,720 | 145.34 MiB | 26.64 ms |
| 30 seconds, all selectors flapping | 10 | 2,211,840 | 243.56 MiB | 31.07 ms |

Both production-cadence cases fit their working envelopes for the tested data. The all-selector flapping case leaves about 6.4 MiB of margin. The seeded measurements cover two specific churn patterns and a sampled journal peak. Changing resolved identities repeatedly can create additional series and rollups. Multi-day steady-state retention, storage recycling, and journal peaks remain Milestone 6 verification work.

### Compatibility expansion

Keep a Composer lockfile and recorded PHP version and Redis client for each future fixture. For each candidate patched Laravel tag, inspect the exact stock adapters, database query builder and Redis key derivation. Run the fixture against an isolated read-only SQL connection and an ACL-limited Redis account. Compare job rows and keys before and after, inspect query and command traces, and exercise default resolution, unsupported custom connectors, timeout, and invalid output. Record dependency advisories and the observed scope before proposing a new exact allowlist entry. The verified versions remain Laravel 12.50.0 and 13.43.0.

### Platform coverage

Physical sleep/wake, minimum macOS, Intel macOS, and launchd-managed execution remain separate release gates. Native notification authorization and presentation remain in Milestone 4.4.
