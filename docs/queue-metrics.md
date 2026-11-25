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

A transient failure after a connection default has been resolved attaches to the last effective queue in the same observed configuration. Durable history records an unavailable reading for that concrete queue and current keeps the separately timestamped last known count. Recovery appends a new fresh observation. A missing connection, invalid default, or explicit configuration invalidation clears the previous default attribution. A failure before a new default is known stays on the unresolved selector.

History uses a separate sequence stream and cursor lineage from process metrics. Use optional symbolic connection and queue scope filters and a half-open Unix-seconds window of at most seven days. Raw data is retained for 24 hours. Finalized five-minute UTC buckets are retained for seven days, with counts of observations and successful samples. A missed probe can leave a gap. Hold the same scope and time bounds while paging. Retention returns `-32020` with a recovery cursor rather than silently dropping history. Start a fresh cursor when crossing resolutions. Pick a five-minute cutover at or after the raw retention floor and include only rollups ending at or before it.

## Validation and cost reproduction

Use disposable Composer installs with the exact framework versions. Composer flagged advisories on Laravel 12.50.0 during fixture setup. The exact older release is used only for this isolated compatibility test. The fixture grants SQLite read-only access at open and restricts the private Unix-socket Redis reader to `LLEN` and session selection. It does not require a Laravel application or Redis instance belonging to the developer.

```sh
cargo build --workspace --locked
python3 scripts/test-queue-metrics.py --laravel12 "$LARAVEL12_FIXTURE" --laravel13 "$LARAVEL13_FIXTURE" --saturation
python3 scripts/benchmark-queue-metrics.py --baseline "$BASELINE_DAEMON" --candidate target/debug/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 140
# Add --uncached "$UNREVISED_QUEUE_DAEMON" for a short comparison against 33258d8.
python3 scripts/benchmark-queue-metrics.py --herd-only --laravel13 "$LARAVEL13_FIXTURE"
python3 scripts/benchmark-queue-metrics.py --storage-only
```

### Read-only, timeout, and lock-contention evidence

Both real Laravel versions produced the expected SQLite and Redis counts for ready, delayed, reserved, empty, and secondary queues. The fixture verified unchanged SQLite rows and Redis keys. Redis MONITOR showed probe `LLEN` calls alongside connection setup and the fixture's snapshot reads. The matrix also checked unsupported and missing adapters and cursor advancement.

An unresponsive Redis connection made its previous count stale while another project stayed fresh. The default queue's concrete history showed the timeout and the subsequent recovery. The fixture then changed the default while SQLite deliberately blocked metric commits. Queue-current took 0.55 ms and the worker and scheduler reads took 0.73 ms while the read lock was held. Watcher invalidation took 108.67 ms. The superseded batch stayed invisible. Opt-out reported busy in 76.48 ms and succeeded on retry using the same idempotency key. A scheduler restart crossed the deliberate 60 ms lock release and acknowledged in 73.56 ms. The newly resolved default published under its own queue after the lock released. Nine hung projects received their first probes within 9.98 seconds of each other. The first round admitted eight, the ninth started in the next round, and all 65 selectors of the largest project were visited. The maximum supervision status latency under saturation was 4.61 ms.

### Cumulative resource measurements

The paired tests ran on an Apple M4 Pro with macOS 26.6.2, PHP 8.4.28, Redis 7.4.7, and SQLite 3.51.0. Each run supervised 31 workers and one scheduler and made process metric reads every two seconds. Queue-enabled runs also served queue-current reads and polled eight Laravel projects with 32 selectors each. Idle pairs and loaded pairs used roughly 140 seconds per build. The uncached comparison used about 60 seconds. The live integration and storage fixtures overlapped parts of the original idle pair. The final cached idle confirmation ran after those fixtures and the loaded pair ended.

CPU sums include daemon user and system time and all CPU charged to its reaped descendants during the measurement interval. This includes Laravel PHP and Herd resolution and any descendants they reap. The fixture kept workers and guardians running throughout each measured window. Child processes straddling a measurement boundary can shift a small amount of CPU between windows. Per-probe PHP shutdown measurements give a separate view of the count command. On this host the calibrated benchmark converts Mach ticks using the 125/3 timebase and checks the resulting child accounting against `getrusage`.

| Run | Seconds | Daemon CPU s | Reaped child CPU s | Laravel count CPU s | Total % of one core | Peak daemon RSS, KiB | SQLite growth, bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Idle, 4.1 baseline | 142.03 | 5.58 | 0.00 | 0.00 | 3.93% | 17,024 | 143,360 |
| Idle, prior uncached queue build | 61.54 | 2.38 | 9.07 | 2.40 | 18.60% | 18,528 | 131,072 |
| Idle, cached paired candidate | 141.76 | 5.47 | 7.62 | 6.33 | 9.23% | 18,784 | 315,392 |
| Idle, final cached confirmation | 140.03 | 5.99 | 7.69 | 6.39 | 9.77% | 18,144 | 290,816 |
| Loaded, 4.1 baseline | 140.17 | 2.15 | 0.00 | 0.00 | 1.53% | 17,184 | 135,168 |
| Loaded, final cached candidate | 140.20 | 2.50 | 7.47 | 6.16 | 7.11% | 18,240 | 286,720 |

| Final candidate RPC | Idle p50 / p95 / p99 (ms) | Loaded p50 / p95 / p99 (ms) |
| --- | ---: | ---: |
| `daemon.status` | 0.92 / 2.05 / 2.42 | 0.26 / 2.12 / 2.21 |
| `process.list` | 8.59 / 11.02 / 14.17 | 2.62 / 4.21 / 4.49 |
| `metrics.process.current` | 6.86 / 11.90 / 14.76 | 2.43 / 3.81 / 4.37 |
| `metrics.project.summary` | 4.12 / 6.36 / 8.19 | 1.24 / 2.33 / 3.49 |
| `metrics.process.history` | 22.94 / 26.40 / 27.75 | 12.81 / 17.49 / 18.47 |
| `metrics.queue.current` | 1.89 / 2.07 / 2.13 | 1.04 / 2.24 / 3.57 |

The final idle and loaded runs each completed 112 Laravel probes, with a two-second maximum persisted process tick gap. Peak single Laravel PHP RSS was about 46.4 MiB idle and 46.6 MiB loaded. The daemon stayed below 50 MB RSS. SQLite growth is measured from the start of each short trial, including journal files at sampled checkpoints.

**Budget decision:** keep the CPU gate open. Caching reduced the idle comparison from 18.60% to 9.23% in the paired trial and 9.77% in the final confirmation. The loaded pair measured 7.11% with probes. Both final workloads exceed the 5% working envelope at eight enabled projects. The storage proposal below also remains open for approval.

The production process-metric sampler has a separate pre-existing unit issue on this host. A control CPU loop changed the raw self counter by 7,427,925 while `getrusage` reported about 0.309 seconds. Applying the Mach scale yields 0.309 seconds. Treating those counts as nanoseconds yields 0.0074 seconds. The budget measurements here use the calibrated conversion. Correcting the exported process CPU gauge is a separate Milestone 4.1 follow-up.

### History frequency options for approval

The runnable implementation continues to poll and persist every ten seconds and keeps raw observations for 24 hours and five-minute rollups for seven days. The options below are a seeded experiment with existing schema and indexes. They keep ten-second polling for current readings and vary only the durable sampling interval. The fixture fills 256 queue series and retains the Milestone 4.1 process streams. It stages three brief availability incidents on 16 selectors. All 96 outage and recovery transitions are present in each option.

| Durable interval | Periodic points per 5m bucket | Queue raw rows | Cumulative seeded database | Retention commit |
| --- | ---: | ---: | ---: | ---: |
| 10 seconds | 30 | 2,211,840 | 227.93 MiB | 27.69 ms |
| 30 seconds plus transitions | 10 | 737,328 | 158.06 MiB | 26.56 ms |
| 60 seconds plus transitions | 5 | 368,720 | 140.70 MiB | 25.54 ms |
| 30 seconds, all selectors flapping at every poll | 10 plus transitions | 2,211,840 | 234.54 MiB | 26.93 ms |

Ten-second storage preserves every admitted observation. Thirty- and sixty-second periodic storage retain one third and one sixth of the regular depth coverage. Between durable points, a brief depth-only spike can disappear from charts. In both sparse options, record the first observation, availability or failure-reason changes, concrete queue identity changes, and recovery at the actual poll time. A periodic sample coinciding with a transition is stored once. Distinguish periodic coverage from transition rows in the rollup so a brief outage or recovery cannot inflate coverage or bias periodic aggregates. Missed admissions remain gaps. Opt-out and configuration changes close the prior context without forwarding its last value to a new default.

**Proposal for review:** use 30-second periodic persistence plus transitions, with a 180 MiB cumulative steady-state working budget and a separate 250 MiB stress envelope for pathological churn, including SQLite journals. The regular and all-flapping fixtures use 158.06 MiB and 234.54 MiB of database pages respectively. The remaining margin is a proposal for journals and deployment variation. Sustained all-selector churn returns to roughly ten-second write volume. Multi-day storage recycling and journal peaks remain Milestone 6 verification work. Changing the durable cadence and rollup coverage contract requires approval.

### Compatibility expansion

Keep a Composer lockfile and recorded PHP version and Redis client for each future fixture. For each candidate patched Laravel tag, inspect the exact stock adapters, database query builder and Redis key derivation. Run the fixture against an isolated read-only SQL connection and an ACL-limited Redis account. Compare job rows and keys before and after, inspect query and command traces, and exercise default resolution, unsupported custom connectors, timeout, and invalid output. Record dependency advisories and the observed scope before proposing a new exact allowlist entry. The verified versions remain Laravel 12.50.0 and 13.43.0.

### Platform coverage

Physical sleep/wake, minimum macOS, Intel macOS, and launchd-managed execution remain separate release gates. Native notification authorization and presentation remain in Milestone 4.4.
