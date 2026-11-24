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

Probes have a two-second slot budget including Herd PHP resolution, PHP execution, and cleanup. Two projects can probe in parallel, with one in-flight probe per project and at most eight admissions in a ten-second round. The rotating project order and selector offsets continue across rounds. Each batch covers at most 32 selectors, and total stdout and stderr are capped at 16 KiB. The daemon stops new launches while the process runner has unconfirmed cleanup. Worker and scheduler supervision is independent of the queue polling lock. A probe invalidated by project revision, opt-out, profile selector changes or the watcher is dropped before publication.

History uses a separate sequence stream and cursor lineage from process metrics. Use optional symbolic connection and queue scope filters and a half-open Unix-seconds window of at most seven days. Raw data is retained for 24 hours. Finalized five-minute UTC buckets are retained for seven days, with counts of observations and successful samples. A missed probe can leave a gap. Hold the same scope and time bounds while paging. Retention returns `-32020` with a recovery cursor rather than silently dropping history. Start a fresh cursor when crossing resolutions. Pick a five-minute cutover at or after the raw retention floor and include only rollups ending at or before it.

## Validation and cost reproduction

Use disposable Composer installs with the exact framework versions. Composer flagged advisories on Laravel 12.50.0 during fixture setup. The exact older release is used only for this isolated compatibility test. The fixture grants SQLite read-only access at open and restricts the private Unix-socket Redis reader to `LLEN` and session selection. It does not require a Laravel application or Redis instance belonging to the developer.

```sh
cargo build --workspace --locked
python3 scripts/test-queue-metrics.py --laravel12 "$LARAVEL12_FIXTURE" --laravel13 "$LARAVEL13_FIXTURE" --saturation
python3 scripts/benchmark-queue-metrics.py --baseline "$BASELINE_DAEMON" --candidate target/debug/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 32
python3 scripts/benchmark-queue-metrics.py --herd-only --laravel13 "$LARAVEL13_FIXTURE"
python3 scripts/benchmark-queue-metrics.py --storage-only
```

The end-to-end matrix recorded three filtered SQLite counts on each framework version and verified unchanged database rows and Redis queues. It verified ready, delayed and reserved semantics, a second queue and measured zero for an empty queue, unsupported and missing adapters, and cursor progression. A Redis connection that never answered authentication produced a stale result with the previous count while another project stayed fresh. A queue worker and scheduler stayed running through the test. Nine hung projects entered their first probes within 10.02 seconds of each other, with eight admitted in the first round and the ninth in the next. All 65 selectors of the large project were visited. The highest supervision status latency during that fixture was 13.25 ms. In the separate timeout-isolation phase, the stale result became visible 1.44 seconds after PHP bootstrap began. That interval includes backend waiting, process-group cleanup, and observation by the client.

Physical sleep/wake, minimum macOS, Intel macOS, and launchd-managed execution remain separate release gates.

### Cumulative resource measurements

Apple M4 Pro, macOS 26.6.2, Rust 1.96.0, PHP 8.4.28, Redis 7.4.7 and system SQLite 3.51.0. The paired debug builds compared the merged Milestone 4.1 daemon with the queue candidate. Both supervised 31 fixture workers and one scheduler. The candidate additionally probed eight Laravel projects with 32 configured database and Redis selectors per project and served a queue-current read in each IPC cycle. Trials lasted about 32 seconds. CPU excludes the supervised workers and guardian processes.

| CPU and RSS | 4.1 idle | 4.2 idle | 4.1 loaded | 4.2 loaded |
| --- | ---: | ---: | ---: | ---: |
| Daemon CPU seconds | 0.91 | 1.05 | 0.20 | 0.32 |
| Queue-count PHP CPU seconds | 0 | 1.225 | 0 | 1.352 |
| Daemon plus queue-count PHP, % of one core | 2.79% | 6.98% | 0.62% | 5.19% |
| Sampled peak daemon RSS, KiB | 15,632 | 17,264 | 15,872 | 16,784 |
| Highest single PHP count-probe RSS, MiB | — | 46.36 | — | 46.69 |

Each enabled trial observed 24 PHP counts and grew the database plus journals by 73,728 bytes at the sampled checkpoints, versus 32,768 bytes in each baseline. The longest persisted process sample gap stayed at two seconds. Queue-current p95 was 5.00 ms idle and 1.12 ms loaded. Process-list p95 was 12.19 ms idle and 2.07 ms loaded with queue polling active.

The measured combined CPU figures include the daemon and Laravel count PHP sampled at probe shutdown. They are lower bounds on total queue-polling CPU. In a separate run, 24 Herd PHP resolutions consumed another 3.109 CPU seconds, with 234.33 ms p95 wall time and a 37.86 MiB peak child RSS. The isolated lookup measurement describes an additional cost rather than a simultaneous peak or an exact end-to-end total. The measured full-polling fixture already exceeds the 5% working envelope against which Milestone 4.1 was reviewed.

The accelerated full-capacity fixture added 108,191,744 bytes for 2,211,840 queue raw rows and 28,450,816 bytes for 516,096 queue rollups. Together with the retained process fixture, SQLite allocated **226.26 MiB**. An isolated queue maintenance transaction that finalized a bucket and pruned both streams committed in 27.96 ms. The exact raw history statement fetched 257 records in 2.36 ms for a recent range and 2.26 ms for a full-day range. These are seeded measurements. Multi-day steady-state file reuse remains a Milestone 6 test.

The combined CPU and storage budgets require review before sign-off. The 4.1 process-only storage envelope was 120 MiB. This candidate preserves the approved cadence and retention for review with the measured costs.
