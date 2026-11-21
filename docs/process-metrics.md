# Process metrics

Roost samples the verified root processes of managed queue-worker and scheduler slots every two seconds on macOS. CPU uses user plus system time and reports 100% per fully used logical core. RSS is resident bytes. Uptime is measured in seconds on the Mach awake clock. Peak RSS is the largest RSS Roost has sampled for that process generation.

A logical slot is identified by project UUID, process kind, profile UUID and slot number. Each generation additionally has an instance UUID and generation number. The daemon checks the managed PID and process start identity around every OS read, then checks the slot again before publishing. Replacements start a new CPU baseline, uptime and peak. CPU is unknown on first sample, after a counter reset, or across a detected wake or sampling gap. A reading at least six seconds old is stale and has a null current value. An unavailable process or counter also yields null. The process summary reports sample coverage and can disclose a partial total. Missing and stale gauges never contribute as zero.

The project summary reports saved desired counts, live running counts, and unresolved leases separately for queue workers and the scheduler. A new generation counts as a restart when its lease is published as running and the logical slot has evidence of an earlier confirmed launch. Migration 005 credits one prior launch for each lease already recorded as running, so its first replacement counts as a restart. A slot with no such lease starts counting on its first confirmed launch after the upgrade. This omits pre-upgrade launches whose evidence was already removed or whose lease never confirmed running. The migration itself records zero restarts. Crashes count committed unexpected-exit reports. Both totals are durable from `lifecycle.counting_since` (migration 005, Unix seconds). Existing crash reports remain available through the diagnostics methods.

## History and polling

Use `metrics.process.history` with the project UUID, an optional profile UUID, explicit `raw` or `5m` resolution, and a half-open Unix-seconds range. Omit `to` to use the current second plus one. Use a range of at most seven days and pages from 1 to 256 records (default 128). Raw records carry an RFC 3339 `observed_at` and satisfy `from <= observed_at < to`. Five-minute results contain only finalized UTC buckets with a start at or after `from` and an end at or before `to`. Both queries apply the time and scope filters to the returned records inside the indexed sequence window. CPU and RSS rollups include mean, min, max and valid-sample counts against 150 expected observations for a full bucket. A missing bucket is a gap.

The result includes `items`, `next_cursor`, `has_more`, and the Unix-seconds `retention_floor`. Treat the cursor as opaque and repeat its project, profile, resolution and range parameters. Each scan captures a committed sequence watermark and pins its effective end time until all pages are drained. The completed cursor can poll again for newer records. Sequence values are decimal strings. Retention can conservatively expire a cursor at the stream watermark even if its filter would have skipped the removed records. A `-32020` error with `data.kind = "cursor_expired"` supplies `recovery_cursor` and `pruned_through`. Resume with the supplied cursor and the same parameters, or start a fresh scan. A database lineage change requires a fresh scan.

Raw samples are visible for 24 hours and rollups for seven days. To combine the two, choose a UTC five-minute boundary at or after the raw floor. Read finalized buckets ending at or before that boundary and raw readings at or after it, with a separate cursor for each. Empty intervals remain gaps. Use cursor polling over IPC v1. Subscriptions are deferred to Milestone 5.

## CLI demonstration

With a daemon running in a disposable state directory and a test Laravel worker or scheduler running, use the project's UUID from `project add`. The existing [scheduler walkthrough](scheduler.md) lists setup, launch, and cleanup commands.

```sh
roost --state-dir "$STATE" metrics process current "$PROJECT_ID"
roost --state-dir "$STATE" metrics project summary "$PROJECT_ID"
FROM=$(($(date +%s) - 600))
roost --state-dir "$STATE" metrics process history "$PROJECT_ID" --from "$FROM" --resolution raw --limit 8
roost --state-dir "$STATE" metrics process history "$PROJECT_ID" --from "$FROM" --resolution raw --limit 8 --cursor "$CURSOR"
```

Set `CURSOR` to the returned `next_cursor`. Restart the worker or scheduler and inspect the new identity and generation. The replacement initially has unknown CPU. Check `5m` after a UTC bucket boundary and a retention pass.

## Candidate storage measurements

Run from this checkout with Python 3.9 or newer. Build the baseline separately from commit `b396440` and use its debug daemon binary. The live fixture runs 31 workers and one scheduler, plus a scheduler restart halfway through each run. Loaded workers busy-loop for 2 ms and then sleep for 10 ms. Both daemons use debug builds. The default candidate client also polls current, summary and history.

```sh
cargo build --workspace --locked
python3 scripts/benchmark-process-metrics.py --baseline "$BASELINE_DAEMON" --candidate target/debug/roost-daemon --seconds 15
python3 scripts/benchmark-process-metrics.py --baseline "$BASELINE_DAEMON" --candidate target/debug/roost-daemon --seconds 15 --loaded
python3 scripts/benchmark-process-metrics.py --baseline "$BASELINE_DAEMON" --candidate target/debug/roost-daemon --seconds 12 --loaded --common-reads
python3 scripts/benchmark-process-metrics-storage.py
```

Measured on Apple M4 Pro (64 GB), macOS 26.6.2 build 25G83, Rust 1.96.0, SQLite 3.51.0 and Python 3.9.6. CPU values are daemon CPU seconds over the interval. Peak RSS excludes workers and guardians.

| Live run | Daemon CPU, baseline / candidate | Peak RSS, baseline / candidate | `process.list` p95, baseline / candidate |
| --- | ---: | ---: | ---: |
| Idle processes, 15 s | 0.72 / 1.94 s | 14,624 / 16,752 KiB | 9.85 / 11.56 ms |
| Loaded processes, 15 s | 0.16 / 0.59 s | 14,960 / 16,736 KiB | 2.47 / 2.51 ms |
| Loaded, identical read mix, 12 s | 0.14 / 0.15 s | 14,912 / 15,808 KiB | 2.50 / 2.58 ms |

The 15-second candidate runs retained 223 raw rows each. The 12-second run retained 192. Peak database plus journal growth was 16,384 bytes in each candidate run. Candidate metrics p95 for current, summary and history was 8.34, 5.06 and 19.04 ms in the idle-process run, and 1.81, 0.97 and 7.58 ms in the loaded run. The short IPC runs preceded the final seeded-history query optimization.

Seeding a full 32-process day produced 1,382,400 raw rows and added 94,662,656 bytes (68.5 bytes per row including indexes). Seven days of five-minute buckets added 5,689,344 bytes for 64,512 rows. The final SQL fetched 257 recent rows in 1.70–2.16 ms (ten reads). An accelerated transaction rolled up 4,800 samples, pruned 512 raw rows and 256 expired rollups, and committed in 9.95 ms. The file retained approximately 96 MiB after the transaction, with SQLite retaining freed pages for reuse. This isolated transaction did not include concurrent readers or writers.

### Concurrent retention

Use a separate private run directory for each live trial. The SQLite benchmark times production SQL against its own seeded rollback-journal database. The live benchmark runs four independent IPC history readers against the daemon while its sampler and retention loop run.

```sh
python3 scripts/benchmark-process-metrics-storage.py --concurrent
RUN=$(mktemp -d /private/tmp/roost-retention.XXXXXX)
python3 scripts/benchmark-process-metrics-reliability.py --candidate target/debug/roost-daemon --run-directory "$RUN" --seconds 75 --seed-retention
```

The 75.25-second live run seeded 1,382,400 raw rows and 64,512 rollups, with 32 managed processes across three projects. It completed 1,143 history reads, pruned 3,072 raw rows and 1,536 rollups, and persisted 1,215 live rows over 38 ticks. The longest persisted tick gap was two seconds. One induced worker crash produced one confirmed restart, and one explicit scheduler restart produced one confirmed replacement. All process-count checkpoints reported 32 running. The daemon used 11.41 CPU seconds, reached 22,160 KiB RSS, and grew the database and journal by 28,672 bytes at the sampled checkpoints.

| Live RPC | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| Raw history, broad window | 10.30 | 20.95 | 22.07 |
| Raw history, recent window | 10.25 | 21.18 | 21.72 |
| Rollup history, broad window | 14.14 | 21.58 | 22.94 |
| Rollup history, near cutover | 3.15 | 6.55 | 15.44 |
| Daemon status | 0.10 | 0.69 | 7.79 |
| Process list | 1.02 | 1.52 | 2.97 |
| Current metrics | 0.90 | 1.12 | 1.82 |
| Project summary | 0.50 | 1.46 | 4.55 |

The scheduler restart RPC returned in 0.41 ms. A separate idle soak was also running on the host during this trial.

On the seeded SQLite fixture, four readers completed 457 read transactions while ten cleanup commits took 9.39–16.61 ms each. There were zero busy errors in that mixed workload, and the main database did not grow during those batches. A deliberately held rollback-journal read blocked the writer. The failed batch returned busy after 74.72 ms end to end, including SQL work and the 50 ms SQLite busy budget. The transaction left the retention watermarks unchanged. After releasing the reader, the same batch committed in 11.19 ms.

### Sustained soak and platform coverage

```sh
RUN=$(mktemp -d /private/tmp/roost-soak.XXXXXX)
caffeinate -i python3 scripts/benchmark-process-metrics-reliability.py --candidate target/debug/roost-daemon --run-directory "$RUN" --seconds 86400 | tee "$RUN/report.jsonl"
```

The soak runs 31 queue workers and one scheduler across three disposable projects, with simultaneous live metric reads. It starts inducing crashes after the first minute, then every 30 minutes, and starts requesting scheduler replacements after two minutes, then every hour. It records progress every five minutes and emits final counters and latency percentiles after 24 hours. A detached run started on 2026-11-21 at 18:09 UTC. The 24-hour result and storage and CPU budget sign-off remain pending.

These measurements cover an Apple M4 Pro on macOS 26.6.2 with directly launched daemon fixtures. Fake-clock tests cover detected sampling gaps. Physical sleep/wake, launchd-managed operation, Intel macOS, and the proposed macOS 13 minimum remain unverified. Queue backend and timeout-contention measurements belong to the queue adapter slice.
