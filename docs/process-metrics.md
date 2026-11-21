# Process metrics

Roost samples the verified root processes of managed queue-worker and scheduler slots every two seconds on macOS. CPU uses user plus system time and reports 100% per fully used logical core. RSS is resident bytes. Uptime is measured in seconds on the Mach awake clock. Peak RSS is the largest RSS Roost has sampled for that process generation.

A logical slot is identified by project UUID, process kind, profile UUID and slot number. Each generation additionally has an instance UUID and generation number. The daemon checks the managed PID and process start identity around every OS read, then checks the slot again before publishing. Replacements start a new CPU baseline, uptime and peak. CPU is unknown on first sample, after a counter reset, or across a detected wake or sampling gap. A reading at least six seconds old is stale and has a null current value. An unavailable process or counter also yields null. The process summary reports sample coverage and can disclose a partial total. Missing and stale gauges never contribute as zero.

The project summary reports saved desired counts, live running counts, and unresolved leases separately for queue workers and the scheduler. Confirmed launches after the first observed launch of each logical slot count as restarts. Crashes count committed unexpected-exit reports. Both totals are durable from `lifecycle.counting_since` (migration 005, Unix seconds). Existing crash records remain available through the existing diagnostics methods.

## History and polling

Use `metrics.process.history` with the project UUID, an optional profile UUID, explicit `raw` or `5m` resolution, and a half-open Unix-seconds range. Omit `to` to use the current second plus one. Use a range of at most seven days and pages from 1 to 256 records (default 128). Raw records carry an RFC 3339 `observed_at`. Five-minute results contain only finalized UTC buckets fully inside the range. CPU and RSS rollups include mean, min, max and valid-sample counts against 150 expected observations for a full bucket. A missing bucket is a gap.

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

The 24-hour soak, storage and CPU budget sign-off, live sleep/wake verification, and Intel macOS verification remain open. Queue-backend and timeout-contention measurements belong to the queue adapter slice. The current candidate uses system SQLite's rollback journal, so concurrent large history reads and ongoing retention are included in the remaining contention and soak review.
