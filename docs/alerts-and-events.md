# Crash inspection, resource alerts, and events

Milestone 4.3 uses the daemon's private v1 IPC connection. Crash inspection reads the already committed, redacted report. `output_state` is `captured` when stored output is available and `unavailable` when the report has metadata only. An intentionally stopped process or a Laravel job exception that leaves the process alive does not create a crash report. Crash history retains up to 12 reports per project and 128 globally in each process stream. A durable event may outlive the report it references.

Events carry stable IDs and symbolic identifiers, without application output or credentials. Crash events commit with the report, crash counter, and lease release. Terminal launch, safety custody, and retry-exhaustion failures of a supervised slot emit `permanent_recovery_failure` events. The daemon retries a failed event write while that slot remains in custody. Resource episodes and their events commit together. The most recent 2,048 events share a durable sequence stream. Use the same optional project scope when paging and polling a cursor. An expired cursor returns error `-32020` with `pruned_through`, a `recovery_cursor`, and `earliest_retained` containing the first retained global sequence and its Unix timestamp `at`. The boundary describes the global stream even when a project filter matches fewer records. Reuse the original filter with the recovery cursor. Keep any missed interval visible in clients. New v1 clients use pagination until `has_more` is false, then poll with `next_cursor`. Event subscriptions remain a Milestone 5 contract.

Set one CPU and one RSS rule per worker profile. CPU percentages use 100% per logical core and can exceed 100. RSS uses integer bytes. `restart` defaults to false. Each owned worker generation evaluates its rules independently. Three consecutive fresh observations at or above `breach_at` open an episode. Three at or below `clear_at` close it. Missing or stale evidence resets the streak. Any failed evaluation cycle clears all in-memory breach and recovery streaks, including streaks visited before the failure. Committed episodes, closures, and attempt charges remain durable. The daemon dispatches actions only after the whole cycle succeeds and revalidates queued decisions under the supervisor lock. A committed charge from a failed cycle is never replayed. History is for inspection and is never fed into alert evaluation. Episode history preserves the rule revision, triggering sample value and time, and closure reason. Rule edits close affected episodes and require new samples. No-op or notification-only updates preserve existing rules and episodes. A daemon restart closes previous active episodes and keeps the cooldown and attempt history.

The five-minute cooldown is per profile, slot, and metric. During cooldown, new episodes are retained with `actions_suppressed` set. A verified elapsed-time clock enforces cooldowns and the rolling hour. It advances only by monotonic intervals that agree with the wall clock to within its one-second precision, up to 15 seconds per interval. A clock jump or longer sampling gap contributes no time and resets streaks. The daemon resumes from the last elapsed value committed with an episode, so downtime and uncheckpointed progress give no eligibility credit. Legacy charges receive a full hour and legacy cooldowns five minutes after migration. Event and evidence timestamps remain wall-clock display data. A backward wall step can delay evaluation of rules whose configured timestamp is now in the future.

Resource restarts require opt-in, a current matching lease and rule, and running intent. Simultaneous CPU and RSS observations both commit an episode. CPU has the first opportunity to charge a restart. If it charges one, the RSS episode remains inspectable with restart state `other_metric` and consumes no attempt. One attempt per episode and two per profile slot per verified rolling hour are charged durably. An interruption or stale evidence between charge and dispatch can consume an attempt without restarting the worker.

Alert transactions stage outside the supervisor lock. Final commit and revalidation under that lock use a zero-wait SQLite connection. If a database lock blocks a manual stop or rule update, the RPC returns a retryable busy error and leaves saved intent and policy intact. Once a manual stop commits, it cancels any pending replacement, including while guardian cleanup is underway. `notifications` is a durable profile opt-in for the later native helper.

## Disposable CLI demo

Run from the repository root on macOS with Rust and `jq` available. The executable is a test fixture that prints a redacted error and exits when requested. It uses a private temporary state directory.

```sh
cargo build --workspace --locked
root=$(mktemp -d /private/tmp/roost-alerts.XXXXXX)
rustc --edition=2024 crates/roost-daemon/tests/fixtures/queue_worker.rs -o "$root/php"
mkdir "$root/app"
printf trace_fail > "$root/app/artisan"
target/debug/roost-daemon --state-dir "$root/state" > "$root/daemon.log" 2>&1 &
daemon=$!
trap 'kill "$daemon" 2>/dev/null; wait "$daemon" 2>/dev/null' EXIT
roost() { target/debug/roost --state-dir "$root/state" "$@"; }
wait_for() {
  for attempt in $(seq 100); do
    "$@" && return
    sleep 0.2
  done
  return 1
}
ready() { roost daemon status >/dev/null 2>&1; }
wait_for ready
project=$(roost project add "$root/app" | jq -r .result.id)
profile=$(roost worker profile create "$project" Demo database --options-json '{"crash_restart":"never","graceful_stop_seconds":1,"queues":["critical","default"]}' | jq -r .result.id)
roost project start "$project" "$root/php"
crash_ready() { crash=$(roost worker crashes "$project" | jq -er '.[0] | select(.exit.code == 7) | .id'); }
wait_for crash_ready
roost worker crash get "$crash"
roost event history --project "$project"
roost project stop "$project"
printf graceful > "$root/app/artisan"
revision=$(roost metrics thresholds get "$profile" | jq -r .revision)
roost metrics thresholds update "$profile" --revision "$revision" --rules-json '[{"metric":"rss_bytes","clear_at":0,"breach_at":1,"restart":true}]'
roost project start "$project" "$root/php"
alert_ready() { roost metrics alerts list "$project" | jq -e '.alerts[0].restart_charged' >/dev/null; }
wait_for alert_ready
roost project stop "$project"
stopped() { roost process list "$project" | jq -e 'length == 1 and .[0].state == "stopped"' >/dev/null; }
wait_for stopped
roost process list "$project"
roost metrics alerts list "$project"
```

## Matched resource comparison

The Apple M4 Pro fixture ran on macOS 26.6.2 with Herd PHP 8.4.28, Redis 7.4.7, and SQLite 3.51.0. Both arms use the same optimized daemon, eight enabled queue projects, 31 worker stand-ins, one scheduler, and the same IPC reads every two seconds, including alert and event reads. The harness applies two 64 MiB RSS excursions on different workers. The alert-enabled arm must commit two episodes, observe one recovery, and confirm one guardian-backed replacement. The alert-disabled arm must observe both excursions with zero episodes and replacements. CPU accounting includes the daemon, Herd, PHP, live guardians, and fixture Redis. In the enabled arm the reaped CPU also includes the retired guardian and any CPU carried from its stand-in worker.

```sh
cargo build --release --workspace --locked
python3 scripts/benchmark-queue-metrics.py --alert-comparison --alerts-first \
  --candidate target/release/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 120
```

| Optimized build `3bec357` | Elapsed | Combined CPU, one core | Peak daemon RSS | Worst local IPC p95 | Episodes / recovered / restarted |
| --- | ---: | ---: | ---: | ---: | ---: |
| Alerts enabled | 120.58 s | 10.89% | 14.92 MiB | 15.56 ms | 2 / 1 / 1 |
| Alerts disabled | 120.58 s | 10.74% | 15.02 MiB | 18.18 ms | 0 / 0 / 0 |

The enabled arm used 3.538 daemon CPU seconds, 5.851 reaped child seconds, a net 3.574 live guardian seconds, and 0.168 Redis seconds. The disabled arm used 3.522, 5.676, 3.588, and 0.163 seconds in the same categories. Both arms completed 96 PHP count probes. Combined CPU was 0.15 percentage points higher with alerts enabled in this pair. An earlier optimized pass with the opposite order and preliminary fixture measured 11.71% with alerts off and 11.30% with alerts on. The direction varied between short trials, so the difference is an observed cost for this workload. Both final arms met the combined CPU, RSS, and IPC targets.

SQLite and journals peaked at 577,536 bytes with alerts enabled and 565,248 bytes with alerts disabled in these fresh fixtures. Both runs had zero warning or error lines. One three-second process sample interval occurred with alerts disabled and still contained 32 rows. The enabled arm had no irregular process sampling ticks. A discarded trial used an allocator-backed RSS fixture that could retain memory after release. The final fixture unmaps its memory explicitly.

The isolated SQLite contention test held a shared reader across episode and attempt commit. Combined worker and scheduler status took at most 76.32 ms. Manual stop and rule update returned busy in 82.38 ms and 83.37 ms. Both succeeded using their original request keys after the reader released. The worker stopped in its original generation, its guardian confirmed cleanup, and the scheduler kept its process identity. No attempted charge or episode survived rollback.

Physical macOS sleep/wake, Intel, minimum macOS compatibility, and launchd operation remain release gates. Milestone 6 includes multi-day retention and storage recycling, journal peaks, and repeated resolved queue identity churn. Native authorization, OS acceptance, and Notification Center delivery remain acceptance gates for Milestone 4.4.
