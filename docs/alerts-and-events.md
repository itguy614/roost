# Crash inspection, resource alerts, and events

Milestone 4.3 uses the daemon's private v1 IPC connection. Crash inspection reads the already committed, redacted report. `output_state` is `captured` when stored output is available and `unavailable` when the report has metadata only. An intentionally stopped process or a Laravel job exception that leaves the process alive does not create a crash report. Crash history retains up to 12 reports per project and 128 globally in each process stream. A durable event may outlive the report it references.

Events carry stable IDs and symbolic identifiers, without application output or credentials. Crash events commit with the report, crash counter, and lease release. Terminal launch, safety custody, and retry-exhaustion failures of a supervised slot emit `permanent_recovery_failure` events. The daemon retries a failed event write while that slot remains in custody. Resource episodes and their events commit together. The most recent 2,048 events share a durable sequence stream. Use the same optional project scope when paging and polling a cursor. An expired cursor returns the prune watermark and a recovery cursor in error `-32020`. Keep any missed interval visible in clients. New v1 clients use pagination until `has_more` is false, then poll with `next_cursor`. Event subscriptions remain a Milestone 5 contract.

Set one CPU and one RSS rule per worker profile. CPU percentages use 100% per logical core and can exceed 100. RSS uses integer bytes. `restart` defaults to false. Each owned worker generation evaluates its rules independently. Three consecutive fresh observations at or above `breach_at` open an episode. Three at or below `clear_at` close it. Missing or stale evidence resets the streak. History is for inspection and is never fed into alert evaluation. Episode history preserves the rule revision, triggering sample value and time, and closure reason. Rule edits close affected episodes and require new samples. No-op or notification-only updates preserve existing rules and episodes. A daemon restart closes previous active episodes and keeps the cooldown and attempt history.

The five-minute cooldown is per profile, slot, and metric. During cooldown, new episodes are retained with `actions_suppressed` set. Resource restarts require opt-in, a current matching lease and rule, and running intent. If both rules request a restart in the same sample, CPU is evaluated first. When its restart starts cleanup, the RSS rule no longer has an eligible running slot. One attempt per episode and two per profile slot per rolling hour are charged durably before the existing guardian receives a stop request. An attempt can remain charged if the daemon stops or its evidence becomes stale before it can request a restart. A committed manual stop takes precedence, including while guardian cleanup is underway. `notifications` is a durable profile opt-in for the later native helper.

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

On a disposable workload, the cumulative benchmark enables two rules on all 31 workers, opens 31 RSS episodes, polls eight Laravel projects, and reads both alert and event IPC. The scheduler is included in supervision and process sampling. The benchmark keeps actual restart coverage in the independent IPC scenario above.

```sh
python3 scripts/benchmark-queue-metrics.py --candidate-only --thresholds --workload idle \
  --candidate target/debug/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 140
```

### Measured resource cost

The Apple M4 Pro fixture ran on macOS 26.6.2 with Herd PHP 8.4.28, Redis 7.4.7, and SQLite 3.51.0. CPU includes the daemon, its reaped probe subprocesses including Herd and PHP, live guardians, and fixture Redis. Each trial used eight enabled queue projects, 31 workers and one scheduler, 31 active alert episodes, and local IPC reads every two seconds.

| Build | Elapsed | Combined CPU, one core | Peak daemon RSS | Worst local IPC p95 |
| --- | ---: | ---: | ---: | ---: |
| Preliminary debug | 142.06 s | 13.62% | 18.48 MiB | 27.50 ms |
| Optimized, `f15c431` | 140.92 s | 11.30% | 15.36 MiB | 13.28 ms |

The optimized run used 4.036 daemon CPU seconds, 7.741 reaped child CPU seconds, 3.963 guardian CPU seconds, and 0.189 Redis CPU seconds. Alert listing p95 was 4.61 ms and event history p95 was 2.20 ms. SQLite and journals peaked at 638,976 bytes in the fresh fixture. The longest persisted process-sampling gap was two seconds and the daemon emitted zero warning or error lines. The debug trial exceeded the 12% combined CPU envelope. The optimized trial met the combined CPU, daemon RSS, and IPC p95 envelopes for this workload. The short trials leave steady-state storage and broader workload costs to the release gates.

Physical macOS sleep/wake, Intel, minimum macOS compatibility, and launchd operation remain release gates. Milestone 6 includes multi-day retention and storage recycling, journal peaks, and repeated resolved queue identity churn. Native authorization, OS acceptance, and Notification Center delivery remain acceptance gates for Milestone 4.4.
