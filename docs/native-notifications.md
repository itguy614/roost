# Native macOS notification helper

The logged-in user starts the windowless Swift helper independently of the daemon. It acquires one exclusive lock per canonical daemon state directory, before it prompts or connects. Closing a terminal or a future Roost main window has no effect on a helper started through Launch Services. A duplicate process exits at the lock. Stop the helper before replacing its development bundle. Reuse the bundle identifier, signing requirement, and private state directory to preserve permission eligibility and the journal.

The helper negotiates IPC v1 and checks the event and notification-policy capabilities on every connection. Unix frames, operation deadlines, and reconnect delays are bounded. An incompatible daemon pauses delivery. The helper reads one committed event at a time and durably stores the receipt before checking the current worker opt-in and submitting to UserNotifications. Worker crash, permanent recovery failure, and resource-threshold events have fixed, privacy-safe text. Scheduler events remain available in `event.history`. Scheduler notification opt-in has no settings contract in this slice.

The journal stores the bounded pending receipt, attempt count, cursor, last terminal outcome, and last cursor gap. A terminal outcome and the new cursor commit together with a private, synchronized atomic file replacement. A write failure before rename leaves the previous journal in place. If rename succeeds but directory sync fails, the running helper keeps its singleton lock and halts event polling, cursor recovery, and delivery. Status shows `journal_uncertain` until the helper is restarted. At startup it reopens the journal and syncs the directory before resuming so the on-disk cursor and attempts are authoritative. Startup fails closed on an unreadable, unsafe, unsupported, or unsynchronized journal. Expired history resumes from the daemon recovery cursor. A changed database lineage records a gap and starts a fresh retained scan. Retention gaps keep the earliest retained global sequence and timestamp. Events older than ten minutes are recorded as expired instead of being submitted.

Events are processed sequentially in cursor order, with at most one durable pending receipt. An eligible event with `undetermined` or `unknown` OS permission holds the cursor and delays later events without spending a submission attempt. The helper rechecks the event's eligibility and permission until it can resume, skip the event, or expire it at ten minutes. The status reports `waiting_for_authorization` for this wait, separately from the connection and permission. During the helper's explicit authorization request, macOS can temporarily report `denied` with the prompt still visible. The helper treats that report as undetermined until the request resolves. `operational` means the delivery loop can process events and terminal skips. It does not certify OS authorization. Missing, stale, and older status records report delivery as `unknown`.

Denial, opt-out, suppression, unavailable events, and unsupported events are terminal skips. Each submission attempt, including retries, requires a fresh daemon policy lookup and OS permission check. The native sink checks OS permission again just before invoking UserNotifications. A profile can opt out after the policy lookup while an already in-flight submission completes. The next retry observes the current policy. Transient submission failures reuse the same OS request identifier and receive at most three attempts, with backoff. A crash after OS acceptance but before the terminal journal write can consume an attempt and repeat the same request. Delivery is best effort. The journal distinguishes receipt from OS acceptance, and `roost notifications status` reports presentation as unknown. The development `--verify-center` option records whether macOS lists the matching request in Notification Center. Visual banner verification requires a separate observation.

## Development build and verification

Use a logged-in macOS session. Set `ROOST_NOTIFICATION_SIGN_IDENTITY` to a local development identity for permission continuity checks. Ad hoc signing is the development script's default and permission behavior can vary. Put the test bundle under `~/Applications`. The preceding spike found a location-dependent authorization failure in a temporary location on this host.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
scripts/test-notification-helper.sh
cargo build --workspace --locked
APP="$HOME/Applications/Roost Notifications.app"
scripts/build-notification-helper.sh "$APP"
ROOT_TEST=$(mktemp -d /private/tmp/roost-notifications.XXXXXX)
STATE="$ROOT_TEST/state"
target/debug/roost-daemon --state-dir "$STATE" > "$ROOT_TEST/daemon.log" 2>&1 &
DAEMON_PID=$!
roost() { target/debug/roost --state-dir "$STATE" "$@"; }
for i in $(seq 100); do roost daemon status >/dev/null 2>&1 && break; sleep 0.1; done
roost daemon status
LOG="$ROOT_TEST/helper.log"
touch "$LOG"
open -n -a "$APP" --stdout "$LOG" --stderr "$LOG" \
  --args --state-dir "$STATE" --request-permission --verify-center
roost notifications status
```

For a fresh permission test, explicitly choose Allow in the OS prompt and record the request result and authorization status. Repeat with a different unused `ROOST_NOTIFICATION_BUNDLE_ID` and a distinct app path, choosing Don’t Allow. A bundle that already reports `denied` at startup cannot establish a fresh prompt decision. Explicit permission requests occur only on a launch with `--request-permission` while authorization is undetermined.

```sh
rustc --edition=2024 crates/roost-daemon/tests/fixtures/queue_worker.rs -o "$ROOT_TEST/php"
mkdir "$ROOT_TEST/app"
printf trace_fail > "$ROOT_TEST/app/artisan"
PROJECT=$(roost project add "$ROOT_TEST/app" | jq -r .result.id)
PROFILE=$(roost worker profile create "$PROJECT" Demo database \
  --options-json '{"queues":["critical","default"],"crash_restart":"never"}' | jq -r .result.id)
REVISION=$(roost metrics thresholds get "$PROFILE" | jq -r .revision)
roost metrics thresholds update "$PROFILE" --revision "$REVISION" --rules-json '[]' --notifications on
roost project start "$PROJECT" "$ROOT_TEST/php"
roost event history --project "$PROJECT"
roost notifications status
cat "$LOG"
```

The event is committed once the worker exits. Poll status and the log for the same event ID. For an authorized app, confirm `submission=accepted` and the matching `Notification Center contains request` entry. For a denied app, expect a terminal `denied` result and no matching delivery. Repeat with the profile opted out to verify a terminal `opted_out` result. To exercise live settings changes, keep the authorized helper running. In System Settings → Notifications, turn off Allow Notifications for the test app. Confirm `permission=denied` and generate a fresh event, then look for a terminal `denied` outcome. Turn Allow Notifications back on and confirm authorization and acceptance of another fresh event with the same helper PID. Historical terminal results remain in the journal. For visual verification, select the banner presentation style for this app, turn off Focus, and observe the Roost worker crash banner while a fresh event is accepted. Record that separately from presence in Notification Center.

For reconnect evidence, stop the test daemon with `kill -TERM "$DAEMON_PID"`, wait for `connection=disconnected` in its helper log, then start the daemon again with the same state directory. The helper should show `connected` with the same PID. For a local helper upgrade, stop the test helper using the PID from its status after verifying the executable and state directory in `ps`. Build a replacement with the same bundle identity and signature and a higher `ROOST_NOTIFICATION_BUILD`, then launch it with the same state directory. Look for `journal resumed` and check that the old event is not resubmitted. After the exercise, stop the test daemons and helpers by their verified PIDs. Keep the private log directories until the evidence has been reviewed.

## Native evidence

On Apple Silicon macOS 26.6.2, a signed bundle under `~/Applications` reused the bundle identifier and designated requirement of the previously approved notification spike. The windowless helper reported `authorized`, durably recorded receipt of a real daemon worker-crash event, then recorded OS acceptance. The matching stable request was found in Notification Center. A different bundle identity on this host reported `denied` and durably skipped a real crash event without submitting it. A separate event with worker notifications disabled was skipped by the authorized helper.

A duplicate Launch Services invocation against the same state directory exited at the lock before requesting permission. Restarting the daemon produced a disconnect and reconnect on the same helper PID. Build 3 used the same bundle identifier and signing requirement, resumed the unchanged journal with a new PID, retained authorization, and submitted a newly committed crash event exactly once during the observed run. Notification Center contained the new request. The preserved journal cursor prevented resubmission of the earlier event. Running against an earlier v1 daemon without the notification-policy capability produced `incompatible`, sent no submissions, and preserved the copied journal byte for byte.

The review follow-up used separate, newly signed bundle identities in the logged-in macOS session. One fresh Allow prompt exposed a permission-reporting race: the prompt was visible and the outstanding OS request had yet to return, while settings temporarily reported denial. The corrected build kept a real committed crash at `waiting_for_authorization` on a second fresh identity. After choosing Allow, macOS returned `granted=true`, the helper became authorized and operational, and the same pending event advanced to OS acceptance and a matching Notification Center entry.

Fresh Don’t Allow remains open. The additional fresh prompt attempts returned `granted=true`. Earlier testing established that a helper which macOS reports as denied skips a committed crash. It did not record a fresh Don’t Allow choice. Live System Settings revocation and restoration also remain open. In the banner trial, macOS accepted the request and Notification Center contained it. Focus logged suppression for that request, and a capture of the banner area did not show the notification. Visual banner confirmation with Focus off remains open.

Formatting, strict Rust Clippy, 88 workspace tests and 11 Swift tests passed on this host. The Swift tests inject faults on both sides of the real journal rename, exercise unresolved permission and bounded retries, and verify fresh policy and permission checks before retries. The local development bundle passed strict signature verification.

## Cumulative resource comparison

Use an already authorized development bundle. The comparison keeps eight real Laravel pollers, 31 fixture workers, one scheduler, and the same alert workload in both optimized arms. It requires two fixture resource episodes, one recovery, and one guardian-backed worker restart in each arm. With the helper running, it additionally requires two native OS acceptances and two matching Notification Center entries. CPU includes the helper, daemon, PHP, Herd, guardian custody, and fixture Redis.

```sh
cargo build --release --workspace --locked
python3 scripts/benchmark-queue-metrics.py --notification-app "$APP" \
  --candidate target/release/roost-daemon --laravel13 "$LARAVEL13_FIXTURE" --seconds 120
```

### Measured cost

Both arms ran the optimized code at `a28fab8` on the Apple Silicon macOS 26.6.2 test host.

| Helper | Measured time | Combined CPU, one core | Peak daemon RSS | Peak helper RSS | Worst local IPC p95 | OS accepted / in Notification Center |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Off | 120.21 s | 10.96% | 15.06 MiB | — | 15.71 ms | 0 / 0 |
| On | 120.61 s | 11.84% | 15.27 MiB | 32.55 MiB | 16.29 ms | 2 / 2 |

The helper used 0.177 CPU seconds, or 0.15% of one core, in the measured window. The combined change was 0.89 percentage points in this pair. Other process costs varied between arms. CPU attribution covers the measured Roost, Herd, PHP, guardian and Redis processes. Shared macOS service CPU is outside that attribution. Helper status IPC p95 was 0.60 ms with delivery enabled. Both arms completed 96 Laravel PHP probes and had a two-second maximum persisted process sampling gap. SQLite and sampled journal footprint peaked at 573,440 bytes with the helper off and 619,120 bytes with it on. These fresh fixtures do not establish steady-state database size.

The helper-on run logged one `resource alert evaluation did not finish` warning from the pre-existing alert evaluator. The log does not identify the underlying cause. On an evaluation error, the evaluator drops transient breach and recovery streaks and suppresses automatic actions for that cycle. Durable episodes and charged attempts already committed remain intact. Both resource episodes, the recovery, the guardian restart, and both native deliveries completed. The helper-off arm had no daemon warnings or errors. All measured combined CPU, daemon RSS, and IPC p95 targets were met for this workload.

Distribution signing and notarization, physical sleep/wake, Intel, minimum macOS compatibility, and launchd operation remain release gates. Multi-day retention and storage recycling, journal peaks, and repeated queue identity churn remain Milestone 6 work. The Swift Testing runner on the Command Line Tools host links a newer testing framework than the helper's macOS 13 deployment target. Host tests do not establish support for the proposed minimum version.
