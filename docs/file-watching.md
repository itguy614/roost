# File watching and targeted restarts

Enable watching per project and select `on_change` for each worker profile that should restart. Both settings are persisted using the existing project and profile revisions. Stop the project's queue workers and wait for cleanup before changing a profile policy. Watching can be enabled or disabled while workers run. Settings owned by a project file return a conflict.

## macOS walkthrough

Build the workspace and start `target/debug/roost-daemon` in a separate terminal. Register a disposable Laravel app, choose a queue connection available in that app, and pick an unused queue. Use the project UUID and revision returned by `project add` and the worker UUID and revision returned by `worker profile create`. If using an isolated daemon, pass the same absolute `--state-dir` to both binaries.

```sh
cargo build --workspace --locked
target/debug/roost-daemon
```

```sh
target/debug/roost project add /path/to/disposable/laravel
target/debug/roost worker profile create PROJECT_UUID WatchDemo CONNECTION roost-watch-demo
target/debug/roost worker profile set-code-restart PROFILE_UUID on-change --revision PROFILE_REV
target/debug/roost watch enable PROJECT_UUID --revision PROJECT_REV
target/debug/roost watch status PROJECT_UUID
target/debug/roost project start PROJECT_UUID
target/debug/roost process list PROJECT_UUID
```

Wait for `watching` and a running worker. Save the worker PID and generation from `process list`. From the repo root in the CLI terminal, replace a new fixture file atomically inside your disposable app.

```sh
app=/path/to/disposable/laravel
printf '%s\n' '<?php // Roost watcher demonstration' > "$app/RoostWatchDemo.tmp"
mv "$app/RoostWatchDemo.tmp" "$app/RoostWatchDemo.php"
target/debug/roost process list PROJECT_UUID
```

Poll until the worker returns to `running` with a new PID and an incremented generation. Use `project stop` and wait for `stopped`, then remove the fixture file. The worker should remain stopped. Use the current revision from `watch status` to disable the watcher.

```sh
target/debug/roost project stop PROJECT_UUID
target/debug/roost process list PROJECT_UUID
rm "$app/RoostWatchDemo.php"
target/debug/roost watch status PROJECT_UUID
target/debug/roost watch disable PROJECT_UUID --revision CURRENT_PROJECT_REV
```

## Matching and recovery

The default includes are PHP files, root `.env`, `artisan`, `composer.json`, and `composer.lock`. A nonempty `include` list replaces these patterns. Use `--rules-json '{"include":["config/**/*.php",".env"],"exclude":["config/generated/**"]}'` to customize them. Added exclusions take precedence over includes. Built-in exclusions cover `.git`, `vendor`, `node_modules`, `build`, `dist`, `target`, `storage/logs`, and `storage/framework`. The watcher skips symlinks and stops at other registered project roots. Shared paths with explicit project mappings are part of the groups slice.

On macOS, FSEvents asks Roost to reconcile the project tree. Roost compares file metadata and does periodic reconciliation every two seconds as a fallback for missed notifications, overflow, backend errors, and wake. A new watch starts from a fresh baseline. A failed scan retains the previous baseline and reports degraded status. Discovery of changes uses metadata rather than file contents. Edits that leave no metadata difference at reconciliation are outside its coverage. Filesystem traversal is bounded to 16 enabled projects, 32,768 entries inspected and 8,192 matched files per project, and a depth of 64. Watch status reports an exceeded scan limit.

Changes are batched until 500 ms of quiet or two seconds from the first observed change. Only a currently running, opted-in queue worker is eligible. The supervisor sends a graceful stop and waits for the guardian and lease cleanup before starting the same slot again. A manual project stop cancels a queued replacement. Disabling watching discards undelivered batches. A restart already accepted by the supervisor can complete. The existing crash policy governs unrelated exits. Scheduler processes have their own lifecycle and are unaffected by these file changes.
