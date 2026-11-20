# Laravel scheduler

Roost supports one managed `php artisan schedule:work` process per registered project. Scheduler configuration and desired state are separate from queue-worker configuration and intent. Configure a scheduler and explicitly start it. Laravel may run the registered app's scheduled tasks, so use a trusted development project.

## CLI demo on macOS

In Terminal 1, from the repository root, build Roost and start the daemon. Leave it running after it prints `Roost IPC is ready`.

```sh
cargo build --workspace --locked
target/debug/roost-daemon
```

In Terminal 2, from the same repository, register a trusted development Laravel project. Use the UUID in the registration response's `result.id`, or an existing project's UUID from `project list`.

```sh
target/debug/roost daemon status
target/debug/roost project add /path/to/laravel
target/debug/roost scheduler configure PROJECT_UUID
target/debug/roost scheduler start PROJECT_UUID
target/debug/roost scheduler status PROJECT_UUID
target/debug/roost scheduler logs PROJECT_UUID
target/debug/roost scheduler restart PROJECT_UUID
target/debug/roost scheduler status PROJECT_UUID
target/debug/roost scheduler stop PROJECT_UUID
target/debug/roost scheduler status PROJECT_UUID
target/debug/roost scheduler crashes PROJECT_UUID
```

Poll status for `running` after start and restart, then `stopped` after stop. Compare the scheduler PIDs in `process.identity.pid` across the scheduler restart. If using a custom state directory, pass the same absolute `--state-dir` to both binaries. Scheduler start and restart accept the same PHP resolution options as the worker CLI. Pass a PHP path directly, opt into system PHP with `--provider system`, or use the configured provider.

Configuration defaults to enabled, `on_failure`, and a 15-second graceful stop. To change an existing scheduler, stop it and wait for cleanup. Read its profile revision from `scheduler status`, then pass that revision. Omitted options use the defaults.

```sh
target/debug/roost scheduler configure PROJECT_UUID --revision 1 \
  --options-json '{"enabled":true,"crash_restart":"always","graceful_stop_seconds":30}'
```

## Ownership and recovery

The scheduler uses the same guardian, process-group verification, graceful stop and escalation, restart backoff, and bounded redacted output as queue workers. SQLite permits one scheduler lease per project and preserves that fence until the guardian confirms cleanup. A restart is queued behind the previous process group's cleanup. An unconfirmed old lease blocks replacement and appears in scheduler status.

Scheduler start, stop, and restart acknowledge durable intent before OS transitions finish. A separate scheduler intent stores the resolved PHP path. A stopped scheduler remains stopped after daemon restart, including for a project with autostart enabled. Autostart can resume saved running intent only for opted-in projects. The `project start`, `project stop`, and `project restart` commands continue to control queue workers. Scheduler status, logs, and crash reports have their own commands. The daemon's capacity of 32 supervised instances covers both process types. Roost's uniqueness guarantee covers processes it owns.

The scheduler integration tests use an executable fixture that checks the exact Artisan arguments and exercises graceful stop, concurrent starts, and real daemon crash recovery without executing any Laravel jobs.

```sh
cargo test --locked -p roost-daemon --test worker_ipc scheduler
```
