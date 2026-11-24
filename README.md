# Roost

Roost is a macOS-first supervisor for local Laravel queue workers and schedulers. The Rust CLI and per-user daemon register Laravel projects and supervise queue workers. The daemon owns SQLite and serves versioned JSON-RPC over a private Unix socket.

## Developer setup

Use a regular macOS user account, Apple Command Line Tools, and a Rust toolchain with Cargo, rustfmt and Clippy. Milestone 1 was built with Rust 1.96 on Apple Silicon. The build links the SQLite library from the macOS SDK. Worker supervision requires a PHP executable and a Laravel app. Registration and the automated tests run with the Rust toolchain and Apple Command Line Tools.

```sh
xcode-select --install  # If Apple Command Line Tools are missing
rustup component add rustfmt clippy
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Run the tests in a session that permits filesystem Unix socket binds. The workspace also tests the protocol over connected Unix socket pairs, including malformed input and concurrent clients.

## Smoke test

From the repository root, run these commands in one terminal. This uses an isolated state directory and a fixture that exercises the `artisan` file check without requiring a Laravel runtime.

```sh
smoke_dir="$(mktemp -d /tmp/roost-smoke.XXXXXX)"
mkdir "$smoke_dir/app"
touch "$smoke_dir/app/artisan"
target/debug/roost-daemon --state-dir "$smoke_dir/state" &
roost_pid=$!
```

After the daemon prints `Roost IPC is ready`, run the following from the same terminal. Successful commands print JSON. Copy the project UUID from the add response's `result.id` into `PROJECT_UUID` below.

```sh
target/debug/roost --state-dir "$smoke_dir/state" daemon status
target/debug/roost --state-dir "$smoke_dir/state" project add "$smoke_dir/app"
target/debug/roost --state-dir "$smoke_dir/state" project list
target/debug/roost --state-dir "$smoke_dir/state" project remove PROJECT_UUID
target/debug/roost --state-dir "$smoke_dir/state" project list
kill -TERM "$roost_pid"
wait "$roost_pid"
```

The first list contains one record and the final list is `[]`. Adding `"$smoke_dir"` itself returns an invalid-params error because it has no `artisan` file. Add a real app by passing its Laravel root directory.

The default macOS state directory is `~/Library/Application Support/Roost`. For an isolated instance or if the default socket path exceeds macOS's Unix socket length limit, pass the same short absolute `--state-dir` to both binaries. Its parent must exist. The daemon runs in the foreground and handles SIGTERM and SIGINT so a future per-user LaunchAgent can manage it.

See [IPC v1](docs/ipc-v1.md), [storage and domain schema](docs/storage.md), and the [product specification](SPEC.md).

See [queue depth metrics](docs/queue-metrics.md) for opt-in and pending-count semantics.

See [process metrics](docs/process-metrics.md) for the CLI walkthrough, history and retention semantics, and benchmark reproduction.

## Run a queue worker on macOS

Start the daemon as above and register a Laravel app with a configured queue connection. Use the project UUID returned by `project add`. Roost uses Herd by default when the PHP path is omitted. You can also pass an explicit executable or select `--provider system`. See [Laravel runtime and queue discovery](docs/laravel-discovery.md) for the discovery commands and a demo.

```sh
target/debug/roost project add /path/to/laravel
target/debug/roost worker profile create PROJECT_UUID Default database default
target/debug/roost project start PROJECT_UUID /path/to/php
target/debug/roost process list PROJECT_UUID
target/debug/roost log tail PROJECT_UUID
target/debug/roost project restart PROJECT_UUID /path/to/php
target/debug/roost project stop PROJECT_UUID
target/debug/roost process list PROJECT_UUID
```

Use `--state-dir "$smoke_dir/state"` before the subcommand if you started an isolated daemon. Poll `process list` for the worker's observed state. `log tail` shows bounded, redacted output lines. Use `roost worker crashes ID` for recent crash reports or `roost project autostart ID on` to opt a project into resuming saved running intent on daemon startup. Use `worker profile create ... --options-json '{"instances":2,"queues":["high","default"],"crash_restart":"on_failure"}'` to override the defaults. See [worker supervision](docs/supervision.md) for ownership, recovery, and logging behavior.

## Run a Laravel scheduler

Configure and run one scheduler per registered project using `roost scheduler`. See the [scheduler commands and macOS walkthrough](docs/scheduler.md).

## Restart workers on file changes

Enable the project watcher and opt in the worker profiles that should restart. See [file watching and targeted restarts](docs/file-watching.md) for the rules, status commands, and macOS demo.

## Run a project group

Use `roost group` to manage queue workers and schedulers across multiple projects. A monorepo group can also map a shared directory to specific members for code-change restarts. See the [group commands and Milestone 3 walkthrough](docs/project-groups.md).
