# Milestone 2 test on macOS

Run these commands from the Roost repository root in Terminal. Use a regular macOS account with Apple Command Line Tools and Rust installed.

## 1. Build, lint, and run the automated suite

```sh
cargo build --workspace --locked
cargo fmt --all -- --check
rustfmt --edition 2024 --check crates/roost-daemon/tests/fixtures/queue_worker.rs
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Expect 20 tests to pass. The automated tests cover the SQLite upgrade, concurrent commands, crash backoff, manual stop, process-group cleanup after daemon SIGKILL, and private socket permissions. If the socket test fails at `bind` with `Operation not permitted`, run the remaining suite with the command below. The socket test and the smoke test still need a session that permits filesystem Unix sockets.

```sh
cargo test --workspace --locked -- --skip protects_private_endpoint_and_recovers_a_stale_socket
```

## 2. Test the CLI against a real daemon socket

Run the script from the checkout. It builds the CLI and daemon for the current Mac before running them, so binaries left by an earlier milestone cannot affect the test. It also builds a Rust worker fixture, uses disposable daemon state, and checks the worker PID change, manual stop, output summaries, and registration persistence.

```sh
./m2-test.sh
```

Expect two different worker PIDs, a `stopped` state after manual stop, and a final `Milestone 2 CLI smoke test passed.` The fixture produces output summaries. A failed run leaves its temporary state and prints the daemon log for diagnosis.

## 3. Repeat with a local Laravel app

Choose a development app and a working local queue connection such as `database` or `redis`. For a database queue, use a development database with the jobs table already present. Use the PHP executable that normally runs that app. The script selects a new UUID-based queue and enqueues no jobs. An idle real worker may produce `[]` for `log tail`. Use a project without Horizon for this smoke test.

Use the `ROOST_TEST_` prefix. Set all three variables and run the script again. Confirm the first line says `Smoke test mode: Laravel app` and that it prints the expected app and PHP paths.

```sh
export ROOST_TEST_APP='/absolute/path/to/laravel-app'
export ROOST_TEST_PHP='/absolute/path/to/php'
export ROOST_TEST_CONNECTION='database'
./m2-test.sh
```

Afterward, clear the overrides to restore the fixture mode.

```sh
unset ROOST_TEST_APP ROOST_TEST_PHP ROOST_TEST_CONNECTION
```
