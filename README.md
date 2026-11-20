# Roost

Roost is a macOS-first supervisor for local Laravel queue workers and schedulers. Milestone 1 registers Laravel projects through a Rust CLI and a per-user daemon. The daemon owns SQLite and serves versioned JSON-RPC over a private Unix socket.

## Developer setup

Use a regular macOS user account, Apple Command Line Tools, and a Rust toolchain with Cargo, rustfmt and Clippy. Milestone 1 was built with Rust 1.96 on Apple Silicon. The build links the SQLite library from the macOS SDK. PHP, Herd, Node.js and Tauri are unnecessary for registration.

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
