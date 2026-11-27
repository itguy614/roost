# Roost desktop development

The desktop shell uses the existing private Roost socket and IPC v1. Its window shows live project registration and observed worker and scheduler counts. The standalone daemon and notification helper have their own lifetimes. Closing the window hides it. Opening Roost again shows the window and refreshes its data. Quit Roost exits the UI.

## Build and verify

Use macOS, Command Line Tools, Rust and Node. Development was verified with Rust 1.96, Node 24.22 and npm 11.19 on Apple Silicon macOS 26.6.2. The npm and desktop Cargo lockfiles are independent of the standalone Rust lockfile. [Tauri desktop prerequisites](https://v2.tauri.app/start/prerequisites/)

Run the checks from the repository root.

```sh
cargo build --workspace --locked
npm --prefix apps/desktop ci
npm --prefix apps/desktop run typecheck
npm --prefix apps/desktop run lint
npm --prefix apps/desktop test
cargo fmt --manifest-path apps/desktop/src-tauri/Cargo.toml -- --check
cargo clippy --manifest-path apps/desktop/src-tauri/Cargo.toml --all-targets --locked -- -D warnings
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked
```

Start the daemon separately with `target/debug/roost-daemon`. In a second terminal run `cd apps/desktop && npm run tauri -- dev`. Vite needs loopback port 1420. Build a production-asset app bundle for local screenshot review.

```sh
(cd apps/desktop && npm run tauri -- build --bundles app --config '{"bundle":{"active":true}}')
APP="$PWD/apps/desktop/src-tauri/target/release/bundle/macos/Roost.app"
codesign --force --sign - "$APP"
codesign --verify --strict "$APP"
```

The signature is for this local test app. Distribution and system service installation are Milestone 6 gates.

## Disposable smoke test

In one terminal from the repository root, start a daemon with its own state. Keep the printed state path for the second terminal.

```sh
TEST_DIR=$(mktemp -d /private/tmp/roost-ui.XXXXXX)
printf '%s\n' "$TEST_DIR"
target/debug/roost-daemon --state-dir "$TEST_DIR/state"
```

In the second terminal, from the repository root, set `TEST_DIR` to that same path. Open the app on that state and confirm the empty dashboard. Register two disposable projects through CLI and confirm their rows appear in the GUI. Each fixture has an `artisan` file for registration. The fixture file does not run Laravel jobs.

```sh
APP="$PWD/apps/desktop/src-tauri/target/release/bundle/macos/Roost.app"
open -n -a "$APP" --env "ROOST_DESKTOP_STATE_DIR=$TEST_DIR/state" \
  --env ROOST_DESKTOP_APPEARANCE=light
mkdir "$TEST_DIR/Dispatch" "$TEST_DIR/Workshop"
touch "$TEST_DIR/Dispatch/artisan" "$TEST_DIR/Workshop/artisan"
target/debug/roost --state-dir "$TEST_DIR/state" project add "$TEST_DIR/Dispatch"
target/debug/roost --state-dir "$TEST_DIR/state" project add "$TEST_DIR/Workshop"
```

To exercise running counts, use the Dispatch UUID from the first registration as `PROJECT`. Build the repository's test worker to act as an executable PHP fixture, then start two worker instances and a scheduler from the CLI.

```sh
rustc --edition=2024 crates/roost-daemon/tests/fixtures/queue_worker.rs -o "$TEST_DIR/php"
printf graceful > "$TEST_DIR/Dispatch/artisan"
target/debug/roost --state-dir "$TEST_DIR/state" worker profile create "$PROJECT" Default database \
  --options-json '{"queues":["critical","default"],"instances":2,"graceful_stop_seconds":1}'
target/debug/roost --state-dir "$TEST_DIR/state" scheduler configure "$PROJECT"
target/debug/roost --state-dir "$TEST_DIR/state" project start "$PROJECT" "$TEST_DIR/php"
target/debug/roost --state-dir "$TEST_DIR/state" scheduler start "$PROJECT" "$TEST_DIR/php"
```

Confirm the dashboard reaches 2 / 2 workers and 1 / 1 scheduler for Dispatch. Check the sidebar and table, select a project with the keyboard, then close the window and reopen it from the Dock. Quit the app before repeating with `ROOST_DESKTOP_APPEARANCE=dark`. Omit the appearance override to follow System Settings. For a fresh offline window point the test app at `"$TEST_DIR/absent"`. To verify disconnect recovery, stop the original test daemon with Control-C while the GUI is connected, confirm the last read is labeled as a previous snapshot, then start the daemon against the same private directory and wait for reconnection.

A dashboard read negotiates IPC on one connection and caps per-project summary reads at eight. The list itself uses the existing bounded IPC response. A three-second deadline covers the entire snapshot. Visible refreshes run every five seconds after completion, with single-flight coalescing and a two- to fifteen-second retry backoff. Hidden or unmounted views stop scheduling reads; a result from an obsolete window/page scope is discarded. Unsupported or failed observations appear as such in the table. Daemon mutations, service management, and the shared streaming coordinator are handled in the planned later slices.

## PR 1 native evidence

The optimized local bundle displayed the populated dashboard and offline state in both appearances. A private-socket fixture that advertised IPC v2 produced the version-mismatch screen. A restart of the real test daemon preserved the GUI's last snapshot during the disconnect and refreshed it after reconnect. In a separate macOS run, a test-only timer invoked the native window close operation. No dashboard IPC connection opened during 6.5 seconds hidden. Opening the app through Launch Services refreshed on the same GUI PID. Exiting the GUI left both real daemon-owned fixture workers, their scheduler and the separately started native notification helper running. [Native window screenshots](https://github.com/itguy614/roost/tree/ba00eeae14775014e0d88d7d02c4efb6037289df)

Process launch to the first screen capture with a usable shell and real table was 0.76 s in light mode and 0.58 s in dark mode on this host. In these two launches the same first capture contained both the shell and the real project snapshot. The offline state appeared at 0.59 s and 0.58 s respectively. Timing came from screenshot sampling during four launches.

The optimized resource smoke ran the same two test projects, two fixture workers and one fixture scheduler in all three observed phases. Queue probes were off. The daemon IPC p95 is from `daemon.status` over a persistent client connection. CPU is percent of one logical core and memory is peak summed RSS in MiB for the listed process group.

| Process group | GUI absent CPU / RSS | GUI open CPU / RSS | GUI hidden CPU / RSS |
| --- | ---: | ---: | ---: |
| Daemon and descendants | 2.205% / 39.73 MiB | 2.058% / 39.64 MiB | 1.960% / 39.48 MiB |
| Standalone notification helper | 0.098% / 32.38 MiB | 0.147% / 32.20 MiB | 0.049% / 32.17 MiB |
| Tauri GUI host | — | 0.098% / 104.77 MiB | 0.049% / 105.38 MiB |
| Three WebKit processes started with the GUI | — | 0.686% / 90.55 MiB | 0.294% / 87.94 MiB |
| Daemon status IPC p95 | 2.46 ms | 1.09 ms | 1.00 ms |

Each phase lasted 20.4 s. The hidden arm used the native test-only close trigger. The daemon alone peaked at 11.67 MiB in that arm. The WebKit grouping was estimated from the new process IDs in the launch interval. This smoke establishes costs for the small fixture workload. The approved 12% backend envelope continues to apply to the established full backend workload and excludes the GUI and WebKit. These measurements do not verify the macOS minimum, Intel, LaunchAgent, distribution or extended release reliability gates.
