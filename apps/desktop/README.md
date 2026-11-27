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

## Clean UI and the Roost theme

Roost uses the published `@itguy614/clean-ui` npm package. The Vue entry point registers `createCleanUI()` and imports `@itguy614/clean-ui/styles`. Import named components and composables from the package root. Prefer the library's public props and slots, and use its sidebar, tables, pagination and feedback components before building a custom control. Use native behaviors for macOS window and application actions. [Clean UI documentation](https://itguy614.github.io/clean-ui/)

The Roost theme lives in [`src/theme/roost.css`](src/theme/roost.css). The document applies `cui-theme-roost` so both regular and teleported components inherit the Roost tokens. `restoreColorMode()` follows system appearance by default and maintains the library's `dark` class. A future appearance setting can use the public `useColorMode().setMode()` for Light, Dark, or System and synchronize native window appearance through the desktop layer. Keep Roost-specific tokens and interaction styling here, with [approved UI v3](../../docs/ui/README.md) as the design reference.

When a reusable component is missing, check the public documentation and existing Clean UI issues. Propose a focused issue upstream with the use case, expected semantics, keyboard and assistive-technology behavior, and a minimal example. Keep any Roost-specific composition local. [Clean UI issues](https://github.com/itguy614/clean-ui/issues)

### Development showcase

Run `npm --prefix apps/desktop run dev` and open `http://127.0.0.1:1420/?showcase` to inspect the live themed components. The showcase uses example data and can run without a daemon. The normal production build excludes it. For a native development-only review bundle, use the explicit showcase build mode.

```sh
cd apps/desktop
npm run tauri -- build --bundles app --config \
  '{"bundle":{"active":true},"build":{"beforeBuildCommand":"npm run build -- --mode showcase"}}'
APP="$PWD/src-tauri/target/release/bundle/macos/Roost.app"
codesign --force --sign - "$APP"
codesign --verify --strict "$APP"
open -n -a "$APP" --env ROOST_DESKTOP_APPEARANCE=light
```

Quit the showcase, then repeat the launch with `ROOST_DESKTOP_APPEARANCE=dark`. Rebuild with the regular build command to restore the dashboard bundle at that output location.

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

A dashboard read negotiates IPC on one connection and reads the bounded project directory. The sidebar uses that full directory, independently of the eight-row dashboard page. A selected project is addressed by UUID and can add one summary read if it is outside the page. A three-second deadline covers the entire snapshot. Visible refreshes run every five seconds after completion, with single-flight coalescing and a two- to fifteen-second retry backoff. Hidden or unmounted views stop scheduling reads. Reopening labels cached data stale until the first successful read and displays the timestamp of the last successful update. Results from obsolete window, page or project scopes are discarded. Unsupported or failed observations appear as such in the table. Below or above desired counts describe a discrepancy. Recovery blocked is reserved for an unresolved count reported by the daemon. Daemon mutations, service management, and the shared streaming coordinator are handled in the planned later slices.

## Initial desktop shell evidence

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

## Clean UI review evidence

The optimized regular and showcase bundles passed strict local signature verification. Real macOS captures show the live dashboard, offline state and development showcase in light and dark mode. The regular build emitted no showcase chunk. The first captured populated dashboard appeared after 1.07 s in light mode and 0.59 s in dark mode. Offline appeared after 0.61 s in each appearance. [Clean UI native screenshots](https://github.com/itguy614/roost/tree/01fe7f4253fcb7e77b6b81e461484480f88a41cf)

The short resource rerun used two test projects, two fixture workers, one fixture scheduler and the standalone helper. Queue probes were off. Each phase lasted about 20.4 s. CPU is percent of one logical core and RSS is peak summed MiB for the process group. WebKit attribution uses the three new process IDs observed at GUI launch.

| Process group | GUI absent CPU / RSS | GUI open CPU / RSS |
| --- | ---: | ---: |
| Daemon and descendants | 1.962% / 39.77 MiB | 2.059% / 39.58 MiB |
| Standalone notification helper | 0.098% / 32.53 MiB | 0.147% / 32.30 MiB |
| Tauri GUI host | — | 0.147% / 105.69 MiB |
| WebKit processes started with the GUI | — | 1.029% / 111.75 MiB |
| Daemon status IPC p95 | 2.39 ms | 2.68 ms |

The daemon reconnected during the native smoke. Both fixture workers, the scheduler and the helper kept running after GUI exit in the resource run. Automated accessibility checks cover keyboard focus handoff, table and navigation semantics, and palette contrast. A manual VoiceOver pass remains open. The production frontend emitted approximately 590 kB JS before gzip (189 kB gzipped), triggering Vite's chunk-size warning. These short fixture measurements leave the full backend budget and platform release gates in place.
