# First Codex task: Roost Milestone 1

Read `SPEC.md` and `AGENTS.md` completely. You are building Roost, a macOS-first local Laravel queue-worker and scheduler supervisor with a Rust daemon and CLI and a later Tauri 2 + Vue 3 desktop UI.

First, inspect the repository and available toolchains. Produce a concise implementation plan and identify any missing dependencies. Then implement **Milestone 1 only** as a tested, runnable vertical slice:

1. Create a Rust Cargo workspace with `roost-core`, `roost-store`, `roost-ipc`, `roost-daemon`, `roost-cli` (add other crates only when necessary).
2. Define typed project/group/profile/desired-state domain models with validation and UUIDs.
3. Implement SQLite migrations and repository CRUD for projects, groups and worker profiles, including a config revision field.
4. Implement a per-user daemon with private Unix socket, protocol handshake/versioning, basic `daemon.status`, `project.list`, `project.add`, `project.get`, and `project.remove` JSON-RPC methods. Enforce request size limits and meaningful error responses.
5. Implement CLI `roost daemon status`, `roost project add PATH`, `roost project list`, and `roost project remove ID` against IPC. Verify that PATH has a Laravel `artisan` file. Do not spawn workers yet.
6. Add unit/integration tests for validation, migrations, IPC requests, concurrent clients and malformed input.
7. Write developer setup instructions and a simple smoke-test walkthrough. Run `cargo fmt`, `cargo clippy`, and `cargo test` and report the actual results.

Architect for a launchd-managed daemon, but do not attempt signed installation, notifications, metrics, queue discovery, process spawning or desktop UI in this first milestone. Avoid fake functionality or placeholder success messages. Do not implement the entire specification at once.

At the end, summarize files changed, commands run, tests passed/failed, known limitations, and a proposed Milestone 2 plan. Ask for review before expanding scope.
