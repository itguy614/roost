# Roost IPC v1

Send one JSON-RPC 2.0 request per UTF-8 line over `roost.sock`, with a newline delimiter. Use named parameters as an object and supply an ID to receive a response. The daemon accepts notifications, which omit the ID and receive no response. This slice supports single requests and sequential pipelining. Batch arrays receive an invalid-request error. A successful `daemon.hello` is required on each connection before other methods. A subsequent unsuccessful handshake clears negotiation.

```json
{"jsonrpc":"2.0","id":1,"method":"daemon.hello","params":{"protocol_version":1}}
```

| Method | Parameters | Result |
| --- | --- | --- |
| `daemon.hello` | `protocol_version` (integer) | `protocol_version` and the available method names in `capabilities` |
| `daemon.status` | Omit or use `{}` | Daemon `pid`, monotonic `uptime_seconds`, `protocol_version`, and `startup` |
| `project.list` | Omit or use `{}` | Array of project records ordered by canonical path |
| `project.add` | Absolute UTF-8 `path`, optional UUID `idempotency_key` | `operation_id` and `result` containing the created project |
| `project.get` | Project UUID `id` | Project record |
| `project.remove` | Project UUID `id`, optional UUID `idempotency_key` | `operation_id` and `result` containing the removed record's `id` |
| `provider.list` | Omit or use `{}` | Supported provider names |
| `provider.detect` | Project UUID `id` | Preferred provider, Herd and system availability, and whether an override is configured |
| `provider.resolvePhp` | Project UUID `id`, optional `provider` or absolute `php` override | Validated executable `path` and `source` |
| `queue.connections` | Project UUID `id` | Latest successful in-memory snapshot for the current project revision, or `null` if absent or invalidated by the watcher |
| `queue.discover` | Same parameters as `provider.resolvePhp` | Fresh snapshot with default connection, connection names and drivers, omission count, observation time, and PHP source |
| `queue.suggestions` | Project UUID `id`, optional `explicit` string array | Incomplete symbolic name suggestions with sources, ordered configured profile queues, and scan coverage flags |
| `scheduler.configure` | Project UUID `id`, optional `options`, optional `expected_revision` for updates, optional UUID `idempotency_key` | Mutation result with the scheduler profile |
| `scheduler.status` | Project UUID `id` | Scheduler profile, saved desired state, and optional observed process state |
| `scheduler.start` | Project UUID `id`, absolute `php` path, optional UUID `idempotency_key` | Mutation result with project `id` and desired state `running` |
| `scheduler.restart` | Same parameters as `scheduler.start` | Queues replacement after cleanup and returns the requested intent |
| `scheduler.stop` | Project UUID `id`, optional UUID `idempotency_key` | Mutation result with project `id` and desired state `stopped` |
| `scheduler.logs` | Project UUID `id` | Bounded, redacted scheduler output lines |
| `scheduler.crashes` | Project UUID `id` | Recent persistent scheduler crash reports |
| `worker.profile.create` | `project_id`, `name`, `connection`, optional `options`, optional UUID `idempotency_key` | Mutation result containing the new profile |
| `worker.profile.list` | Project UUID `id` | Configured worker profiles |
| `worker.profile.setCodeRestart` | Worker profile UUID `id`, integer `expected_revision`, `policy` (`never` or `on_change`), optional UUID `idempotency_key` | Mutation result containing the worker profile |
| `watch.status` | Project UUID `id` | Project `id`, current project `revision`, saved `rules`, `state`, `pending` batch flag, and nullable `problem` |
| `watch.enable` | Project UUID `id`, integer `expected_revision`, optional `rules`, optional UUID `idempotency_key` | Mutation result containing the project with watching enabled |
| `watch.disable` | Project UUID `id`, integer `expected_revision`, optional UUID `idempotency_key` | Mutation result containing the project with watching disabled |
| `project.start` | Project UUID `id`, absolute `php` path, optional Boolean `allow_horizon`, optional UUID `idempotency_key` | Mutation result with project `id` and desired state `running` |
| `project.restart` | Same parameters as `project.start` | Queues replacement after cleanup and returns the requested intent |
| `project.setAutostart` | Project UUID `id`, Boolean `enabled`, optional UUID `idempotency_key` | Mutation result containing the project record |
| `project.stop` | Project UUID `id`, optional UUID `idempotency_key` | Mutation result with project `id` and desired state `stopped` |
| `process.list` | Project UUID `id` | Slot status including desired and observed state, identity, generation, restart count, backoff, last exit, `reason`, optional `launch_failure`, and recovery block |
| `log.tail` | Project UUID `id` | Bounded output lines with instance ID, generation, timestamp, stream, byte count, newline count, and redacted `text` |
| `worker.crashes` | Project UUID `id` | Recent persistent crash reports including exit metadata, reason, and redacted output |
| `group.list` | Omit or use `{}` | Array of group records |
| `group.get` | Group UUID `id` | Group record and member projects with saved worker intent and nullable scheduler intent |
| `group.create` | `name`, optional `options`, optional UUID `idempotency_key` | Mutation result containing the new group |
| `group.update` | Group UUID `id`, `expected_revision`, `name`, complete `options`, optional UUID `idempotency_key` | Mutation result containing the updated group |
| `group.delete` | Group UUID `id`, `expected_revision`, optional UUID `idempotency_key` | Mutation result containing the deleted group UUID |
| `group.addProject`, `group.removeProject` | Group UUID `id`, `project_id`, group `expected_revision`, optional UUID `idempotency_key` | Mutation result containing the group and its members |
| `group.start`, `group.restart` | Group UUID `id`, group `expected_revision`, `launches` with distinct member `id`, absolute `php` path, optional Boolean `allow_horizon`, optional UUID `idempotency_key` | Mutation result with a per-project outcome |
| `group.stop` | Group UUID `id`, optional UUID `idempotency_key` | Mutation result acknowledging per-project stop intent |

Worker profile `options` accepts the following fields and defaults.

| Field | Default |
| --- | --- |
| `queues` | `[]` |
| `instances` | `1` |
| `sleep_seconds` | `3` |
| `tries` | `1` |
| `timeout_seconds` | `60` |
| `graceful_stop_seconds` | `15` |
| `crash_restart` | `"on_failure"` |

Worker mutations use the `operation_id` and `result` envelope. Start, stop, and restart acknowledge intent before the OS transition finishes. A replay returns the historical acknowledgement even across a daemon restart. A replay leaves current intent unchanged. Project reads report saved desired state. A new session resumes saved running intent only when autostart is enabled and the prior lease has confirmed cleanup. A project with active supervision, queued recovery, or unresolved ownership must be stopped before removal. See [worker supervision](supervision.md) for lifecycle and safety details.

Scheduler options are `enabled` (defaults to true), `crash_restart` (defaults to `on_failure`), and `graceful_stop_seconds` (defaults to 15, allowed 1–60). Configuration requires stopped scheduler intent and confirmed cleanup. Local edits to file-owned scheduler settings return a conflict. The scheduler is independently managed from queue workers. See [scheduler lifecycle and demo](scheduler.md).

Watch rules contain `include` and `exclude` arrays of relative globs. Omitting `rules` preserves the saved rules. Watch state is `disabled`, `initializing`, `watching`, or `degraded`. The project watcher and profile policy both default to off. Changing a profile policy requires the project's queue workers to be stopped and their leases cleaned up. File-owned settings and stale revisions return conflicts. See [file watching](file-watching.md).

The optional `watch.status.shared` array lists watched group directories for that project, with `group_id`, the group-relative `path`, `state`, `pending`, and nullable `problem`. Group options contain optional `root_path`, `watch_rules`, and `shared_paths` entries with a relative directory `path` and explicit `projects` UUIDs. Group lifecycle results contain `id` and a `projects` array. Each entry has a project `id` and a `status` of `accepted` with `workers` and `scheduler` flags, `unchanged`, or `failed` with a structured `error`. Acceptance records intent. Poll the usual worker and scheduler status methods for observed state.

The `startup` field is `first_start`, `after_clean_shutdown`, or `after_interrupted_shutdown`. Worker exits can also include a `stop_cause` such as `project_stop`, `project_restart`, `daemon_shutdown`, or `daemon_disconnected`. Startup failure classifications include `validation`, `spawn_rejected`, `resource_unavailable`, `readiness_timeout`, `state_publication`, and `cancelled`. The response fields and methods are additive to IPC v1.

UUIDs are strings. Path and registration validation runs in the daemon. The CLI resolves relative paths against its own working directory before sending them. `project.add` verifies a regular file at `artisan` after resolving the project directory. It does not execute PHP.

Configuration mutations commit before the response. Worker lifecycle mutations acknowledge acceptance before the process reaches the requested state. With an idempotency key, the daemon uses it as the operation ID and can replay a committed success after a lost response or restart. Retry with the same method and arguments. Reusing a committed key for different arguments or a different method returns a conflict. Without a key, the server generates a fresh operation ID. There is no separate operation lookup in v1.

| Code | Meaning |
| --- | --- |
| `-32700` | Invalid JSON or incomplete newline-delimited frame |
| `-32600` | Invalid request envelope |
| `-32601` | Unknown method |
| `-32602` | Invalid parameters or project validation failure |
| `-32603` | Internal failure or database busy |
| `-32000` | Handshake required |
| `-32001` | Unsupported protocol version, with `error.data.supported` |
| `-32002` | Request or response exceeds its size limit |
| `-32003` | Project, profile, or group not found |
| `-32004` | Already registered, mutation identity or revision conflict, or a worker safety constraint |
| `-32005` | PHP provider or discovery unavailable, or the probe failed or returned invalid data |
| `-32006` | Probe timed out |
| `-32007` | Probe output exceeded its limit |
| `-32008` | All discovery slots are busy |

The endpoint uses a user-owned state directory with mode `0700`, a socket with mode `0600`, peer UID validation, and an exclusive lock for the daemon. The server accepts up to 32 active connections. Requests are capped at 64 KiB and responses at 4 MiB, excluding delimiters. Reads and writes have five-second deadlines. The server closes idle connections and connections that send an oversized request.

See [Laravel runtime and queue discovery](laravel-discovery.md) for selection precedence, limits, and the bootstrap trust boundary.
