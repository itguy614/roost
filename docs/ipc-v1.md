# Roost IPC v1

Send one JSON-RPC 2.0 request per UTF-8 line over `roost.sock`, with a newline delimiter. Use named parameters as an object and supply an ID to receive a response. The daemon accepts notifications, which omit the ID and receive no response. This slice supports single requests and sequential pipelining. Batch arrays receive an invalid-request error. A successful `daemon.hello` is required on each connection before other methods. A subsequent unsuccessful handshake clears negotiation.

```json
{"jsonrpc":"2.0","id":1,"method":"daemon.hello","params":{"protocol_version":1}}
```

| Method | Parameters | Result |
| --- | --- | --- |
| `daemon.hello` | `protocol_version` (integer) | `protocol_version` and the available method names in `capabilities` |
| `daemon.status` | Omit or use `{}` | Daemon `pid`, monotonic `uptime_seconds`, and `protocol_version` |
| `project.list` | Omit or use `{}` | Array of project records ordered by canonical path |
| `project.add` | Absolute UTF-8 `path`, optional UUID `idempotency_key` | `operation_id` and `result` containing the created project |
| `project.get` | Project UUID `id` | Project record |
| `project.remove` | Project UUID `id`, optional UUID `idempotency_key` | `operation_id` and `result` containing the removed record's `id` |

UUIDs are strings. Path and registration validation runs in the daemon. The CLI resolves relative paths against its own working directory before sending them. `project.add` verifies a regular file at `artisan` after resolving the project directory. It does not execute PHP.

Mutations complete synchronously before the response. With an idempotency key, the daemon uses it as the operation ID and can replay a committed success after a lost response or restart. Retry with the same method and arguments. Reusing a committed key for different arguments or a different method returns a conflict. Without a key, the server generates a fresh operation ID. There is no separate operation lookup in v1.

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
| `-32003` | Project not found |
| `-32004` | Already registered, or mutation identity or revision conflict |

The endpoint uses a user-owned state directory with mode `0700`, a socket with mode `0600`, peer UID validation, and an exclusive lock for the daemon. The server accepts up to 32 active connections. Requests are capped at 64 KiB and responses at 4 MiB, excluding delimiters. Reads and writes have five-second deadlines. The server closes idle connections and connections that send an oversized request.
