# Laravel runtime and queue discovery

Roost resolves PHP for each registered project. On macOS the Herd provider runs Herd's installed `herd.phar which-php` through its bundled PHP from the project directory. The result must be an absolute executable in the installed Herd bin directory. This integration was verified against the local macOS Herd installation. If Herd cannot resolve the project or its installation layout changes, Roost reports a provider error. An explicit PHP path remains available for those projects.

Resolution order is an explicit executable, an explicitly selected provider, the stored project override, and the stored provider preference. New projects default to Herd. Select `--provider system` to search the daemon's absolute PATH entries and common macOS installation directories. System mode skips Herd and paths within the project. Use an explicit executable if the daemon's PATH differs from your terminal's. The CLI resolves PHP before sending an ordinary start or restart request. Existing explicit paths and saved autostart launch paths keep their behavior.

## CLI demo

Start the local daemon and register a trusted development Laravel project with its dependencies installed. Use the UUID in the registration response.

```sh
target/debug/roost project add /path/to/laravel
target/debug/roost provider list
target/debug/roost provider detect PROJECT_UUID
target/debug/roost provider resolve-php PROJECT_UUID
target/debug/roost queue connections PROJECT_UUID
target/debug/roost queue discover PROJECT_UUID
target/debug/roost queue connections PROJECT_UUID
target/debug/roost queue suggestions PROJECT_UUID high default
target/debug/roost worker profile list PROJECT_UUID
```

Use `--state-dir` before the command if your daemon uses a custom state directory. Add `--php /absolute/path/to/php` to discovery to use a specific executable, or use `--provider system` when system PHP is installed. `queue connections` returns `null` until discovery succeeds. Profile listing shows that discovery creates no workers or profiles.

## Effective connection discovery

An explicit discovery request runs a short PHP process with fixed arguments to load the app's Composer autoloader and Laravel console kernel. It reads effective `queue.default` and `queue.connections` through Laravel's config repository, including deliberate cached configuration. The probe receives a minimal environment. Values supplied only through the caller's shell environment can differ from the values Laravel reads from its project files.

The response includes the default connection when present, up to 128 symbolic connection names and driver identifiers, an omitted count, the observation time, and the PHP source. The probe extracts only those fields. The daemon rejects unknown response fields and does not return raw process output, Laravel errors, or connection settings. A connection's presence does not indicate backend reachability. The last successful result is held in memory for up to 64 projects and is labeled with its observation time. A failed refresh preserves the earlier result. Filesystem edits do not refresh the snapshot, and daemon restart clears it.

Run discovery only for projects whose bootstrap code you trust. Bootstrap runs as the daemon user and can perform I/O or other side effects. Process-group management and output filtering do not sandbox the application. Roost limits Herd lookup to 1.2 seconds and 32 KiB, and Laravel discovery to 2.4 seconds and 64 KiB, combining stdout and stderr for each limit. The daemon allows two simultaneous discovery or resolution jobs and returns a retryable busy error for additional requests. It cleans up its owned process group upon completion or timeout. If the probe child cannot be reaped, further runtime probes are refused while cleanup is pending. A sudden daemon death or descendants that escape the group can defeat that cleanup. Identifiers that resemble common credentials are excluded, but an application can still put arbitrary values in identifier fields.

## Queue-name suggestions

The static scanner recognizes literal `onQueue(...)`, `$queue = ...`, and `Queue::pushOn(...)` in bounded PHP source scans. It skips common dependency, storage, and build directories and does not follow symlinks. Results include explicit names passed to the command and names from configured worker profiles. Configured profile results preserve queue order. Symbolic-name filtering and scan limits can omit values. Dynamic references and scan limits are reported separately. The list is always marked incomplete. No source paths or source text are returned.
