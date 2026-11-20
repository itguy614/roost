# Project groups and monorepo watches

A project belongs to at most one group. Create a group with `group create`, inspect its configuration with `group list` and `group get`, and use `group add-project` or `group remove-project` to edit membership. Use the current group revision for mutations to configuration and membership. A membership change advances the group revision. Remove shared watch references before detaching a target. A group must be empty before deletion, and a grouped project must be detached before `project remove`. Membership owned by a project file is read-only through the CLI.

`group start` and `group restart` target enabled queue-worker profiles and any enabled scheduler profile in each member. The CLI resolves PHP per project unless you supply an explicit `--php` path for the whole group. It prints an outcome for each member and exits unsuccessfully if any member fails. Each member's workers and scheduler are validated together. Valid members can proceed when another member fails validation. The daemon writes accepted intents and the idempotency receipt together before applying lifecycle transitions. A database write failure rolls back every intent in that request. Group stop requests graceful cleanup for queue workers and configured schedulers and persists their stopped intent. A manually stopped project remains stopped on watcher events. An explicit group start or restart can run that member again.

A group root is optional for lifecycle commands. Shared monitoring requires a root and a list of existing directories relative to it. Shared directories must be disjoint within and across groups. Each directory declares its target project UUIDs, and every target must be a member under the root. You can map a shared directory inside one app to a different app. Local project watchers exclude mapped directories, and a shared directory watcher excludes registered project roots below it. Built-in exclusions and symlink restrictions apply. The daemon supports up to 16 shared directory mappings. Group `watch_rules` apply relative to each mapped directory and use the same defaults as [local watching](file-watching.md). Each target must enable project watching and opt its worker profiles into code-change restarts. Shared changes invalidate the targeted projects' discovery caches.

`watch status PROJECT_ID` reports the local watcher in the top-level state and each configured mapping in `shared`. Project watching is a single opt-in for both. If it is disabled, the CLI still lists the project's shared mappings with state `disabled` and no pending changes. Other opted-in projects can keep receiving changes from the same directory.

## Milestone 3 walkthrough on macOS

Use two disposable Laravel apps inside a test monorepo, with app directories named `alpha` and `beta`, installed dependencies, a trusted PHP executable, and configured queue connections. For an idle test, configure a `null` connection with the `null` driver in both apps and keep the console schedule empty. From the Roost repository, build and start an isolated daemon in one terminal.

```sh
cargo build --workspace --locked
state_parent="$(mktemp -d /tmp/roost-m3.XXXXXX)"
printf '%s\n' "$state_parent/state"
target/debug/roost-daemon --state-dir "$state_parent/state"
```

In a second terminal from the Roost repository, set `state` to the path printed above. Use paths to your disposable monorepo and PHP executable.

```sh
state=/tmp/roost-m3.EXAMPLE/state
repo=/absolute/path/to/disposable/monorepo
php=/absolute/path/to/php
roost() { target/debug/roost --state-dir "$state" "$@"; }
roost daemon status
mkdir -p "$repo/beta/shared"
alpha="$(roost project add "$repo/alpha" | plutil -extract result.id raw -o - -)"
beta="$(roost project add "$repo/beta" | plutil -extract result.id raw -o - -)"
created="$(roost group create Monorepo)"
group="$(printf '%s' "$created" | plutil -extract result.id raw -o - -)"
rev="$(printf '%s' "$created" | plutil -extract result.revision raw -o - -)"
for project in "$alpha" "$beta"; do
  rev="$(roost group add-project "$group" "$project" --revision "$rev" |
    plutil -extract result.group.revision raw -o - -)"
done
options="$(printf '{"root_path":"%s","shared_paths":[{"path":"beta/shared","projects":["%s"]}]}' "$repo" "$alpha")"
roost group update "$group" Monorepo --revision "$rev" --options-json "$options"
roost group get "$group"
```

Discover the effective queue connections and inspect suggested names. The example uses the `null` connection and a private test queue. Set `connection` to a different discovered connection if needed.

```sh
connection=null
for project in "$alpha" "$beta"; do
  roost queue discover "$project" --php "$php"
  roost queue suggestions "$project" roost-m3-demo
  profile="$(roost worker profile create "$project" Demo "$connection" roost-m3-demo)"
  profile_id="$(printf '%s' "$profile" | plutil -extract result.id raw -o - -)"
  profile_rev="$(printf '%s' "$profile" | plutil -extract result.revision raw -o - -)"
  roost worker profile set-code-restart "$profile_id" on-change --revision "$profile_rev"
  project_rev="$(roost watch status "$project" | plutil -extract revision raw -o - -)"
  roost watch enable "$project" --revision "$project_rev"
done
roost scheduler configure "$alpha"
roost watch status "$alpha"
roost group start "$group" --php "$php"
roost process list "$alpha"
roost process list "$beta"
roost scheduler status "$alpha"
```

Wait for both workers and the scheduler to report `running`. Also check that Alpha's `watch status` lists the shared directory as `watching` before the edit. Record the worker and scheduler PIDs. After the atomic save, poll until the Alpha worker returns to `running` with a new PID. The Beta PID and Alpha scheduler PID should be unchanged.

```sh
printf '%s\n' '<?php // Shared edit' > "$repo/beta/shared/roost-m3.tmp"
mv "$repo/beta/shared/roost-m3.tmp" "$repo/beta/shared/roost-m3-demo.php"
roost process list "$alpha"
roost process list "$beta"
roost scheduler status "$alpha"
roost project stop "$alpha"
roost process list "$alpha"
```

Wait for Alpha's worker to stop before deleting the fixture file. After the next watcher batch settles, verify the Alpha worker is still stopped. Stop the group and poll until Beta's worker and Alpha's scheduler are stopped and the recorded PIDs have exited. Shut down the isolated daemon with Control-C in its terminal.

```sh
rm "$repo/beta/shared/roost-m3-demo.php"
roost watch status "$alpha"
roost process list "$alpha"
roost group stop "$group"
roost process list "$beta"
roost scheduler status "$alpha"
roost group get "$group"
```
