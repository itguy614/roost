# Milestone 1 storage and domain schema

The daemon owns `roost.sqlite3` in its private state directory. The CLI uses IPC for all operations. SQLite migrations run transactionally at daemon startup and record their version in `PRAGMA user_version`. The daemon refuses an unknown schema version. Migration 001 creates groups, projects, worker profiles, and mutation receipts. Indexed identity and relationship fields live in SQLite columns alongside the serialized record. Foreign keys are enabled for every connection.

## Records

Each record has a UUID `id` and integer `revision`. Revisions start at 1. Repository updates compare the supplied revision and increment it on success. Deleting a project cascades to its worker profiles. Group deletion requires the group to contain no projects.

- **Project** includes optional `group_id`, `name`, canonical `path`, `provider` (`herd` or `system`), optional absolute `php_override`, `desired_state` (`stopped` or `running`), `autostart`, `watch_enabled`, `watch_rules`, and `config_owner` (`local` or `file`). Registration saves a unique canonical path, Herd as the configured provider, desired state stopped, local ownership, and disabled autostart and watching.
- **Group** includes `name`, optional absolute `root_path`, `watch_rules`, and `created_at` and `updated_at` in Unix seconds.
- **Worker profile** includes `project_id`, `name`, `connection`, ordered `queues`, `desired_instances`, `sleep_seconds`, `tries`, `timeout_seconds`, `graceful_stop_seconds`, `crash_restart` (`never`, `on_failure`, or `always`), `code_restart` (`never` or `on_change`), `notifications`, and `enabled`. An empty queue list leaves the connection default to the future runner.
- **Watch rules** are an object with `include` and `exclude` arrays of strings.

The domain validates names, paths and worker settings. The daemon registers projects through IPC. Group and worker profile CRUD is available through the Rust repository. Portable `.roost.json` loading, PHP resolution, runtime state, and process ownership are later milestones.

For successful keyed mutations, the daemon stores the serialized request identity and original result in the same SQLite transaction as the change. Receipts currently remain in the local database indefinitely.
