CREATE TABLE daemon_event_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    lineage TEXT NOT NULL,
    pruned INTEGER NOT NULL DEFAULT 0
);
INSERT INTO daemon_event_state(singleton, lineage) VALUES (1, lower(hex(randomblob(16))));
CREATE TABLE daemon_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX daemon_events_project ON daemon_events(project_id, sequence);
CREATE TABLE alert_rules (
    profile_id TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE CASCADE,
    metric TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY(profile_id, metric)
) WITHOUT ROWID;
CREATE TABLE alert_episodes (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    slot INTEGER NOT NULL,
    metric TEXT NOT NULL,
    closed_at INTEGER,
    closed_reason TEXT,
    data TEXT NOT NULL
);
CREATE INDEX alert_episodes_project ON alert_episodes(project_id, sequence);
CREATE UNIQUE INDEX alert_active_key ON alert_episodes(profile_id,slot,metric) WHERE closed_at IS NULL;
CREATE TABLE alert_cooldowns (
    profile_id TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE CASCADE,
    slot INTEGER NOT NULL,
    metric TEXT NOT NULL,
    until_at INTEGER NOT NULL,
    PRIMARY KEY(profile_id,slot,metric)
) WITHOUT ROWID;
CREATE TABLE alert_restart_attempts (
    episode_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE CASCADE,
    slot INTEGER NOT NULL,
    charged_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX alert_attempts_scope ON alert_restart_attempts(profile_id,slot,charged_at);
PRAGMA user_version = 8;
