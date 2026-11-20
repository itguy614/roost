CREATE TABLE project_intents (
    project_id TEXT PRIMARY KEY NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    data TEXT NOT NULL
);
CREATE TABLE daemon_lifecycle (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    clean_shutdown INTEGER NOT NULL CHECK (clean_shutdown IN (0, 1))
);
CREATE TABLE worker_crashes (
    sequence INTEGER PRIMARY KEY,
    id TEXT UNIQUE NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    data TEXT NOT NULL
);
CREATE INDEX worker_crashes_project ON worker_crashes(project_id, sequence);
PRAGMA user_version = 3;
