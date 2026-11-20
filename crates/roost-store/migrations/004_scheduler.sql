CREATE TABLE scheduler_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL UNIQUE REFERENCES projects(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 4294967295),
    data TEXT NOT NULL
);
CREATE TABLE scheduler_intents (
    project_id TEXT PRIMARY KEY NOT NULL REFERENCES scheduler_profiles(project_id) ON DELETE CASCADE,
    data TEXT NOT NULL
);
CREATE TABLE scheduler_leases (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL UNIQUE REFERENCES projects(id) ON DELETE RESTRICT,
    profile_id TEXT NOT NULL REFERENCES scheduler_profiles(id) ON DELETE RESTRICT,
    slot INTEGER NOT NULL CHECK (slot = 0),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    data TEXT NOT NULL
);
CREATE TABLE scheduler_crashes (
    sequence INTEGER PRIMARY KEY,
    id TEXT UNIQUE NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    data TEXT NOT NULL
);
CREATE INDEX scheduler_crashes_project ON scheduler_crashes(project_id, sequence);
PRAGMA user_version = 4;
