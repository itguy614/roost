CREATE TABLE worker_leases (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    profile_id TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE RESTRICT,
    slot INTEGER NOT NULL CHECK (slot >= 0),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    data TEXT NOT NULL,
    UNIQUE (profile_id, slot)
);
CREATE INDEX worker_leases_project ON worker_leases(project_id);
PRAGMA user_version = 2;
