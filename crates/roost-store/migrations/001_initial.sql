CREATE TABLE groups (
    id TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 4294967295),
    data TEXT NOT NULL
);
CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    path TEXT NOT NULL UNIQUE,
    group_id TEXT REFERENCES groups(id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 4294967295),
    data TEXT NOT NULL
);
CREATE INDEX projects_group ON projects(group_id);
CREATE TABLE worker_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 4294967295),
    data TEXT NOT NULL
);
CREATE INDEX worker_profiles_project ON worker_profiles(project_id);
CREATE TABLE mutation_receipts (
    id TEXT PRIMARY KEY NOT NULL,
    request TEXT NOT NULL,
    result TEXT NOT NULL
);
PRAGMA user_version = 1;
