CREATE TABLE queue_probe_projects (
    project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    token TEXT NOT NULL
);
CREATE TABLE queue_metric_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    lineage TEXT NOT NULL,
    rolled_until INTEGER NOT NULL DEFAULT 0,
    raw_pruned INTEGER NOT NULL DEFAULT 0,
    rollup_pruned INTEGER NOT NULL DEFAULT 0
);
INSERT INTO queue_metric_state (id,lineage) VALUES (1,lower(hex(randomblob(16))));

CREATE TABLE queue_metric_series (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    connection TEXT NOT NULL,
    queue TEXT NOT NULL,
    UNIQUE(project_id,connection,queue)
);
CREATE TABLE queue_metric_raw (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER NOT NULL REFERENCES queue_metric_series(id) ON DELETE CASCADE,
    observed_at INTEGER NOT NULL,
    pending INTEGER,
    reason TEXT
);
CREATE INDEX queue_metric_raw_time ON queue_metric_raw(observed_at);
CREATE INDEX queue_metric_raw_series ON queue_metric_raw(series_id,sequence);
CREATE TABLE queue_metric_rollups (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER NOT NULL REFERENCES queue_metric_series(id) ON DELETE CASCADE,
    bucket INTEGER NOT NULL,
    samples INTEGER NOT NULL,
    observations INTEGER NOT NULL,
    mean REAL,
    minimum INTEGER,
    maximum INTEGER,
    UNIQUE(series_id,bucket)
);
CREATE INDEX queue_metric_rollups_time ON queue_metric_rollups(bucket);
PRAGMA user_version = 6;
