CREATE TABLE process_metric_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    lineage TEXT NOT NULL,
    counting_since INTEGER NOT NULL,
    rolled_until INTEGER NOT NULL DEFAULT 0,
    raw_pruned INTEGER NOT NULL DEFAULT 0,
    rollup_pruned INTEGER NOT NULL DEFAULT 0
);
INSERT INTO process_metric_state (id, lineage, counting_since)
VALUES (1, lower(hex(randomblob(16))), unixepoch());

CREATE TABLE process_metric_series (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    slot INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    UNIQUE (instance_id, generation)
);
CREATE INDEX process_metric_series_project ON process_metric_series(project_id, profile_id);

CREATE TABLE process_metric_raw (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER NOT NULL REFERENCES process_metric_series(id) ON DELETE CASCADE,
    observed_at INTEGER NOT NULL,
    cpu_percent REAL,
    rss_bytes INTEGER,
    uptime_seconds INTEGER,
    peak_rss_bytes INTEGER
);
CREATE INDEX process_metric_raw_time ON process_metric_raw(observed_at);
CREATE INDEX process_metric_raw_series ON process_metric_raw(series_id, sequence);

CREATE TABLE process_metric_rollups (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER NOT NULL REFERENCES process_metric_series(id) ON DELETE CASCADE,
    bucket INTEGER NOT NULL,
    cpu_count INTEGER NOT NULL,
    cpu_mean REAL,
    cpu_min REAL,
    cpu_max REAL,
    rss_count INTEGER NOT NULL,
    rss_mean REAL,
    rss_min INTEGER,
    rss_max INTEGER,
    peak_rss_bytes INTEGER,
    max_uptime_seconds INTEGER,
    UNIQUE (series_id, bucket)
);
CREATE INDEX process_metric_rollups_time ON process_metric_rollups(bucket);

CREATE TABLE process_lifecycle_totals (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    slot INTEGER NOT NULL,
    launches INTEGER NOT NULL,
    crashes INTEGER NOT NULL,
    PRIMARY KEY (project_id, kind, profile_id, slot)
) WITHOUT ROWID;

CREATE TRIGGER count_worker_metric_launch
AFTER UPDATE OF data ON worker_leases
WHEN json_extract(NEW.data, '$.state.state') = 'running'
 AND json_extract(OLD.data, '$.state.state') <> 'running'
BEGIN
    INSERT INTO process_lifecycle_totals VALUES (NEW.project_id, 'worker', NEW.profile_id, NEW.slot, 1, 0)
    ON CONFLICT DO UPDATE SET launches = launches + 1;
END;

CREATE TRIGGER count_scheduler_metric_launch
AFTER UPDATE OF data ON scheduler_leases
WHEN json_extract(NEW.data, '$.state.state') = 'running'
 AND json_extract(OLD.data, '$.state.state') <> 'running'
BEGIN
    INSERT INTO process_lifecycle_totals VALUES (NEW.project_id, 'scheduler', NEW.profile_id, NEW.slot, 1, 0)
    ON CONFLICT DO UPDATE SET launches = launches + 1;
END;

PRAGMA user_version = 5;
