-- Existing draft 006 rows were sampled every poll. Keep their provenance
-- separate from the 30-second coverage contract.
ALTER TABLE queue_metric_raw ADD COLUMN flags INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue_metric_series ADD COLUMN periodic_slot INTEGER NOT NULL DEFAULT -1;
ALTER TABLE queue_metric_rollups ADD COLUMN periodic_observations INTEGER;
ALTER TABLE queue_metric_rollups ADD COLUMN transition_observations INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue_metric_rollups ADD COLUMN legacy_observations INTEGER NOT NULL DEFAULT 0;
UPDATE queue_metric_rollups SET legacy_observations=observations;

CREATE TABLE queue_metric_observed (
    project_id TEXT NOT NULL REFERENCES queue_probe_projects(project_id) ON DELETE CASCADE,
    connection TEXT NOT NULL,
    requested_queue TEXT NOT NULL,
    effective_queue TEXT NOT NULL,
    reason TEXT,
    PRIMARY KEY(project_id,connection,requested_queue)
) WITHOUT ROWID;

PRAGMA user_version = 7;
