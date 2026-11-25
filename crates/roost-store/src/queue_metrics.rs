use std::{collections::BTreeSet, ffi::CStr};

use roost_core::{
    ProjectId, Revision,
    metrics::{BUCKET_SECONDS, RAW_SECONDS, ROLLUP_SECONDS},
    queue_metrics::{Reading, Selector, safe_identifier},
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    Error, Store,
    metrics::{HistoryPage, HistoryState, Resolution},
};

pub struct QueueHistory<'a> {
    pub project: ProjectId,
    pub connection: Option<&'a str>,
    pub queue: Option<&'a str>,
    pub resolution: Resolution,
    pub from: u64,
    pub to: u64,
    pub after: u64,
    pub high: Option<u64>,
    pub limit: u32,
    pub lineage: Option<&'a str>,
    pub retention_floor: u64,
}

pub struct QueueProbeStamp<'a> {
    pub project: ProjectId,
    pub revision: Revision,
    pub token: &'a str,
    pub selectors: &'a [Selector],
}

/// An uncommitted queue batch. A dropped batch rolls back without publication.
pub struct QueueMetricWrite<'a> {
    store: &'a Store,
    committed: bool,
}

impl QueueMetricWrite<'_> {
    /// Never wait on SQLite readers while the caller holds a publication lock.
    pub fn commit(mut self) -> Result<(), Error> {
        self.store.0.exec(c"PRAGMA busy_timeout = 0")?;
        self.store.0.exec(c"COMMIT")?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for QueueMetricWrite<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.store.0.exec(c"ROLLBACK");
        }
        // Use the bounded metric writer timeout for subsequent operations.
        let _ = self.store.0.exec(c"PRAGMA busy_timeout = 50");
    }
}

impl Store {
    pub fn has_queue_metrics(&self) -> Result<bool, Error> {
        Ok(self
            .0
            .query(c"SELECT EXISTS(SELECT 1 FROM queue_metric_series)", &[])?
            == ["1"])
    }

    pub fn queue_probe_projects(&self) -> Result<Vec<ProjectId>, Error> {
        self.0
            .query(
                c"SELECT json_quote(q.project_id) FROM queue_probe_projects q
                WHERE EXISTS (SELECT 1 FROM worker_profiles p WHERE p.project_id=q.project_id
                              AND json_extract(p.data,'$.enabled')=1) ORDER BY q.project_id",
                &[],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    pub fn queue_probe_token(&self, project: ProjectId) -> Result<Option<String>, Error> {
        Ok(self
            .0
            .query(
                c"SELECT token FROM queue_probe_projects WHERE project_id=?1",
                &[Some(&project.0.to_string())],
            )?
            .pop())
    }

    pub fn configure_queue_probe(&self, project: ProjectId, enabled: bool) -> Result<(), Error> {
        let id = project.0.to_string();
        if enabled {
            self.0.query(
                c"INSERT OR IGNORE INTO queue_probe_projects VALUES (?1,?2)",
                &[Some(&id), Some(&Uuid::new_v4().to_string())],
            )?;
        } else {
            self.0.query(
                c"DELETE FROM queue_probe_projects WHERE project_id=?1",
                &[Some(&id)],
            )?;
        }
        Ok(())
    }

    pub fn queue_probe_selectors(&self, id: ProjectId) -> Result<Vec<Selector>, Error> {
        let mut result = BTreeSet::new();
        for profile in self
            .list_worker_profiles(id)?
            .into_iter()
            .filter(|p| p.enabled)
        {
            if !safe_identifier(&profile.connection) {
                continue;
            }
            if profile.queues.is_empty() {
                result.insert(Selector {
                    connection: profile.connection,
                    queue: None,
                });
            } else {
                for queue in profile
                    .queues
                    .into_iter()
                    .filter(|queue| safe_identifier(queue))
                {
                    result.insert(Selector {
                        connection: profile.connection.clone(),
                        queue: Some(queue),
                    });
                }
            }
        }
        Ok(result.into_iter().collect())
    }

    pub fn stage_queue_metrics(
        &self,
        stamp: QueueProbeStamp<'_>,
        at: u64,
        batch: &[(Selector, Reading)],
    ) -> Result<Option<QueueMetricWrite<'_>>, Error> {
        self.0.exec(c"BEGIN IMMEDIATE")?;
        let staged = QueueMetricWrite {
            store: self,
            committed: false,
        };
        let store = self;
        {
            if store
                .get_project(stamp.project)?
                .is_none_or(|p| p.revision != stamp.revision)
                || store.queue_probe_token(stamp.project)?.as_deref() != Some(stamp.token)
                || store.queue_probe_selectors(stamp.project)? != stamp.selectors
            {
                return Ok(None);
            }
            let mut recorded = BTreeSet::new();
            for (selector, reading) in batch {
                let project = stamp.project.0.to_string();
                let connection = &selector.connection;
                let queue = reading
                    .queue
                    .as_deref()
                    .or(selector.queue.as_deref())
                    .unwrap_or_default();
                if !recorded.insert((connection, queue)) {
                    continue;
                }
                store.0.query(
                    c"INSERT OR IGNORE INTO queue_metric_series (project_id,connection,queue)
                    VALUES (?1,?2,?3)",
                    &[Some(&project), Some(connection), Some(queue)],
                )?;
                let reason = reading
                    .reason
                    .map(|value| serde_json::to_string(&value))
                    .transpose()?;
                store.0.query(c"INSERT INTO queue_metric_raw (series_id,observed_at,pending,reason)
                    SELECT s.id,?4,?5,json_extract(?6,'$') FROM queue_metric_series s,queue_metric_state m
                    WHERE s.project_id=?1 AND s.connection=?2 AND s.queue=?3
                        AND CAST(?4 AS INTEGER)>=m.rolled_until AND CAST(?4 AS INTEGER)>=coalesce(
                        (SELECT observed_at FROM queue_metric_raw ORDER BY sequence DESC LIMIT 1),0)",
                    &[Some(&project), Some(connection), Some(queue), Some(&at.to_string()),
                      reading.pending.map(|n|n.to_string()).as_deref(), reason.as_deref()])?;
            }
            Ok(Some(staged))
        }
    }

    pub fn maintain_queue_metrics(&self, now: u64) -> Result<(), Error> {
        let settled = now / BUCKET_SECONDS * BUCKET_SECONDS;
        self.transaction(|store| {
            let end = store.0.query(c"SELECT coalesce((SELECT (min(observed_at)/300+1)*300
                FROM queue_metric_raw WHERE observed_at>=rolled_until AND observed_at<CAST(?1 AS INTEGER)),
                CAST(?1 AS INTEGER)) FROM queue_metric_state WHERE id=1",
                &[Some(&settled.to_string())])?.pop().ok_or(Error::Schema)?;
            store.0.query(c"INSERT OR IGNORE INTO queue_metric_rollups
                (series_id,bucket,samples,observations,mean,minimum,maximum)
                SELECT series_id, observed_at/300*300,count(pending), count(*),avg(pending),min(pending),max(pending)
                FROM queue_metric_raw,queue_metric_state
                WHERE observed_at>=rolled_until AND observed_at<CAST(?1 AS INTEGER)
                GROUP BY series_id, observed_at/300", &[Some(&end)])?;
            store.0.query(c"UPDATE queue_metric_state SET rolled_until=max(rolled_until,CAST(?1 AS INTEGER))",
                &[Some(&end)])?;
            store.prune_queue_rows(
                c"SELECT coalesce(max(sequence),0) FROM (SELECT sequence FROM queue_metric_raw
                   WHERE observed_at<?1 AND observed_at<(SELECT rolled_until FROM queue_metric_state)
                   ORDER BY observed_at,sequence LIMIT 512)",
                c"DELETE FROM queue_metric_raw WHERE sequence<=?1 AND observed_at<?2 AND observed_at<(SELECT rolled_until FROM queue_metric_state)",
                c"UPDATE queue_metric_state SET raw_pruned=max(raw_pruned,CAST(?1 AS INTEGER))",
                now.saturating_sub(RAW_SECONDS),
            )?;
            store.prune_queue_rows(
                c"SELECT coalesce(max(sequence),0) FROM (SELECT sequence FROM queue_metric_rollups
                    WHERE bucket<=CAST(?1 AS INTEGER)-300 ORDER BY bucket,sequence LIMIT 256)",
                c"DELETE FROM queue_metric_rollups WHERE sequence<=?1 AND bucket<=CAST(?2 AS INTEGER)-300",
                c"UPDATE queue_metric_state SET rollup_pruned=max(rollup_pruned,CAST(?1 AS INTEGER))",
                now.saturating_sub(ROLLUP_SECONDS),
            )?;
            store.0.exec(c"DELETE FROM queue_metric_series WHERE id IN (
              SELECT id FROM queue_metric_series s
              WHERE NOT EXISTS (SELECT 1 FROM queue_metric_raw WHERE series_id=s.id)
                AND NOT EXISTS (SELECT 1 FROM queue_metric_rollups WHERE series_id=s.id) LIMIT 128)")?;
            Ok(())
        })
    }

    fn prune_queue_rows(
        &self,
        choose: &CStr,
        delete: &CStr,
        mark: &CStr,
        floor: u64,
    ) -> Result<(), Error> {
        // Both streams append in timestamp order. Bound the physical deletion
        // by the last sequence in the selected prefix.
        let last = self.0.query(choose, &[Some(&floor.to_string())])?;
        if let Some(last) = last.first().filter(|value| *value != "0") {
            self.0
                .query(delete, &[Some(last), Some(&floor.to_string())])?;
            self.0.query(mark, &[Some(last)])?;
        }
        Ok(())
    }

    pub fn queue_metric_history(&self, query: QueueHistory<'_>) -> Result<HistoryPage, Error> {
        self.transaction_with(c"BEGIN", |store| {
            let (window, rows) = match query.resolution {
                Resolution::Raw => (RAW_WINDOW, RAW_HISTORY),
                Resolution::FiveMinutes => (ROLLUP_WINDOW, ROLLUP_HISTORY),
            };
            let args = [
                Some(query.retention_floor.to_string()),
                Some(query.from.to_string()),
                Some(query.to.to_string()),
            ];
            let selected = store
                .0
                .query(window, &args.each_ref().map(|q| q.as_deref()))?
                .pop()
                .ok_or(Error::Schema)?;
            let Window { state, first, last } = serde_json::from_str(&selected)?;
            if query
                .lineage
                .is_some_and(|lineage| state.lineage != lineage)
            {
                return Ok(HistoryPage::DifferentLineage);
            }
            if query.lineage.is_some() && query.after < state.pruned {
                return Ok(HistoryPage::Expired(state));
            }
            let mut result = Vec::new();
            if let (Some(first), Some(last)) = (first, last) {
                let after = query.after.max(state.pruned).max(first.saturating_sub(1));
                let high = query.high.unwrap_or(state.high).min(last);
                if after < high {
                    result = store
                        .0
                        .query(
                            rows,
                            &[
                                Some(&query.project.0.to_string()),
                                query.connection,
                                query.queue,
                                Some(&query.from.to_string()),
                                Some(&query.to.to_string()),
                                Some(&after.to_string()),
                                Some(&high.to_string()),
                                Some(&query.limit.to_string()),
                            ],
                        )?
                        .iter()
                        .map(|row| serde_json::from_str(row))
                        .collect::<Result<_, _>>()?;
                }
            }
            Ok(HistoryPage::Rows {
                state,
                rows: result,
            })
        })
    }
}

#[derive(Deserialize)]
struct Window {
    #[serde(flatten)]
    state: HistoryState,
    first: Option<u64>,
    last: Option<u64>,
}

const RAW_WINDOW: &CStr = c"SELECT json_object('lineage',lineage,
    'pruned',max(raw_pruned,coalesce((SELECT sequence FROM queue_metric_raw WHERE observed_at<?1
      ORDER BY observed_at DESC,sequence DESC LIMIT 1),0)),
    'high',max(raw_pruned,coalesce((SELECT max(sequence) FROM queue_metric_raw),0)),
    'first',(SELECT sequence FROM queue_metric_raw WHERE observed_at>=?2 ORDER BY observed_at,sequence LIMIT 1),
    'last',(SELECT sequence FROM queue_metric_raw WHERE observed_at<?3 ORDER BY observed_at DESC,sequence DESC LIMIT 1))
    FROM queue_metric_state";

const ROLLUP_WINDOW: &CStr = c"SELECT json_object('lineage',lineage,
    'pruned',max(rollup_pruned,coalesce((SELECT sequence FROM queue_metric_rollups WHERE bucket<=CAST(?1 AS INTEGER)-300
      ORDER BY bucket DESC,sequence DESC LIMIT 1),0)),
    'high',max(rollup_pruned,coalesce((SELECT max(sequence) FROM queue_metric_rollups),0)),
    'first',(SELECT sequence FROM queue_metric_rollups WHERE bucket>=?2 ORDER BY bucket,sequence LIMIT 1),
    'last',(SELECT sequence FROM queue_metric_rollups WHERE bucket<=CAST(?3 AS INTEGER)-300 ORDER BY bucket DESC,sequence DESC LIMIT 1))
    FROM queue_metric_state";

const RAW_HISTORY: &CStr = c"SELECT json_object(
    'sequence',CAST(r.sequence AS TEXT),'connection',s.connection,'queue',nullif(s.queue,''),
    'observed_at',strftime('%Y-%m-%dT%H:%M:%SZ',r.observed_at,'unixepoch'),
    'pending',json_object('state',CASE WHEN r.pending IS NOT NULL THEN 'fresh'
       WHEN r.reason IN ('driver_unsupported','framework_unsupported','custom_connector') THEN 'unsupported'
       ELSE 'unknown' END,'value',r.pending,'reason',r.reason))
    FROM queue_metric_raw r JOIN queue_metric_series s ON r.series_id=s.id
    WHERE s.project_id=?1 AND (?2 IS NULL OR s.connection=?2) AND (?3 IS NULL OR s.queue=?3)
      AND r.observed_at>=?4 AND r.observed_at<?5 AND r.sequence>?6 AND r.sequence<=?7
    ORDER BY r.sequence LIMIT ?8";

const ROLLUP_HISTORY: &CStr = c"SELECT json_object(
    'sequence',CAST(r.sequence AS TEXT),'connection',s.connection,'queue',nullif(s.queue,''),
    'from',strftime('%Y-%m-%dT%H:%M:%SZ',r.bucket,'unixepoch'),
    'to',strftime('%Y-%m-%dT%H:%M:%SZ',r.bucket+300,'unixepoch'),
    'pending',json_object('state',CASE WHEN r.samples=0 THEN 'unknown' ELSE 'fresh' END,
        'mean',r.mean,'min',r.minimum,'max',r.maximum,'samples',r.samples,
        'observations',r.observations,'expected_samples',30))
    FROM queue_metric_rollups r JOIN queue_metric_series s ON r.series_id=s.id
    WHERE s.project_id=?1 AND (?2 IS NULL OR s.connection=?2) AND (?3 IS NULL OR s.queue=?3)
      AND r.bucket>=?4 AND r.bucket<=CAST(?5 AS INTEGER)-300 AND r.sequence>?6 AND r.sequence<=?7
    ORDER BY r.sequence LIMIT ?8";

#[cfg(test)]
mod tests {
    use super::*;
    use roost_core::{
        CodeRestartPolicy, CrashRestartPolicy, Project, WorkerProfile, WorkerProfileId,
        queue_metrics::Problem,
    };

    fn save(
        store: &Store,
        stamp: QueueProbeStamp<'_>,
        at: u64,
        batch: &[(Selector, Reading)],
    ) -> Result<bool, Error> {
        let Some(write) = store.stage_queue_metrics(stamp, at, batch)? else {
            return Ok(false);
        };
        write.commit()?;
        Ok(true)
    }

    #[test]
    fn queue_batches_fence_previous_opt_in_and_keep_retention_atomic() {
        let scratch = std::env::temp_dir().join(format!("roost-queue-store-{}", Uuid::new_v4()));
        std::fs::create_dir(&scratch).unwrap();
        struct Guard(std::path::PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let scratch = Guard(scratch.canonicalize().unwrap());
        std::fs::write(scratch.0.join("artisan"), b"").unwrap();
        let project = Project::register(&scratch.0).unwrap();
        let path = scratch.0.join("state.sqlite3");
        let legacy = Store(crate::sqlite::Connection::open(&path, true).unwrap());
        for migration in [
            crate::INITIAL,
            crate::WORKER_LEASES,
            crate::RUNTIME,
            crate::SCHEDULER,
            crate::PROCESS_METRICS,
        ] {
            legacy.0.exec(migration).unwrap();
        }
        legacy.create_project(&project).unwrap();
        legacy
            .create_worker_profile(&WorkerProfile {
                id: WorkerProfileId(Uuid::new_v4()),
                project_id: project.id,
                name: "jobs".into(),
                connection: "database".into(),
                queues: vec![],
                desired_instances: 1,
                sleep_seconds: 3,
                timeout_seconds: 60,
                tries: 1,
                graceful_stop_seconds: 15,
                crash_restart: CrashRestartPolicy::OnFailure,
                code_restart: CodeRestartPolicy::Never,
                notifications: false,
                enabled: true,
                revision: Revision(1),
            })
            .unwrap();
        drop(legacy);
        Store::initialize(&path).unwrap();
        let store = Store::open(&path).unwrap();
        assert!(store.queue_probe_projects().unwrap().is_empty());
        let selectors = store.queue_probe_selectors(project.id).unwrap();
        let at = 1_800_000_000;
        let sample = [(
            selectors[0].clone(),
            Reading {
                queue: Some("default".into()),
                pending: Some(0),
                reason: None,
            },
        )];
        let stamp = |token| QueueProbeStamp {
            project: project.id,
            revision: project.revision,
            token,
            selectors: &selectors,
        };

        store.configure_queue_probe(project.id, true).unwrap();
        let before = store.queue_probe_token(project.id).unwrap().unwrap();
        assert_eq!(store.queue_probe_projects().unwrap(), [project.id]);
        assert!(save(&store, stamp(&before), at, &sample).unwrap());
        store.configure_queue_probe(project.id, false).unwrap();
        store.configure_queue_probe(project.id, true).unwrap();
        let after = store.queue_probe_token(project.id).unwrap().unwrap();
        assert!(!save(&store, stamp(&before), at + 1, &sample).unwrap());
        let missing = [(
            selectors[0].clone(),
            Reading {
                queue: Some("default".into()),
                ..Reading::failed(Problem::Timeout)
            },
        )];
        assert!(save(&store, stamp(&after), at + 10, &missing).unwrap());
        let page = |resolution, after, lineage| {
            store
                .queue_metric_history(QueueHistory {
                    project: project.id,
                    connection: Some("database"),
                    queue: Some("default"),
                    resolution,
                    from: at,
                    to: at + 300,
                    after,
                    high: None,
                    limit: 4,
                    lineage,
                    retention_floor: 0,
                })
                .unwrap()
        };
        let HistoryPage::Rows { state, rows } = page(Resolution::Raw, 0, None) else {
            panic!("missing raw samples")
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["pending"]["value"], 0);
        assert_eq!(rows[1]["pending"]["reason"], "timeout");
        assert_eq!(rows[1]["pending"]["state"], "unknown");

        store
            .0
            .exec(
                c"CREATE TRIGGER fail_queue_raw_prune BEFORE DELETE ON queue_metric_raw
                        BEGIN SELECT RAISE(ABORT,'fixture'); END",
            )
            .unwrap();
        assert!(
            store
                .maintain_queue_metrics(at + RAW_SECONDS + 300)
                .is_err()
        );
        assert!(
            matches!(page(Resolution::Raw, 0, Some(&state.lineage)), HistoryPage::Rows { rows, .. } if rows.len() == 2)
        );
        assert!(
            matches!(page(Resolution::FiveMinutes, 0, None), HistoryPage::Rows { rows, .. } if rows.is_empty())
        );
        store.0.exec(c"DROP TRIGGER fail_queue_raw_prune").unwrap();
        for _ in 0..2 {
            store
                .maintain_queue_metrics(at + RAW_SECONDS + 300)
                .unwrap();
        }
        assert!(
            matches!(page(Resolution::Raw, 1, Some(&state.lineage)), HistoryPage::Expired(s) if s.pruned == 2)
        );
        assert!(
            matches!(page(Resolution::Raw, 2, Some(&state.lineage)), HistoryPage::Rows { rows, .. } if rows.is_empty())
        );
        let HistoryPage::Rows { rows, .. } = page(Resolution::FiveMinutes, 0, None) else {
            panic!("missing rollup")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["pending"]["samples"], 1);
        assert_eq!(rows[0]["pending"]["observations"], 2);
        store
            .maintain_queue_metrics(at + ROLLUP_SECONDS + 300)
            .unwrap();
        assert!(
            matches!(page(Resolution::FiveMinutes, 0, Some(&state.lineage)), HistoryPage::Expired(s) if s.pruned == 1)
        );
    }
}
