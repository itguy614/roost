use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CStr,
};

use roost_core::{
    ProjectId, Revision,
    metrics::{BUCKET_SECONDS, RAW_SECONDS, ROLLUP_SECONDS},
    queue_metrics::{Problem, Reading, Selector, safe_identifier},
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    Error, Store,
    metrics::{HistoryPage, HistoryState, Resolution},
};

const HISTORY_SECONDS: u64 = 30;
const PERIODIC: u8 = 1;
const INITIAL: u8 = 2;
const AVAILABILITY: u8 = 4;
const REASON: u8 = 8;
const IDENTITY: u8 = 16;

#[derive(Deserialize)]
struct Observed {
    queue: String,
    reason: Option<Problem>,
}

impl Observed {
    fn transitions(&self, queue: &str, reason: Option<Problem>) -> u8 {
        let availability = |reason: Option<Problem>| match reason {
            None => 0,
            Some(reason) if reason.unsupported() => 1,
            Some(_) => 2,
        };
        (u8::from(self.queue != queue) * IDENTITY)
            | (u8::from(availability(self.reason) != availability(reason)) * AVAILABILITY)
            | (u8::from(self.reason != reason) * REASON)
    }
}

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
        if self
            .get_project(stamp.project)?
            .is_none_or(|p| p.revision != stamp.revision)
            || self.queue_probe_token(stamp.project)?.as_deref() != Some(stamp.token)
            || self.queue_probe_selectors(stamp.project)? != stamp.selectors
        {
            return Ok(None);
        }
        let observed_at = at.to_string();
        let in_order = self.0.query(
            c"SELECT CAST(?1 AS INTEGER)>=max(rolled_until,coalesce(
            (SELECT observed_at FROM queue_metric_raw ORDER BY sequence DESC LIMIT 1),0))
            FROM queue_metric_state",
            &[Some(&observed_at)],
        )?;
        if in_order == ["1"] {
            self.stage_observations(stamp.project, at, batch)?;
        }
        Ok(Some(staged))
    }

    fn stage_observations(
        &self,
        project: ProjectId,
        at: u64,
        batch: &[(Selector, Reading)],
    ) -> Result<(), Error> {
        let project = project.0.to_string();
        let mut series: BTreeMap<(&str, &str), (&Reading, u8)> = BTreeMap::new();
        let periodic_slot = (at / HISTORY_SECONDS).to_string();
        let at = at.to_string();
        for (selector, reading) in batch {
            let connection = selector.connection.as_str();
            let requested = selector.queue.as_deref().unwrap_or_default();
            let queue = reading.queue.as_deref().unwrap_or(requested);
            let prior = self.0.query(c"SELECT json_object('queue',effective_queue,'reason',reason)
                FROM queue_metric_observed WHERE project_id=?1 AND connection=?2 AND requested_queue=?3",
                &[Some(&project), Some(connection), Some(requested)])?
                .pop().map(|row| serde_json::from_str::<Observed>(&row)).transpose()?;
            let flags = prior.map_or(INITIAL, |prior| prior.transitions(queue, reading.reason));
            if flags != 0 {
                let reason = reading
                    .reason
                    .map(|value| serde_json::to_string(&value))
                    .transpose()?;
                self.0.query(
                    c"INSERT INTO queue_metric_observed
                    VALUES (?1,?2,?3,?4,json_extract(?5,'$'))
                    ON CONFLICT(project_id,connection,requested_queue)
                    DO UPDATE SET effective_queue=excluded.effective_queue,reason=excluded.reason",
                    &[
                        Some(&project),
                        Some(connection),
                        Some(requested),
                        Some(queue),
                        reason.as_deref(),
                    ],
                )?;
            }
            series
                .entry((connection, queue))
                .and_modify(|(previous, events)| {
                    *events |= flags;
                    // If aliases disagree, prefer the measured or already
                    // resolved observation. A partial failure can still be
                    // represented by an observed transition flag.
                    if previous.pending.is_none() && reading.pending.is_some() {
                        *previous = reading;
                    }
                })
                .or_insert((reading, flags));
        }
        for ((connection, queue), (reading, flags)) in series {
            let keys = [Some(project.as_str()), Some(connection), Some(queue)];
            self.0.query(
                c"INSERT OR IGNORE INTO queue_metric_series (project_id,connection,queue)
                VALUES (?1,?2,?3)",
                &keys,
            )?;
            let periodic = self.0.query(c"UPDATE queue_metric_series
                SET periodic_slot=CAST(?4 AS INTEGER)
                WHERE project_id=?1 AND connection=?2 AND queue=?3 AND periodic_slot<CAST(?4 AS INTEGER)
                RETURNING 1", &[keys[0],keys[1],keys[2],Some(&periodic_slot)])? == ["1"];
            let flags = flags | (u8::from(periodic) * PERIODIC);
            if flags == 0 {
                continue;
            }
            let reason = reading
                .reason
                .map(|value| serde_json::to_string(&value))
                .transpose()?;
            self.0.query(
                c"INSERT INTO queue_metric_raw (series_id,observed_at,pending,reason,flags)
                SELECT id,?4,?5,json_extract(?6,'$'),?7 FROM queue_metric_series
                WHERE project_id=?1 AND connection=?2 AND queue=?3",
                &[
                    keys[0],
                    keys[1],
                    keys[2],
                    Some(&at),
                    reading.pending.map(|n| n.to_string()).as_deref(),
                    reason.as_deref(),
                    Some(&flags.to_string()),
                ],
            )?;
        }
        Ok(())
    }

    pub fn maintain_queue_metrics(&self, now: u64) -> Result<(), Error> {
        let settled = now / BUCKET_SECONDS * BUCKET_SECONDS;
        self.transaction(|store| {
            let end = store.0.query(c"SELECT coalesce((SELECT (min(observed_at)/300+1)*300
                FROM queue_metric_raw WHERE observed_at>=rolled_until AND observed_at<CAST(?1 AS INTEGER)),
                CAST(?1 AS INTEGER)) FROM queue_metric_state WHERE id=1",
                &[Some(&settled.to_string())])?.pop().ok_or(Error::Schema)?;
            store.0.query(c"INSERT OR IGNORE INTO queue_metric_rollups
                (series_id,bucket,samples,observations,mean,minimum,maximum,
                 periodic_observations,transition_observations,legacy_observations)
                SELECT series_id, observed_at/300*300,count(pending) FILTER (WHERE flags & 1 != 0),
                    count(*),avg(pending) FILTER (WHERE flags & 1 != 0),
                    min(pending) FILTER (WHERE flags & 1 != 0),max(pending) FILTER (WHERE flags & 1 != 0),
                    count(*) FILTER (WHERE flags & 1 != 0), count(*) FILTER (WHERE flags & 30 != 0),
                    count(*) FILTER (WHERE flags = 0)
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
            store.0.exec(c"DELETE FROM queue_metric_observed WHERE (project_id,connection,requested_queue) IN (
              SELECT o.project_id,o.connection,o.requested_queue FROM queue_metric_observed o
              WHERE NOT EXISTS (SELECT 1 FROM queue_metric_series s WHERE s.project_id=o.project_id
                AND s.connection=o.connection AND s.queue=o.effective_queue) LIMIT 128)")?;
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
    'coverage',json_object('periodic',json(CASE WHEN flags & 1 != 0 THEN 'true' ELSE 'false' END),
        'legacy',json(CASE WHEN flags = 0 THEN 'true' ELSE 'false' END),
        'initial',json(CASE WHEN flags & 2 != 0 THEN 'true' ELSE 'false' END),
        'availability_changed',json(CASE WHEN flags & 4 != 0 THEN 'true' ELSE 'false' END),
        'reason_changed',json(CASE WHEN flags & 8 != 0 THEN 'true' ELSE 'false' END),
        'identity_changed',json(CASE WHEN flags & 16 != 0 THEN 'true' ELSE 'false' END)),
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
        'observations',r.observations,
        'periodic_observations',r.periodic_observations,'transition_observations',r.transition_observations,
        'legacy_observations',r.legacy_observations,
        'expected_samples',CASE WHEN r.periodic_observations IS NULL THEN 30 ELSE 10 END))
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

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn fixture() -> (Guard, Project, std::path::PathBuf, Store) {
        let scratch = std::env::temp_dir().join(format!("roost-queue-store-{}", Uuid::new_v4()));
        std::fs::create_dir(&scratch).unwrap();
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
        (scratch, project, path, legacy)
    }

    #[test]
    fn queue_batches_fence_previous_opt_in_and_keep_retention_atomic() {
        let (_scratch, project, path, legacy) = fixture();
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
    #[test]
    fn transitions_and_periodic_coverage_stay_independent_across_restart() {
        let (_scratch, project, path, legacy) = fixture();
        drop(legacy);
        Store::initialize(&path).unwrap();
        let store = Store::open(&path).unwrap();
        store.configure_queue_probe(project.id, true).unwrap();
        let selectors = store.queue_probe_selectors(project.id).unwrap();
        let token = store.queue_probe_token(project.id).unwrap().unwrap();
        let start = 1_800_000_000;
        let append = |store: &Store, offset, queue: &str, pending, reason| {
            assert!(
                save(
                    store,
                    QueueProbeStamp {
                        project: project.id,
                        revision: project.revision,
                        token: &token,
                        selectors: &selectors,
                    },
                    start + offset,
                    &[(
                        selectors[0].clone(),
                        Reading {
                            queue: Some(queue.into()),
                            pending,
                            reason
                        }
                    )],
                )
                .unwrap()
            );
        };
        append(&store, 0, "alpha", Some(1), None);
        append(&store, 10, "alpha", Some(999), None);
        append(&store, 15, "alpha", None, Some(Problem::Timeout));
        append(&store, 20, "alpha", None, Some(Problem::BackendUnavailable));
        append(&store, 23, "alpha", Some(99), None);
        drop(store);
        let store = Store::open(&path).unwrap();
        append(&store, 27, "alpha", Some(777), None);
        append(&store, 30, "alpha", Some(3), None);
        append(&store, 40, "beta", Some(55), None);
        append(&store, 50, "alpha", Some(6), None);

        let rows = |resolution, queue| {
            let HistoryPage::Rows { rows, .. } = store
                .queue_metric_history(QueueHistory {
                    project: project.id,
                    connection: Some("database"),
                    queue: Some(queue),
                    resolution,
                    from: start,
                    to: start + 300,
                    after: 0,
                    high: None,
                    limit: 12,
                    lineage: None,
                    retention_floor: 0,
                })
                .unwrap()
            else {
                panic!("expected history rows")
            };
            rows
        };
        let alpha = rows(Resolution::Raw, "alpha");
        assert_eq!(alpha.len(), 6);
        assert_eq!(
            alpha[0]["coverage"],
            serde_json::json!({
                "periodic": true, "legacy": false, "initial": true,
                "availability_changed": false, "reason_changed": false, "identity_changed": false
            })
        );
        assert_eq!(alpha[1]["coverage"]["availability_changed"], true);
        assert_eq!(alpha[1]["coverage"]["reason_changed"], true);
        assert_eq!(alpha[2]["coverage"]["availability_changed"], false);
        assert_eq!(alpha[2]["coverage"]["reason_changed"], true);
        assert_eq!(alpha[3]["pending"]["value"], 99);
        assert_eq!(alpha[3]["coverage"]["availability_changed"], true);
        assert_eq!(alpha[3]["coverage"]["periodic"], false);
        assert_eq!(alpha[4]["coverage"]["periodic"], true);
        assert_eq!(alpha[4]["coverage"]["initial"], false);
        assert_eq!(alpha[5]["coverage"]["identity_changed"], true);
        assert_eq!(alpha[5]["coverage"]["periodic"], false);
        let beta = rows(Resolution::Raw, "beta");
        assert_eq!(beta[0]["coverage"]["identity_changed"], true);
        assert_eq!(beta[0]["coverage"]["periodic"], true);

        store.maintain_queue_metrics(start + 300).unwrap();
        assert_eq!(
            rows(Resolution::FiveMinutes, "alpha")[0]["pending"],
            serde_json::json!({
                "state": "fresh", "mean": 2.0, "min": 1, "max": 3, "samples": 2,
                "observations": 6, "periodic_observations": 2, "transition_observations": 5,
                "legacy_observations": 0, "expected_samples": 10
            })
        );
        store
            .maintain_queue_metrics(start + ROLLUP_SECONDS + 600)
            .unwrap();
        assert!(
            store
                .0
                .query(c"SELECT 1 FROM queue_metric_observed", &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn migration_preserves_legacy_coverage_without_counting_it_as_periodic() {
        let (_scratch, project, path, legacy) = fixture();
        legacy.0.exec(crate::QUEUE_METRICS).unwrap();
        legacy
            .0
            .query(
                c"INSERT INTO queue_metric_series (project_id,connection,queue)
            VALUES (?1,'database','default')",
                &[Some(&project.id.0.to_string())],
            )
            .unwrap();
        legacy
            .0
            .exec(
                c"INSERT INTO queue_metric_rollups
            (series_id,bucket,samples,observations,mean,minimum,maximum)
                VALUES (1,1800000000,1,1,12,12,12);
            INSERT INTO queue_metric_raw (series_id,observed_at,pending) VALUES (1,1800000300,99);
            UPDATE queue_metric_state SET rolled_until=1800000300",
            )
            .unwrap();
        drop(legacy);
        Store::initialize(&path).unwrap();
        let store = Store::open(&path).unwrap();
        let rows = |resolution| {
            let HistoryPage::Rows { rows, .. } = store
                .queue_metric_history(QueueHistory {
                    project: project.id,
                    connection: None,
                    queue: None,
                    resolution,
                    from: 1_800_000_000,
                    to: 1_800_000_600,
                    after: 0,
                    high: None,
                    limit: 4,
                    lineage: None,
                    retention_floor: 0,
                })
                .unwrap()
            else {
                panic!("expected history rows")
            };
            rows
        };
        let raw = rows(Resolution::Raw);
        assert_eq!(raw[0]["coverage"]["legacy"], true);
        assert_eq!(raw[0]["coverage"]["periodic"], false);
        store.maintain_queue_metrics(1_800_000_600).unwrap();
        let rollup = rows(Resolution::FiveMinutes);
        assert_eq!(
            rollup[0]["pending"]["periodic_observations"],
            serde_json::Value::Null
        );
        assert_eq!(rollup[0]["pending"]["legacy_observations"], 1);
        assert_eq!(rollup[0]["pending"]["expected_samples"], 30);
        assert_eq!(rollup[0]["pending"]["mean"], 12.0);
        assert_eq!(rollup[1]["pending"]["periodic_observations"], 0);
        assert_eq!(rollup[1]["pending"]["legacy_observations"], 1);
        assert_eq!(rollup[1]["pending"]["expected_samples"], 10);
        assert_eq!(rollup[1]["pending"]["mean"], serde_json::Value::Null);
    }
}
