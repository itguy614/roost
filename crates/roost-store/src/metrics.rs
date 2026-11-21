use std::{ffi::CStr, path::Path};

use roost_core::{
    ProjectId,
    metrics::{BUCKET_SECONDS, ProcessKind, ProcessSample, RAW_SECONDS, ROLLUP_SECONDS},
    worker_runtime::{ProcessCrash, ProcessProfileId},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Store};

#[derive(Debug, Deserialize, Serialize)]
pub struct LifecycleTotals {
    pub counting_since: u64,
    pub restarts: u64,
    pub crashes: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryState {
    pub lineage: String,
    pub pruned: u64,
    pub high: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Resolution {
    Raw,
    #[serde(rename = "5m")]
    FiveMinutes,
}

pub struct HistoryQuery<'a> {
    pub project_id: ProjectId,
    pub profile: Option<&'a str>,
    pub resolution: Resolution,
    pub from: u64,
    pub to: u64,
    pub after: u64,
    pub retention_floor: u64,
    pub high: Option<u64>,
    pub lineage: Option<&'a str>,
    pub limit: u32,
}

#[derive(Debug)]
pub enum HistoryPage {
    Rows {
        state: HistoryState,
        rows: Vec<Value>,
    },
    Expired(HistoryState),
    DifferentLineage,
}

impl Store {
    pub fn open_for_metric_writes(path: &Path) -> Result<Self, Error> {
        let store = Self::open(path)?;
        // Dropping a sample is preferable to queuing behind a lifecycle write.
        store.0.exec(c"PRAGMA busy_timeout = 50")?;
        Ok(store)
    }

    pub fn save_process_samples(&self, samples: &[ProcessSample]) -> Result<(), Error> {
        if samples.is_empty() {
            return Ok(());
        }
        self.transaction(|store| {
            for sample in samples {
                let key = sample.identity;
                let project = key.project_id.0.to_string();
                let instance = key.instance_id.to_string();
                let generation = key.generation.to_string();
                store.0.query(
                    c"INSERT OR IGNORE INTO process_metric_series
                      (project_id,kind,profile_id,slot,instance_id,generation)
                      SELECT id,?2,?3,?4,?5,?6 FROM projects WHERE id=?1",
                    &[Some(&project), Some(key.kind.as_str()), Some(&key.profile_id.to_string()),
                        Some(&key.slot.to_string()), Some(&instance), Some(&generation)],
                )?;
                store.0.query(
                    c"INSERT INTO process_metric_raw
                      (series_id,observed_at,cpu_percent,rss_bytes,uptime_seconds,peak_rss_bytes)
                      SELECT s.id,?3,?4,?5,?6,?7 FROM process_metric_series s, process_metric_state m
                      WHERE s.instance_id=?1 AND s.generation=?2 AND CAST(?3 AS INTEGER)>=m.rolled_until
                        AND CAST(?3 AS INTEGER)>=coalesce(
                          (SELECT observed_at FROM process_metric_raw ORDER BY sequence DESC LIMIT 1),0)",
                    &[Some(&instance), Some(&generation), Some(&sample.observed_at.to_string()),
                        sample.cpu_percent.map(|v| v.to_string()).as_deref(),
                        sample.rss_bytes.map(|v| v.to_string()).as_deref(),
                        sample.uptime_seconds.map(|v| v.to_string()).as_deref(),
                        sample.peak_rss_bytes.map(|v| v.to_string()).as_deref()],
                )?;
            }
            Ok(())
        })
    }

    pub(crate) fn count_metric_crash<P: ProcessProfileId>(
        &self,
        report: &ProcessCrash<P>,
    ) -> Result<(), Error> {
        self.0.query(
            c"INSERT INTO process_lifecycle_totals VALUES (?1,?2,?3,?4,0,1)
              ON CONFLICT DO UPDATE SET crashes=crashes+1",
            &[
                Some(&report.project_id.0.to_string()),
                Some(ProcessKind::for_profile(P::SCHEDULER).as_str()),
                Some(&report.profile_id.uuid().to_string()),
                Some(&report.slot.to_string()),
            ],
        )?;
        Ok(())
    }

    pub fn process_lifecycle_totals(
        &self,
        project: ProjectId,
        profile: Option<&str>,
    ) -> Result<LifecycleTotals, Error> {
        let row = self
            .0
            .query(
                c"SELECT json_object('counting_since',counting_since,
             'restarts',coalesce((SELECT sum(max(0, launches-1)) FROM process_lifecycle_totals
                 WHERE project_id=?1 AND (?2 IS NULL OR profile_id=?2)),0),
             'crashes',coalesce((SELECT sum(crashes) FROM process_lifecycle_totals
                 WHERE project_id=?1 AND (?2 IS NULL OR profile_id=?2)),0))
             FROM process_metric_state WHERE id=1",
                &[Some(&project.0.to_string()), profile],
            )?
            .pop()
            .ok_or(Error::NotFound)?;
        Ok(serde_json::from_str(&row)?)
    }

    pub fn maintain_process_metrics(&self, now: u64) -> Result<(), Error> {
        let settled = now / BUCKET_SECONDS * BUCKET_SECONDS;
        self.transaction(|store| {
            // Process at most one populated bucket per pass.
            let end = store.0.query(
                c"SELECT coalesce(
                   (SELECT (min(observed_at)/300+1)*300 FROM process_metric_raw
                    WHERE observed_at>=rolled_until AND observed_at<CAST(?1 AS INTEGER)),
                   CAST(?1 AS INTEGER))
                  FROM process_metric_state WHERE id=1",
                &[Some(&settled.to_string())],
            )?.pop().ok_or(Error::NotFound)?;
            store.0.query(
                c"INSERT OR IGNORE INTO process_metric_rollups
                  (series_id,bucket,cpu_count,cpu_mean,cpu_min,cpu_max,rss_count,rss_mean,rss_min,rss_max,peak_rss_bytes,max_uptime_seconds)
                  SELECT series_id,observed_at/300*300,count(cpu_percent),avg(cpu_percent),min(cpu_percent),max(cpu_percent),
                   count(rss_bytes),avg(rss_bytes),min(rss_bytes),max(rss_bytes),max(peak_rss_bytes),max(uptime_seconds)
                  FROM process_metric_raw, process_metric_state
                  WHERE observed_at>=rolled_until AND observed_at<CAST(?1 AS INTEGER)
                  GROUP BY series_id,observed_at/300",
                &[Some(&end)],
            )?;
            store.0.query(
                c"UPDATE process_metric_state SET rolled_until=max(rolled_until,CAST(?1 AS INTEGER)) WHERE id=1",
                &[Some(&end)],
            )?;
            store.prune_process_metrics(
                now.saturating_sub(RAW_SECONDS), now.saturating_sub(ROLLUP_SECONDS),
            )?;
            store.0.exec(c"DELETE FROM process_metric_series WHERE id IN (
              SELECT id FROM process_metric_series WHERE NOT EXISTS (
                SELECT 1 FROM process_metric_raw WHERE series_id=process_metric_series.id)
                AND NOT EXISTS (SELECT 1 FROM process_metric_rollups WHERE series_id=process_metric_series.id)
              LIMIT 128)")?;
            Ok(())
        })
    }

    fn prune_process_metrics(&self, raw_before: u64, rollup_before: u64) -> Result<(), Error> {
        self.prune_metric_rows(
            c"SELECT sequence FROM process_metric_raw WHERE observed_at<CAST(?1 AS INTEGER)
              AND observed_at<(SELECT rolled_until FROM process_metric_state WHERE id=1)
              ORDER BY observed_at LIMIT 512",
            c"DELETE FROM process_metric_raw WHERE sequence=?1",
            c"UPDATE process_metric_state SET raw_pruned=max(raw_pruned,CAST(?1 AS INTEGER)) WHERE id=1",
            raw_before,
        )?;
        self.prune_metric_rows(
            c"SELECT sequence FROM process_metric_rollups WHERE bucket<=CAST(?1 AS INTEGER)-300 ORDER BY bucket LIMIT 256",
            c"DELETE FROM process_metric_rollups WHERE sequence=?1",
            c"UPDATE process_metric_state SET rollup_pruned=max(rollup_pruned,CAST(?1 AS INTEGER)) WHERE id=1",
            rollup_before,
        )
    }

    fn prune_metric_rows(
        &self,
        select: &CStr,
        delete: &CStr,
        watermark: &CStr,
        before: u64,
    ) -> Result<(), Error> {
        let mut highest = 0;
        for sequence in self.0.query(select, &[Some(&before.to_string())])? {
            highest = highest.max(sequence.parse::<u64>().map_err(|_| Error::Schema)?);
            self.0.query(delete, &[Some(&sequence)])?;
        }
        if highest > 0 {
            self.0.query(watermark, &[Some(&highest.to_string())])?;
        }
        Ok(())
    }

    pub fn process_history(&self, query: HistoryQuery<'_>) -> Result<HistoryPage, Error> {
        self.transaction_with(c"BEGIN", |store| {
            let (state_sql, bounds_sql, rows_sql) = match query.resolution {
                Resolution::Raw => (c"SELECT json_object('lineage',lineage,'pruned',max(raw_pruned,
                    coalesce((SELECT sequence FROM process_metric_raw WHERE observed_at<?1
                      ORDER BY observed_at DESC,sequence DESC LIMIT 1),0)),
                    'high',max(raw_pruned,coalesce((SELECT max(sequence) FROM process_metric_raw),0)))
                    FROM process_metric_state WHERE id=1", RAW_BOUNDS, RAW_HISTORY),
                Resolution::FiveMinutes => (c"SELECT json_object('lineage',lineage,'pruned',max(rollup_pruned,
                    coalesce((SELECT sequence FROM process_metric_rollups WHERE bucket<=CAST(?1 AS INTEGER)-300
                      ORDER BY bucket DESC,sequence DESC LIMIT 1),0)),
                    'high',max(rollup_pruned,coalesce((SELECT max(sequence) FROM process_metric_rollups),0)))
                    FROM process_metric_state WHERE id=1", ROLLUP_BOUNDS, ROLLUP_HISTORY),
            };
            let state: HistoryState = serde_json::from_str(&store.0.query(state_sql,
                &[Some(&query.retention_floor.to_string())])?.pop().ok_or(Error::NotFound)?)?;
            if query.lineage.is_some_and(|lineage| lineage != state.lineage) {
                return Ok(HistoryPage::DifferentLineage);
            }
            if query.lineage.is_some() && query.after < state.pruned {
                return Ok(HistoryPage::Expired(state));
            }
            let bounds: Option<(u64, u64)> = serde_json::from_str(&store.0.query(bounds_sql,
                &[Some(&query.from.to_string()), Some(&query.to.to_string())])?
                .pop().ok_or(Error::NotFound)?)?;
            let mut rows = Vec::new();
            if let Some((first, last)) = bounds {
                let after = query.after.max(state.pruned).max(first.saturating_sub(1));
                let high = query.high.unwrap_or(state.high).min(last);
                if after < high {
                    rows = store.0.query(rows_sql,
                        &[Some(&query.project_id.0.to_string()), query.profile, Some(&after.to_string()),
                        Some(&high.to_string()), Some(&query.limit.to_string())],
                    )?.into_iter().map(|row| serde_json::from_str(&row)).collect::<Result<_,_>>()?;
                }
            }
            Ok(HistoryPage::Rows { state, rows })
        })
    }
}

// Observations are committed in chronological order within each stream.
// Resolve time bounds via the time index before scanning the sequence index.
const RAW_BOUNDS: &CStr = c"SELECT CASE WHEN first IS NOT NULL AND last IS NOT NULL THEN json_array(first,last) ELSE 'null' END
  FROM (SELECT
    (SELECT sequence FROM process_metric_raw WHERE observed_at>=?1 ORDER BY observed_at,sequence LIMIT 1) AS first,
    (SELECT sequence FROM process_metric_raw WHERE observed_at<?2 ORDER BY observed_at DESC,sequence DESC LIMIT 1) AS last)";

const ROLLUP_BOUNDS: &CStr = c"SELECT CASE WHEN first IS NOT NULL AND last IS NOT NULL THEN json_array(first,last) ELSE 'null' END
  FROM (SELECT
    (SELECT sequence FROM process_metric_rollups WHERE bucket>=?1 ORDER BY bucket,sequence LIMIT 1) AS first,
    (SELECT sequence FROM process_metric_rollups WHERE bucket<=CAST(?2 AS INTEGER)-300
      ORDER BY bucket DESC,sequence DESC LIMIT 1) AS last)";

const RAW_HISTORY: &CStr = c"SELECT json_object(
  'sequence',CAST(r.sequence AS TEXT),'kind',s.kind,'profile_id',s.profile_id,'slot',s.slot,
  'instance_id',s.instance_id,'generation',s.generation,
  'observed_at',strftime('%Y-%m-%dT%H:%M:%SZ',r.observed_at,'unixepoch'),
  'cpu_percent',json_object('state',CASE WHEN r.cpu_percent IS NULL THEN 'unknown' ELSE 'fresh' END,'value',r.cpu_percent),
  'rss_bytes',json_object('state',CASE WHEN r.rss_bytes IS NULL THEN 'unknown' ELSE 'fresh' END,'value',r.rss_bytes),
  'uptime_seconds',r.uptime_seconds,'peak_rss_bytes',r.peak_rss_bytes)
  FROM process_metric_raw r JOIN process_metric_series s ON r.series_id=s.id
  WHERE s.project_id=?1 AND (?2 IS NULL OR s.profile_id=?2)
    AND r.sequence>?3 AND r.sequence<=?4
  ORDER BY r.sequence LIMIT ?5";

const ROLLUP_HISTORY: &CStr = c"SELECT json_object(
  'sequence',CAST(r.sequence AS TEXT),'kind',s.kind,'profile_id',s.profile_id,'slot',s.slot,
  'instance_id',s.instance_id,'generation',s.generation,
  'from',strftime('%Y-%m-%dT%H:%M:%SZ',r.bucket,'unixepoch'),
  'to',strftime('%Y-%m-%dT%H:%M:%SZ',r.bucket+300,'unixepoch'),
  'cpu_percent',json_object('state',CASE WHEN r.cpu_count=0 THEN 'unknown' ELSE 'fresh' END,
   'mean',r.cpu_mean,'min',r.cpu_min,'max',r.cpu_max,'samples',r.cpu_count,'expected_samples',150),
  'rss_bytes',json_object('state',CASE WHEN r.rss_count=0 THEN 'unknown' ELSE 'fresh' END,
   'mean',r.rss_mean,'min',r.rss_min,'max',r.rss_max,'samples',r.rss_count,'expected_samples',150),
   'peak_rss_bytes',r.peak_rss_bytes,'max_uptime_seconds',r.max_uptime_seconds)
  FROM process_metric_rollups r JOIN process_metric_series s ON r.series_id=s.id
  WHERE s.project_id=?1 AND (?2 IS NULL OR s.profile_id=?2)
    AND r.sequence>?3 AND r.sequence<=?4
  ORDER BY r.sequence LIMIT ?5";
