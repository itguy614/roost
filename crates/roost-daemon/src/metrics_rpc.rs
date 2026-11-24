use std::collections::HashSet;

use roost_core::{
    DesiredState, ProjectId,
    metrics::{MetricSubject, ProcessKind, RAW_SECONDS, ROLLUP_SECONDS},
    scheduler::SchedulerProfileId,
    unix_timestamp,
    worker_runtime::WorkerLeaseState,
};
use roost_ipc::{
    RpcError,
    metrics::{HistoryParams, Resolution},
    workers::ProjectParams,
};
use roost_store::{
    Store,
    metrics::{HistoryPage, HistoryQuery},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{DaemonService, decode, internal_error, metrics::Observation, rpc, store_error};

const MAX_PAGE: u32 = 256;
const CURSOR_ERROR: i32 = -32020;

pub(super) enum MetricsCall {
    Current(ProjectParams),
    History(HistoryParams),
    Summary(ProjectParams),
}

impl MetricsCall {
    pub(super) fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "metrics.process.current" => Ok(Self::Current(decode(params)?)),
            "metrics.process.history" => Ok(Self::History(decode(params)?)),
            "metrics.project.summary" => Ok(Self::Summary(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub(super) fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let id = match &self {
            Self::Current(p) | Self::Summary(p) => p.id,
            Self::History(p) => p.project_id,
        };
        let store = Store::open(&service.db).map_err(store_error)?;
        if store.get_project(id).map_err(store_error)?.is_none() {
            return Err(store_error(roost_store::Error::NotFound));
        }
        match self {
            Self::History(params) => history(&store, params),
            Self::Current(_) => {
                let (samples, _) = current(service, id)?;
                Ok(json!({"project_id": id, "processes": samples.into_iter()
                    .map(|(subject, sample)| render(subject, sample)).collect::<Vec<_>>() }))
            }
            Self::Summary(_) => summary(service, &store, id),
        }
    }
}

type Current = Vec<(MetricSubject, Option<Observation>)>;

fn current(service: &DaemonService, id: ProjectId) -> Result<(Current, HashSet<Uuid>), RpcError> {
    let supervisor = service.workers.lock().map_err(|_| internal_error())?;
    let stored = service
        .process_metrics
        .lock()
        .map_err(|_| internal_error())?;
    let mut current: Current = supervisor
        .metric_subjects()
        .into_iter()
        .filter(|subject| subject.key.project_id == id)
        .map(|subject| (subject, stored.get(&subject.key).copied()))
        .collect();
    current.sort_unstable_by_key(|(subject, _)| (subject.key.profile_id, subject.key.slot));
    Ok((current, supervisor.owned_metric_leases()))
}

pub(super) fn timestamp(seconds: u64) -> Option<String> {
    time::OffsetDateTime::from_unix_timestamp(i64::try_from(seconds).ok()?)
        .ok()?
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

fn stale(observation: Observation) -> bool {
    observation.at.elapsed().as_secs() >= 6
        || unix_timestamp().saturating_sub(observation.sample.observed_at) >= 6
}

fn gauge<T: Serialize>(value: Option<T>, stale: bool, reason: &str) -> Value {
    if stale {
        json!({"state":"stale", "value":null, "reason":"sampling_gap"})
    } else if let Some(value) = value {
        json!({"state":"fresh", "value":value})
    } else {
        json!({"state":"unknown", "value":null, "reason":reason})
    }
}

fn render(subject: MetricSubject, observed: Option<Observation>) -> Value {
    let expired = observed.is_some_and(stale);
    let reading = observed.map(|observation| observation.sample);
    json!({
        "identity":subject.key,
        "observed_at":reading.and_then(|sample| timestamp(sample.observed_at)),
        "cpu_percent":gauge(reading.and_then(|sample| sample.cpu_percent),expired,
            observed.and_then(|o| o.cpu_reason).unwrap_or("awaiting_sample")),
        "rss_bytes":gauge(reading.and_then(|sample| sample.rss_bytes),expired,"process_unavailable"),
        "uptime_seconds":gauge(reading.and_then(|sample| sample.uptime_seconds),expired,"process_unavailable"),
        "peak_rss_bytes":gauge(reading.and_then(|sample| sample.peak_rss_bytes),expired,"awaiting_sample")
    })
}

fn summary(service: &DaemonService, store: &Store, id: ProjectId) -> Result<Value, RpcError> {
    let (current, owned) = current(service, id)?;
    let worker_running = current
        .iter()
        .filter(|(subject, _)| subject.key.kind == ProcessKind::Worker)
        .count();
    let scheduler_running = current.len() - worker_running;
    let worker_desired =
        store.project_intent(id).map_err(store_error)?.desired() == DesiredState::Running;
    let scheduler_desired =
        store.scheduler_intent(id).map_err(store_error)?.desired() == DesiredState::Running;
    let workers = store.list_worker_profiles(id).map_err(store_error)?;
    let scheduler = store.project_scheduler(id).map_err(store_error)?;
    let mut profiles = Vec::new();
    for worker in &workers {
        let desired = if worker_desired && worker.enabled {
            worker.desired_instances
        } else {
            0
        };
        profiles.push(json!({"profile_id":worker.id, "kind":"worker", "desired":desired,
            "running":current.iter().filter(|(subject, _)| subject.key.profile_id == worker.id.0).count(),
            "lifecycle":store.process_lifecycle_totals(id, Some(&worker.id.0.to_string())).map_err(store_error)?}));
    }
    if let Some(scheduler) = &scheduler {
        profiles.push(json!({"profile_id":scheduler.id, "kind":"scheduler",
            "desired":u8::from(scheduler_desired && scheduler.enabled), "running":scheduler_running,
            "lifecycle":store.process_lifecycle_totals(id, Some(&scheduler.id.0.to_string())).map_err(store_error)?}));
    }
    let worker_unresolved = store
        .project_leases::<roost_core::WorkerProfileId>(id)
        .map_err(store_error)?
        .iter()
        .filter(|lease| unresolved(&owned, lease.id, &lease.state))
        .count();
    let scheduler_unresolved = store
        .project_leases::<SchedulerProfileId>(id)
        .map_err(store_error)?
        .iter()
        .filter(|lease| unresolved(&owned, lease.id, &lease.state))
        .count();
    let mut cpu = (0.0, 0);
    let mut rss = (0_u64, 0);
    for sample in current
        .iter()
        .filter_map(|(_, sample)| *sample)
        .filter(|s| !stale(*s))
    {
        if let Some(value) = sample.sample.cpu_percent {
            cpu.0 += value;
            cpu.1 += 1;
        }
        if let Some(value) = sample.sample.rss_bytes {
            rss.0 = rss.0.saturating_add(value);
            rss.1 += 1;
        }
    }
    let desired_workers: u64 = workers
        .iter()
        .filter(|p| worker_desired && p.enabled)
        .map(|p| u64::from(p.desired_instances))
        .sum();
    Ok(json!({
        "project_id":id,
        "workers":{"desired":desired_workers, "running":worker_running, "unresolved":worker_unresolved},
        "scheduler":{"desired":u8::from(scheduler_desired && scheduler.is_some_and(|p| p.enabled)),
                     "running":scheduler_running, "unresolved":scheduler_unresolved},
        "cpu_percent":{"state":coverage(cpu.1,current.len()), "value":(cpu.1>0).then_some(cpu.0), "sampled":cpu.1, "expected":current.len()},
        "rss_bytes":{"state":coverage(rss.1,current.len()), "value":(rss.1>0).then_some(rss.0), "sampled":rss.1, "expected":current.len()},
        "lifecycle":store.process_lifecycle_totals(id, None).map_err(store_error)?,
        "profiles":profiles
    }))
}

fn coverage(sampled: usize, expected: usize) -> &'static str {
    match sampled {
        0 => "unknown",
        count if count < expected => "partial",
        _ => "fresh",
    }
}

fn unresolved(owned: &HashSet<Uuid>, id: Uuid, state: &WorkerLeaseState) -> bool {
    !state.is_complete()
        && (!owned.contains(&id) || matches!(state, WorkerLeaseState::Uncertain { .. }))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Scope {
    project_id: ProjectId,
    profile_id: Option<Uuid>,
    resolution: Resolution,
    from: u64,
    to: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    scope: Scope,
    lineage: String,
    after: u64,
    high: Option<u64>,
    scan_to: Option<u64>,
}

fn history(store: &Store, params: HistoryParams) -> Result<Value, RpcError> {
    let scope = Scope {
        project_id: params.project_id,
        profile_id: params.profile_id,
        resolution: params.resolution,
        from: params.from,
        to: params.to,
    };
    let now = unix_timestamp();
    let limit = params.limit.unwrap_or(128);
    let cursor: Option<Cursor> = params
        .cursor
        .as_deref()
        .map(|cursor| {
            if cursor.len() > 1024 {
                return Err(cursor_error("cursor_invalid", "invalid history cursor"));
            }
            serde_json::from_str(cursor)
                .map_err(|_| cursor_error("cursor_invalid", "invalid history cursor"))
        })
        .transpose()?;
    if let Some(cursor) = &cursor {
        if cursor.scope != scope {
            return Err(cursor_error(
                "cursor_scope_mismatch",
                "use a new cursor for this history scope",
            ));
        }
        if cursor.after > i64::MAX as u64
            || cursor.high.is_some() != cursor.scan_to.is_some()
            || cursor
                .high
                .is_some_and(|high| high < cursor.after || high > i64::MAX as u64)
        {
            return Err(cursor_error("cursor_invalid", "invalid history cursor"));
        }
    }
    let to = cursor
        .as_ref()
        .and_then(|cursor| cursor.scan_to)
        .or(scope.to)
        .unwrap_or(now.saturating_add(1));
    if !(1..=MAX_PAGE).contains(&limit)
        || scope.from >= to
        || to > i64::MAX as u64
        || to - scope.from > ROLLUP_SECONDS + 1
    {
        return Err(RpcError::new(
            rpc::INVALID_PARAMS,
            "history needs a bounded range of at most seven days and a limit of 1 to 256",
        ));
    }
    let retention = match scope.resolution {
        Resolution::Raw => RAW_SECONDS,
        Resolution::FiveMinutes => ROLLUP_SECONDS,
    };
    let page = store
        .process_history(HistoryQuery {
            project_id: scope.project_id,
            profile: scope.profile_id.map(|id| id.to_string()).as_deref(),
            resolution: match scope.resolution {
                Resolution::Raw => roost_store::metrics::Resolution::Raw,
                Resolution::FiveMinutes => roost_store::metrics::Resolution::FiveMinutes,
            },
            from: scope.from,
            to,
            after: cursor.as_ref().map_or(0, |cursor| cursor.after),
            retention_floor: now.saturating_sub(retention),
            high: cursor.as_ref().and_then(|cursor| cursor.high),
            lineage: cursor.as_ref().map(|cursor| cursor.lineage.as_str()),
            limit: limit + 1,
        })
        .map_err(store_error)?;
    let (state, mut rows) = match page {
        HistoryPage::DifferentLineage => {
            return Err(cursor_error(
                "cursor_lineage_changed",
                "start history again for this database",
            ));
        }
        HistoryPage::Expired(state) => {
            let recovery = serde_json::to_string(&Cursor {
                scope,
                lineage: state.lineage,
                after: state.pruned,
                high: None,
                scan_to: None,
            })
            .map_err(|_| internal_error())?;
            let mut error = cursor_error(
                "cursor_expired",
                "retention expired records after this cursor",
            );
            error.data = Some(
                json!({"kind":"cursor_expired", "retention_floor":now.saturating_sub(retention),
                "recovery_cursor":recovery, "pruned_through":state.pruned.to_string()}),
            );
            return Err(error);
        }
        HistoryPage::Rows { state, rows } => (state, rows),
    };
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let high = cursor
        .as_ref()
        .and_then(|cursor| cursor.high)
        .unwrap_or(state.high);
    let after = if more {
        rows.last()
            .and_then(|row| row["sequence"].as_str())
            .and_then(|s| s.parse().ok())
            .ok_or_else(internal_error)?
    } else {
        high
    };
    let resolution = scope.resolution;
    let next = serde_json::to_string(&Cursor {
        scope,
        lineage: state.lineage,
        after,
        high: more.then_some(high),
        scan_to: more.then_some(to),
    })
    .map_err(|_| internal_error())?;
    Ok(
        json!({"resolution":resolution, "items":rows, "next_cursor":next, "has_more":more,
        "retention_floor":now.saturating_sub(retention)}),
    )
}

fn cursor_error(kind: &str, message: &str) -> RpcError {
    let mut error = RpcError::new(CURSOR_ERROR, message);
    error.data = Some(json!({"kind":kind}));
    error
}
