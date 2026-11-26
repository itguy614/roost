use roost_core::{
    ConfigOwner, WorkerProfile,
    alerts::{ConfiguredRule, Episode, Metric},
    unix_timestamp,
};
use roost_ipc::{
    Mutation, RpcError,
    alerts::{AlertListParams, ThresholdProfileParams, ThresholdUpdateParams},
};
use roost_store::{Error, Store};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    DaemonService, alerts::fresh, decode, internal_error, request_key, rpc, store_error,
    supervisor, worker_rpc::worker_mutation,
};

pub(super) enum Call {
    Get(ThresholdProfileParams),
    Update(ThresholdUpdateParams),
    List(AlertListParams),
}

impl Call {
    pub(super) fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "metrics.thresholds.get" => Ok(Self::Get(decode(params)?)),
            "metrics.thresholds.update" => Ok(Self::Update(decode(params)?)),
            "metrics.alerts.list" => Ok(Self::List(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub(super) fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let store = Store::open_for_metric_writes(&service.db).map_err(store_error)?;
        match self {
            Self::Get(params) => {
                let (profile, rules) = store
                    .alert_settings(params.profile_id)
                    .map_err(store_error)?;
                Ok(settings(&profile, rules))
            }
            Self::Update(params) => update(service, &store, params),
            Self::List(params) => list(service, &store, params),
        }
    }
}

fn settings(profile: &WorkerProfile, rules: Vec<ConfiguredRule>) -> Value {
    json!({"profile_id":profile.id,"revision":profile.revision,"notifications":profile.notifications,
        "rules":rules.into_iter().map(|entry|entry.rule).collect::<Vec<_>>()})
}

fn update(
    service: &DaemonService,
    store: &Store,
    params: ThresholdUpdateParams,
) -> Result<Value, RpcError> {
    if params.rules.len() > 2
        || params.rules.iter().any(|rule| !rule.validate())
        || matches!(params.rules.as_slice(), [a,b] if a.metric() == b.metric())
    {
        return Err(RpcError::new(
            rpc::INVALID_PARAMS,
            "provide at most one rule per metric with 0 <= clear_at < breach_at",
        ));
    }
    let request = request_key("metrics.thresholds.update", &params)?;
    let supervisor = service.workers.lock().map_err(|_| internal_error())?;
    let (operation_id, result) = worker_mutation(store, params.idempotency_key, &request, || {
        supervisor.validate_mutation()?;
        let mut profile = store
            .get_worker_profile(params.profile_id)?
            .ok_or(Error::NotFound)?;
        let project = store
            .get_project(profile.project_id)?
            .ok_or(Error::NotFound)?;
        if project.config_owner == ConfigOwner::File {
            return Err(supervisor::Error::Conflict("alert settings are file-owned"));
        }
        if profile.revision != params.expected_revision {
            return Err(Error::RevisionConflict.into());
        }
        let original = store.alert_rules(&[profile.id])?;
        let notifications = params.notifications.unwrap_or(profile.notifications);
        if notifications != profile.notifications
            || original.len() != params.rules.len()
            || original
                .iter()
                .any(|entry| !params.rules.contains(&entry.rule))
        {
            profile.notifications = notifications;
            profile = store.update_worker_profile(profile)?;
            store.replace_alert_rules(&profile, &original, &params.rules, unix_timestamp())?;
        }
        Ok(settings(&profile, store.alert_rules(&[profile.id])?))
    })?;
    crate::encode(Mutation {
        operation_id,
        result,
    })
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    project: roost_core::ProjectId,
    before: u64,
}

fn list(
    service: &DaemonService,
    store: &Store,
    params: AlertListParams,
) -> Result<Value, RpcError> {
    let limit = params.limit.unwrap_or(64);
    if !(1..=128).contains(&limit) {
        return Err(RpcError::new(
            rpc::INVALID_PARAMS,
            "alert list limit must be 1 to 128",
        ));
    }
    let before = params
        .cursor
        .as_deref()
        .map(|value| {
            let error = || RpcError::new(rpc::INVALID_PARAMS, "invalid alert list cursor");
            if value.len() > 256 {
                return Err(error());
            }
            let cursor: Cursor = serde_json::from_str(value).map_err(|_| error())?;
            if cursor.project != params.project_id || cursor.before > i64::MAX as u64 {
                return Err(error());
            }
            Ok(cursor.before)
        })
        .transpose()?;
    store
        .get_project(params.project_id)
        .map_err(store_error)?
        .ok_or_else(|| store_error(Error::NotFound))?;
    let mut rows = store
        .list_alerts(params.project_id, before, limit + 1)
        .map_err(store_error)?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if more {
        let before = rows
            .last()
            .and_then(|row| row["sequence"].as_str())
            .and_then(|value| value.parse().ok())
            .ok_or_else(internal_error)?;
        Some(
            serde_json::to_string(&Cursor {
                project: params.project_id,
                before,
            })
            .map_err(|_| internal_error())?,
        )
    } else {
        None
    };
    let (owned, current) = {
        let supervisor = service.workers.lock().map_err(|_| internal_error())?;
        let owned = supervisor.metric_subjects();
        let current = service
            .process_metrics
            .lock()
            .map_err(|_| internal_error())?
            .clone();
        (owned, current)
    };
    let items: Vec<_> = rows.into_iter().map(|mut row| {
        let episode: Episode = serde_json::from_value(row.clone()).map_err(|_|internal_error())?;
        let active = row["closed_at"].is_null();
        row["state"] = json!(if active {"active"} else {"closed"});
        row["restart"] = if let Some(deferred) = episode.restart_deferred {
            json!(deferred)
        } else { json!(if episode.restart_charged { "attempt_charged" }
            else if !episode.rule.restart() { "off" }
            else if episode.actions_suppressed { "cooldown" }
            else { "attempt_limit" }) };
        let evidence = if active && owned.iter().any(|subject| subject.key == episode.identity) { current.get(&episode.identity).copied() } else { None };
        row["evidence"] = match evidence {
            Some(sample) if fresh(sample) => {
                let value = match episode.rule.metric() {
                    Metric::CpuPercent => json!(sample.sample.cpu_percent),
                    Metric::RssBytes => json!(sample.sample.rss_bytes),
                };
                json!({"state": if value.is_null() { "unknown" } else { "fresh" }, "value":value})
            }
            Some(_) => json!({"state":"stale","value":null}),
            None => json!({"state":"unknown","value":null}),
        };
        Ok(row)
    }).collect::<Result<_, RpcError>>()?;
    Ok(json!({"alerts":items,"next_cursor":next_cursor}))
}
