use roost_core::{
    ProjectId,
    metrics::{RAW_SECONDS, ROLLUP_SECONDS},
    queue_metrics::safe_identifier,
    unix_timestamp,
};
use roost_ipc::{
    RpcError,
    metrics::Resolution,
    queue_metrics::{ConfigureQueueMetrics, QueueHistoryParams},
    workers::ProjectParams,
};
use roost_store::{Store, metrics::HistoryPage, queue_metrics::QueueHistory};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    DaemonService, decode, internal_error, request_key, rpc, store_error, worker_rpc::worker_error,
};

pub(super) enum QueueCall {
    Configure(ConfigureQueueMetrics),
    Current(ProjectParams),
    History(QueueHistoryParams),
}

impl QueueCall {
    pub(super) fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "metrics.queue.configure" => Ok(Self::Configure(decode(params)?)),
            "metrics.queue.current" => Ok(Self::Current(decode(params)?)),
            "metrics.queue.history" => Ok(Self::History(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub(super) fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let id = match &self {
            Self::Configure(p) => p.id,
            Self::Current(p) => p.id,
            Self::History(p) => p.project_id,
        };
        let store = Store::open(&service.db).map_err(store_error)?;
        let project = store
            .get_project(id)
            .map_err(store_error)?
            .ok_or_else(|| store_error(roost_store::Error::NotFound))?;
        match self {
            Self::Configure(params) => {
                // Serialize opt-out with final batch publication. Historical
                // receipt replay must not invalidate a newly enabled window.
                let mut cache = service.queue_metrics.lock().map_err(|_| internal_error())?;
                let key = request_key("metrics.queue.configure", &params)?;
                let mut changed = false;
                let (operation_id, result) = store
                    .try_mutate::<_, crate::supervisor::Error>(
                        params.idempotency_key,
                        &key,
                        |store| {
                            changed = store.queue_probe_token(id)?.is_some() != params.enabled;
                            store.configure_queue_probe(id, params.enabled)?;
                            Ok(json!({"project_id":id,"enabled":params.enabled}))
                        },
                    )
                    .map_err(worker_error)?;
                if changed {
                    cache.invalidate(id);
                }
                Ok(json!({"operation_id":operation_id,"result":result}))
            }
            Self::Current(_) => {
                let token = store.queue_probe_token(id).map_err(store_error)?;
                let queues = if let Some(token) = &token {
                    let selectors = store.queue_probe_selectors(id).map_err(store_error)?;
                    service
                        .queue_metrics
                        .lock()
                        .map_err(|_| internal_error())?
                        .render(&project, token, &selectors, unix_timestamp())
                } else {
                    Vec::new()
                };
                Ok(json!({"project_id":id,"enabled":token.is_some(),"queues":queues}))
            }
            Self::History(p) => history(&store, p),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Scope {
    stream: String,
    project_id: ProjectId,
    connection: Option<String>,
    queue: Option<String>,
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

fn history(store: &Store, params: QueueHistoryParams) -> Result<Value, RpcError> {
    let invalid = |message| RpcError::new(rpc::INVALID_PARAMS, message);
    if [&params.connection, &params.queue]
        .into_iter()
        .any(|name| name.as_deref().is_some_and(|name| !safe_identifier(name)))
    {
        return Err(invalid(
            "history filters require symbolic connection and queue names",
        ));
    }
    let scope = Scope {
        stream: "queue".into(),
        project_id: params.project_id,
        connection: params.connection,
        queue: params.queue,
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
                return Err(cursor_error(
                    "cursor_invalid",
                    "invalid queue history cursor",
                ));
            }
            serde_json::from_str(cursor)
                .map_err(|_| cursor_error("cursor_invalid", "invalid queue history cursor"))
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
            return Err(cursor_error(
                "cursor_invalid",
                "invalid queue history cursor",
            ));
        }
    }
    let to = cursor
        .as_ref()
        .and_then(|c| c.scan_to)
        .or(scope.to)
        .unwrap_or(now.saturating_add(1));
    if !(1..=256).contains(&limit)
        || scope.from >= to
        || to > i64::MAX as u64
        || to - scope.from > ROLLUP_SECONDS + 1
    {
        return Err(invalid(
            "history needs a range of at most seven days and a limit of 1 to 256",
        ));
    }
    let (resolution, retention) = match scope.resolution {
        Resolution::Raw => (roost_store::metrics::Resolution::Raw, RAW_SECONDS),
        Resolution::FiveMinutes => (
            roost_store::metrics::Resolution::FiveMinutes,
            ROLLUP_SECONDS,
        ),
    };
    let retention_floor = now.saturating_sub(retention);
    let result = store
        .queue_metric_history(QueueHistory {
            project: scope.project_id,
            connection: scope.connection.as_deref(),
            queue: scope.queue.as_deref(),
            resolution,
            from: scope.from,
            to,
            after: cursor.as_ref().map_or(0, |c| c.after),
            high: cursor.as_ref().and_then(|c| c.high),
            limit: limit + 1,
            lineage: cursor.as_ref().map(|c| c.lineage.as_str()),
            retention_floor,
        })
        .map_err(store_error)?;
    let (state, mut rows) = match result {
        HistoryPage::Rows { state, rows } => (state, rows),
        HistoryPage::DifferentLineage => {
            return Err(cursor_error(
                "cursor_lineage_changed",
                "start history again for this database",
            ));
        }
        HistoryPage::Expired(state) => {
            let recovery_cursor = serialize(&Cursor {
                scope,
                lineage: state.lineage,
                after: state.pruned,
                high: None,
                scan_to: None,
            })?;
            let mut error = cursor_error(
                "cursor_expired",
                "retention expired records after this cursor",
            );
            error.data = Some(
                json!({"kind":"cursor_expired","recovery_cursor":recovery_cursor,
                "retention_floor":retention_floor,"pruned_through":state.pruned.to_string()}),
            );
            return Err(error);
        }
    };
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let high = cursor.as_ref().and_then(|c| c.high).unwrap_or(state.high);
    let after = if has_more {
        rows.last()
            .and_then(|row| row["sequence"].as_str())
            .and_then(|sequence| sequence.parse().ok())
            .ok_or_else(internal_error)?
    } else {
        high
    };
    let resolution = scope.resolution;
    let next_cursor = serialize(&Cursor {
        scope,
        lineage: state.lineage,
        after,
        high: has_more.then_some(high),
        scan_to: has_more.then_some(to),
    })?;
    Ok(
        json!({"resolution":resolution,"items":rows,"next_cursor":next_cursor,"has_more":has_more,"retention_floor":retention_floor}),
    )
}

fn serialize(cursor: &Cursor) -> Result<String, RpcError> {
    serde_json::to_string(cursor).map_err(|_| internal_error())
}

fn cursor_error(kind: &str, message: &str) -> RpcError {
    let mut error = RpcError::new(-32020, message);
    error.data = Some(json!({"kind":kind}));
    error
}
