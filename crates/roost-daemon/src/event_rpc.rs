use roost_core::ProjectId;
use roost_ipc::{RpcError, events::EventHistoryParams};
use roost_store::{
    Store,
    events::{EventPage, EventQuery},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{DaemonService, internal_error, rpc, store_error};

#[derive(Deserialize, Serialize)]
struct Cursor {
    project: Option<ProjectId>,
    lineage: String,
    after: u64,
    high: Option<u64>,
}

pub(super) fn history(
    service: &DaemonService,
    params: EventHistoryParams,
) -> Result<Value, RpcError> {
    let project = params.project_id;
    let limit = params.limit.unwrap_or(128);
    if !(1..=256).contains(&limit) {
        return Err(RpcError::new(
            rpc::INVALID_PARAMS,
            "event history limit must be 1 to 256",
        ));
    }
    let cursor: Option<Cursor> = params
        .cursor
        .as_deref()
        .map(|cursor| {
            if cursor.len() > 1024 {
                return Err(cursor_error("cursor_invalid"));
            }
            let cursor: Cursor =
                serde_json::from_str(cursor).map_err(|_| cursor_error("cursor_invalid"))?;
            if cursor.project != project {
                return Err(cursor_error("cursor_scope_mismatch"));
            }
            if cursor.after > i64::MAX as u64
                || cursor
                    .high
                    .is_some_and(|high| high < cursor.after || high > i64::MAX as u64)
            {
                return Err(cursor_error("cursor_invalid"));
            }
            Ok(cursor)
        })
        .transpose()?;
    let store = Store::open(&service.db).map_err(store_error)?;
    let page = store
        .event_history(EventQuery {
            project,
            lineage: cursor.as_ref().map(|cursor| cursor.lineage.as_str()),
            after: cursor.as_ref().map_or(0, |cursor| cursor.after),
            high: cursor.as_ref().and_then(|cursor| cursor.high),
            limit: limit + 1,
        })
        .map_err(store_error)?;
    let (state, mut rows) = match page {
        EventPage::Rows { state, rows } => (state, rows),
        EventPage::DifferentLineage => return Err(cursor_error("cursor_lineage_changed")),
        EventPage::Expired { state, earliest } => {
            let recovery_cursor = serde_json::to_string(&Cursor {
                project,
                lineage: state.lineage,
                after: state.pruned,
                high: None,
            })
            .map_err(|_| internal_error())?;
            let mut error = cursor_error("cursor_expired");
            error.data = Some(
                json!({"kind":"cursor_expired", "pruned_through":state.pruned.to_string(), "earliest_retained":earliest, "recovery_cursor":recovery_cursor}),
            );
            return Err(error);
        }
    };
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let high = cursor.and_then(|cursor| cursor.high).unwrap_or(state.high);
    let after = if more {
        rows.last()
            .and_then(|row| row["sequence"].as_str())
            .and_then(|value| value.parse().ok())
            .ok_or_else(internal_error)?
    } else {
        high
    };
    let next = serde_json::to_string(&Cursor {
        project,
        lineage: state.lineage,
        after,
        high: more.then_some(high),
    })
    .map_err(|_| internal_error())?;
    Ok(
        json!({"items": rows, "next_cursor": next, "has_more": more, "pruned_through":state.pruned.to_string()}),
    )
}

fn cursor_error(kind: &str) -> RpcError {
    let mut error = RpcError::new(-32020, "event history cursor could not be resumed");
    error.data = Some(json!({"kind": kind}));
    error
}
