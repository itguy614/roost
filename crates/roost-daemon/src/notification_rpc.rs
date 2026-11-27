use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};

use roost_ipc::{RpcError, notifications::EventPolicyParams};
use roost_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{DaemonService, internal_error, store_error};

const STATUS_LIMIT: u64 = 8192;

pub(super) fn policy(
    service: &DaemonService,
    params: EventPolicyParams,
) -> Result<Value, RpcError> {
    let store = Store::open_for_metric_writes(&service.db).map_err(store_error)?;
    let policy = store
        .notification_policy(params.id, &params.lineage)
        .map_err(store_error)?;
    Ok(json!({"policy":policy}))
}

#[derive(Deserialize, Serialize)]
struct Status {
    version: u8,
    pid: u32,
    updated_at: u64,
    connection: Connection,
    permission: Permission,
    delivery: Option<DeliveryState>,
    last_received_event_id: Option<Uuid>,
    last_submission: Option<Outcome>,
    last_gap: Option<Gap>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Connection {
    Connecting,
    Connected,
    Disconnected,
    Incompatible,
    Degraded,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Permission {
    Unknown,
    Undetermined,
    Denied,
    Authorized,
    Provisional,
    Ephemeral,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeliveryState {
    Operational,
    WaitingForAuthorization,
    JournalUncertain,
}

#[derive(Deserialize, Serialize)]
struct Outcome {
    event_id: Uuid,
    at: u64,
    state: Submission,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Submission {
    Accepted,
    Denied,
    OptedOut,
    Suppressed,
    Unsupported,
    Expired,
    Unavailable,
    Failed,
}

#[derive(Deserialize, Serialize)]
struct Gap {
    at: u64,
    kind: GapKind,
    pruned_through: Option<String>,
    earliest_retained: Option<roost_store::events::EventBoundary>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum GapKind {
    Retention,
    LineageChanged,
}

pub(super) fn status(service: &DaemonService) -> Result<Value, RpcError> {
    let file = service.db.with_file_name("notification-helper-status.json");
    let status = (|| {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(file)
            .ok()?;
        let metadata = file.metadata().ok()?;
        if !metadata.is_file()
            || metadata.uid() != roost_ipc::endpoint::effective_uid()
            || metadata.mode() & 0o777 != 0o600
            || metadata.len() > STATUS_LIMIT
        {
            return None;
        }
        let mut buffer = Vec::new();
        file.take(STATUS_LIMIT + 1).read_to_end(&mut buffer).ok()?;
        if buffer.len() as u64 > STATUS_LIMIT {
            return None;
        }
        let status: Status = serde_json::from_slice(&buffer).ok()?;
        (status.version == 1).then_some(status)
    })();
    let Some(status) = status else {
        return Ok(
            json!({"connection":"unknown", "permission":"unknown", "delivery":"unknown", "last_seen_at":null,
            "last_received_event_id":null, "last_submission":null, "last_gap":null, "presentation":"unknown"}),
        );
    };
    let now = roost_core::unix_timestamp();
    let fresh = now
        .checked_sub(status.updated_at)
        .is_some_and(|age| age <= 12);
    let connection = if fresh {
        serde_json::to_value(status.connection).map_err(|_| internal_error())?
    } else {
        json!("stale")
    };
    let permission = if fresh {
        serde_json::to_value(status.permission).map_err(|_| internal_error())?
    } else {
        json!("unknown")
    };
    let delivery = match (fresh, status.delivery) {
        (true, Some(state)) => serde_json::to_value(state).map_err(|_| internal_error())?,
        _ => json!("unknown"),
    };
    Ok(
        json!({"connection":connection, "permission":permission, "delivery":delivery, "pid":status.pid,
        "last_seen_at":crate::metrics_rpc::timestamp(status.updated_at), "last_received_event_id":status.last_received_event_id,
        "last_submission":status.last_submission, "last_gap":status.last_gap, "presentation":"unknown"}),
    )
}
