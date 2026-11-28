use roost_core::{ProjectId, Revision};
use roost_ipc::{
    Mutation,
    mutations::{LaunchFence, ReceiptParams},
};
use roost_store::{Error as StoreError, Store};
use serde_json::{Value, json};

use crate::{DaemonService, RpcError, encode, internal_error, rpc, store_error, supervisor};

pub(crate) fn fence(service: &DaemonService, id: ProjectId) -> Result<Value, RpcError> {
    let _workers = service.workers.lock().map_err(|_| internal_error())?;
    let store = Store::open_without_busy_wait(&service.db).map_err(store_error)?;
    encode(
        store
            .transaction_read(|store| snapshot(service, store, id))
            .map_err(store_error)?,
    )
}

fn snapshot(
    service: &DaemonService,
    store: &Store,
    project_id: ProjectId,
) -> Result<LaunchFence, StoreError> {
    let project = store.get_project(project_id)?.ok_or(StoreError::NotFound)?;
    let workers = store
        .list_worker_profiles(project_id)?
        .into_iter()
        .map(|profile| (profile.id, profile.revision))
        .collect();
    let scheduler = store
        .project_scheduler(project_id)?
        .map(|profile| (profile.id, profile.revision));
    Ok(LaunchFence {
        daemon_id: service.daemon_id,
        project_id,
        project_revision: project.revision,
        workers,
        scheduler,
    })
}

/// The caller runs this inside the same mutation transaction as intent and
/// receipt commit. CLI callers without a fence keep the original v1 behavior.
pub(crate) fn validate_launch(
    service: &DaemonService,
    store: &Store,
    id: ProjectId,
    expected: Option<&LaunchFence>,
) -> Result<(), supervisor::Error> {
    if let Some(expected) = expected
        && &snapshot(service, store, id)? != expected
    {
        return Err(supervisor::Error::Conflict(
            "prepared launch no longer matches daemon or project configuration",
        ));
    }
    Ok(())
}

pub(crate) fn validate_revision(
    store: &Store,
    id: ProjectId,
    expected: Option<Revision>,
) -> Result<(), supervisor::Error> {
    if let Some(revision) = expected {
        let project = store.get_project(id)?.ok_or(StoreError::NotFound)?;
        if project.revision != revision {
            return Err(StoreError::RevisionConflict.into());
        }
    }
    Ok(())
}

pub(crate) fn receipt(service: &DaemonService, request: ReceiptParams) -> Result<Value, RpcError> {
    if request
        .params
        .get("idempotency_key")
        .and_then(Value::as_str)
        .and_then(|key| key.parse().ok())
        != Some(request.operation_id)
    {
        return Err(RpcError::new(
            rpc::INVALID_PARAMS,
            "receipt request requires the original idempotency key",
        ));
    }
    // All desktop mutations serialize on this lock. Receipt lookup waits for
    // any currently dispatching operation to finish before checking SQLite.
    let _workers = service.workers.lock().map_err(|_| internal_error())?;
    let store = Store::open_without_busy_wait(&service.db).map_err(store_error)?;
    let result = store
        .mutation_receipt(
            request.operation_id,
            &json!([request.method, request.params]),
        )
        .map_err(store_error)?;
    encode(result.map(|result| Mutation {
        operation_id: request.operation_id,
        result,
    }))
}
