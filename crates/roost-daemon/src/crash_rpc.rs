use roost_core::{
    WorkerProfileId, scheduler::SchedulerProfileId, worker_runtime::ProcessProfileId,
};
use roost_ipc::{
    RpcError,
    workers::{CrashDetail, CrashOutputState, CrashParams},
};
use roost_store::{Error, Store};
use serde_json::Value;

use crate::{DaemonService, encode, store_error};

pub(super) fn get(
    service: &DaemonService,
    scheduler: bool,
    params: CrashParams,
) -> Result<Value, RpcError> {
    let store = Store::open(&service.db).map_err(store_error)?;
    if scheduler {
        detail::<SchedulerProfileId>(&store, params)
    } else {
        detail::<WorkerProfileId>(&store, params)
    }
}

fn detail<P: ProcessProfileId>(store: &Store, params: CrashParams) -> Result<Value, RpcError> {
    let report = store
        .process_crash::<P>(params.id)
        .map_err(store_error)?
        .ok_or_else(|| store_error(Error::NotFound))?;
    let output_state = if report.output.is_empty() {
        CrashOutputState::Unavailable
    } else {
        CrashOutputState::Captured
    };
    encode(CrashDetail {
        report,
        output_state,
    })
}
