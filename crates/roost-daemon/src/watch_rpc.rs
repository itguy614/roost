use roost_core::{ConfigOwner, ProjectId, Revision, WatchRules};
use roost_ipc::{
    Mutation, RpcError,
    watch::{CodeRestartParams, DisableWatchParams, EnableWatchParams, WatchState, WatchStatus},
    workers::ProjectParams,
};
use roost_store::{Error as StoreError, Store};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    DaemonService, decode, encode, internal_error, request_key, rpc, store_error, supervisor,
    worker_rpc::{worker_error, worker_mutation},
};

pub(super) enum WatchCall {
    Status(ProjectParams),
    Enable(EnableWatchParams),
    Disable(DisableWatchParams),
    Policy(CodeRestartParams),
}

impl WatchCall {
    pub(super) fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "watch.status" => Ok(Self::Status(decode(params)?)),
            "watch.enable" => Ok(Self::Enable(decode(params)?)),
            "watch.disable" => Ok(Self::Disable(decode(params)?)),
            "worker.profile.setCodeRestart" => Ok(Self::Policy(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub(super) fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let store = Store::open(&service.db).map_err(store_error)?;
        match self {
            Self::Status(params) => {
                let project = store
                    .get_project(params.id)
                    .map_err(store_error)?
                    .ok_or_else(|| store_error(StoreError::NotFound))?;
                let runtime = service.watch_status.lock().map_err(|_| internal_error())?;
                let runtime = runtime.get(&params.id).filter(|runtime| {
                    project.watch_enabled && runtime.revision == project.revision
                });
                let state = runtime.map_or(
                    if project.watch_enabled {
                        WatchState::Initializing
                    } else {
                        WatchState::Disabled
                    },
                    |runtime| runtime.state,
                );
                encode(WatchStatus {
                    id: params.id,
                    revision: project.revision,
                    state,
                    rules: project.watch_rules,
                    pending: runtime.is_some_and(|runtime| runtime.pending),
                    problem: runtime
                        .and_then(|runtime| runtime.problem)
                        .map(str::to_owned),
                })
            }
            Self::Enable(params) => {
                let key = request_key("watch.enable", &params)?;
                let rules = params.rules;
                set_enabled(
                    service,
                    &store,
                    &key,
                    params.idempotency_key,
                    WatchChange {
                        id: params.id,
                        revision: params.expected_revision,
                        enabled: true,
                        rules,
                    },
                )
            }
            Self::Disable(params) => {
                let key = request_key("watch.disable", &params)?;
                set_enabled(
                    service,
                    &store,
                    &key,
                    params.idempotency_key,
                    WatchChange {
                        id: params.id,
                        revision: params.expected_revision,
                        enabled: false,
                        rules: None,
                    },
                )
            }
            Self::Policy(params) => {
                let key = request_key("worker.profile.setCodeRestart", &params)?;
                let supervisor = service.workers.lock().map_err(|_| internal_error())?;
                let profile = store.get_worker_profile(params.id).map_err(store_error)?;
                if let Some(profile) = &profile {
                    supervisor
                        .clean_previous(&store, profile.project_id)
                        .map_err(worker_error)?;
                }
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        let mut profile = profile.ok_or(StoreError::NotFound)?;
                        let project = store
                            .get_project(profile.project_id)?
                            .ok_or(StoreError::NotFound)?;
                        require_local(project.config_owner)?;
                        if profile.revision != params.expected_revision {
                            return Err(StoreError::RevisionConflict.into());
                        }
                        supervisor.validate_config_change(&store, profile.project_id)?;
                        if profile.code_restart != params.policy {
                            profile.code_restart = params.policy;
                            profile = store.update_worker_profile(profile)?;
                        }
                        Ok(profile)
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
        }
    }
}

struct WatchChange {
    id: ProjectId,
    revision: Revision,
    enabled: bool,
    rules: Option<WatchRules>,
}

fn set_enabled(
    service: &DaemonService,
    store: &Store,
    key: &str,
    operation: Option<Uuid>,
    change: WatchChange,
) -> Result<Value, RpcError> {
    let supervisor = service.workers.lock().map_err(|_| internal_error())?;
    let (operation_id, result) = worker_mutation(store, operation, key, || {
        supervisor.validate_mutation()?;
        let mut project = store.get_project(change.id)?.ok_or(StoreError::NotFound)?;
        require_local(project.config_owner)?;
        if change.revision != project.revision {
            return Err(StoreError::RevisionConflict.into());
        }
        let rules = change.rules.unwrap_or_else(|| project.watch_rules.clone());
        if change.enabled {
            roost_watch::Rules::compile(&rules).map_err(|_| supervisor::Error::Invalid(
                "watch rules must be valid project-relative globs (at most 64 rules of 256 bytes)"
            ))?;
            if !project.watch_enabled
                && store
                    .list_projects()?
                    .iter()
                    .filter(|p| p.watch_enabled)
                    .count()
                    >= roost_watch::MAX_PROJECTS
            {
                return Err(supervisor::Error::Conflict(
                    "daemon watch capacity is 16 enabled projects",
                ));
            }
        }
        if project.watch_enabled != change.enabled || project.watch_rules != rules {
            project.watch_enabled = change.enabled;
            project.watch_rules = rules;
            project = store.update_project(project)?;
        }
        Ok(project)
    })?;
    encode(Mutation {
        operation_id,
        result,
    })
}

fn require_local(owner: ConfigOwner) -> Result<(), supervisor::Error> {
    if owner == ConfigOwner::File {
        return Err(supervisor::Error::Conflict("watch settings are file-owned"));
    }
    Ok(())
}
