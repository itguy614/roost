use roost_core::{
    ConfigOwner, DesiredState, Revision,
    scheduler::{SchedulerIntent, SchedulerMode, SchedulerProfile, SchedulerProfileId},
};
use roost_ipc::{
    Mutation, RpcError,
    scheduler::{ConfigureSchedulerParams, SchedulerLaunchParams},
    workers::{Intent, ProjectParams, StopParams},
};
use roost_store::{Error as StoreError, Store};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    DaemonService, decode, encode, internal_error, request_key, rpc, store_error, supervisor,
    worker_rpc::{require_project, worker_error, worker_mutation},
};

pub enum SchedulerCall {
    Configure(ConfigureSchedulerParams),
    Status(ProjectParams),
    Launch(SchedulerLaunchParams, bool),
    Stop(StopParams),
    Logs(ProjectParams),
    Crashes(ProjectParams),
}

impl SchedulerCall {
    pub fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "scheduler.configure" => Ok(Self::Configure(decode(params)?)),
            "scheduler.status" => Ok(Self::Status(decode(params)?)),
            "scheduler.start" => Ok(Self::Launch(decode(params)?, false)),
            "scheduler.restart" => Ok(Self::Launch(decode(params)?, true)),
            "scheduler.stop" => Ok(Self::Stop(decode(params)?)),
            "scheduler.logs" => Ok(Self::Logs(decode(params)?)),
            "scheduler.crashes" => Ok(Self::Crashes(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let mut supervisor = service.workers.lock().map_err(|_| internal_error())?;
        let store = Store::open(&service.db).map_err(store_error)?;
        match self {
            Self::Configure(params) => {
                let key = request_key("scheduler.configure", &params)?;
                supervisor
                    .clean_previous_scheduler(&store, params.id)
                    .map_err(worker_error)?;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        let project = store.get_project(params.id)?.ok_or(StoreError::NotFound)?;
                        if project.config_owner == ConfigOwner::File {
                            return Err(supervisor::Error::Conflict(
                                "scheduler settings are file-owned",
                            ));
                        }
                        supervisor.scheduler_clear_for_change(&store, params.id)?;
                        if store.scheduler_intent(params.id)?.desired() == DesiredState::Running {
                            return Err(supervisor::Error::Conflict(
                                "stop the saved scheduler intent before configuring it",
                            ));
                        }
                        configure(&store, params)
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Status(params) => {
                require_project(&store, params.id).map_err(worker_error)?;
                encode(
                    supervisor
                        .scheduler_status(&store, params.id)
                        .map_err(worker_error)?,
                )
            }
            Self::Launch(params, restart) => {
                let method = if restart {
                    "scheduler.restart"
                } else {
                    "scheduler.start"
                };
                let key = request_key(method, &params)?;
                supervisor
                    .clean_previous_scheduler(&store, params.id)
                    .map_err(worker_error)?;
                let mut plan = None;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        plan = supervisor.prepare_scheduler(
                            &store,
                            params.id,
                            &params.php,
                            restart,
                        )?;
                        if let Some(plan) = &plan {
                            store.set_scheduler_intent(
                                params.id,
                                &SchedulerIntent::Running {
                                    php: plan.php.clone(),
                                },
                            )?;
                        }
                        Ok(Intent {
                            id: params.id,
                            desired_state: DesiredState::Running,
                        })
                    })?;
                if let Some(plan) = plan {
                    supervisor.start_scheduler(params.id, plan);
                }
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Stop(params) => {
                let key = request_key("scheduler.stop", &params)?;
                let mut apply = false;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        require_project(&store, params.id)?;
                        supervisor.validate_mutation()?;
                        if store.project_scheduler(params.id)?.is_none() {
                            return Err(supervisor::Error::Conflict("scheduler is not configured"));
                        }
                        store.set_scheduler_intent(params.id, &SchedulerIntent::Stopped)?;
                        apply = true;
                        Ok(Intent {
                            id: params.id,
                            desired_state: DesiredState::Stopped,
                        })
                    })?;
                if apply {
                    supervisor.stop_scheduler(params.id);
                }
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Logs(params) => {
                require_project(&store, params.id).map_err(worker_error)?;
                encode(supervisor.scheduler_logs(params.id))
            }
            Self::Crashes(params) => {
                require_project(&store, params.id).map_err(worker_error)?;
                supervisor
                    .clean_previous_scheduler(&store, params.id)
                    .map_err(worker_error)?;
                encode(
                    store
                        .recent_process_crashes::<SchedulerProfileId>(params.id)
                        .map_err(store_error)?,
                )
            }
        }
    }
}

fn configure(
    store: &Store,
    params: ConfigureSchedulerParams,
) -> Result<SchedulerProfile, supervisor::Error> {
    let options = params.options;
    if let Some(mut profile) = store.project_scheduler(params.id)? {
        if params.expected_revision != Some(profile.revision) {
            return Err(StoreError::RevisionConflict.into());
        }
        profile.enabled = options.enabled;
        profile.crash_restart = options.crash_restart;
        profile.graceful_stop_seconds = options.graceful_stop_seconds;
        return Ok(store.update_scheduler(profile)?);
    }
    if params.expected_revision.is_some() {
        return Err(StoreError::RevisionConflict.into());
    }
    let profile = SchedulerProfile {
        id: SchedulerProfileId(Uuid::new_v4()),
        project_id: params.id,
        enabled: options.enabled,
        mode: SchedulerMode::ScheduleWork,
        crash_restart: options.crash_restart,
        graceful_stop_seconds: options.graceful_stop_seconds,
        revision: Revision(1),
    };
    store.create_scheduler(&profile)?;
    Ok(profile)
}
