use roost_core::{
    CodeRestartPolicy, DesiredState, ProjectId, Revision, WorkerProfile, WorkerProfileId,
    worker_runtime::{MAX_GRACEFUL_STOP_SECONDS, MAX_PROFILE_INSTANCES},
};
use roost_ipc::{
    Mutation, RpcError,
    workers::{CreateWorkerParams, Intent, LaunchParams, ProjectParams, StopParams},
};
use roost_store::{Error as StoreError, Store};
use serde_json::Value;
use uuid::Uuid;

use crate::{Service, decode, encode, internal_error, request_key, rpc, store_error, supervisor};

pub enum WorkerCall {
    Create(CreateWorkerParams),
    ListProfiles(ProjectParams),
    ListProcesses(ProjectParams),
    Logs(ProjectParams),
    Launch(LaunchParams, bool),
    Stop(StopParams),
}

impl WorkerCall {
    pub fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "worker.profile.create" => Ok(Self::Create(decode(params)?)),
            "worker.profile.list" => Ok(Self::ListProfiles(decode(params)?)),
            "process.list" => Ok(Self::ListProcesses(decode(params)?)),
            "log.tail" => Ok(Self::Logs(decode(params)?)),
            "project.start" => Ok(Self::Launch(decode(params)?, false)),
            "project.restart" => Ok(Self::Launch(decode(params)?, true)),
            "project.stop" => Ok(Self::Stop(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub fn execute(self, service: &Service) -> Result<Value, RpcError> {
        let mut supervisor = service.workers.lock().map_err(|_| internal_error())?;
        let store = Store::open(&service.db).map_err(store_error)?;
        match self {
            Self::Create(params) => {
                let key = request_key("worker.profile.create", &params)?;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        require_project(&store, params.project_id)?;
                        supervisor.clean_previous(&store, params.project_id)?;
                        supervisor.validate_config_change(&store, params.project_id)?;
                        let options = params.options;
                        if options.instances > MAX_PROFILE_INSTANCES
                            || options.graceful_stop_seconds > MAX_GRACEFUL_STOP_SECONDS
                        {
                            return Err(supervisor::Error::Invalid(
                                "worker profile exceeds supervision limits",
                            ));
                        }
                        let profile = WorkerProfile {
                            id: WorkerProfileId(Uuid::new_v4()),
                            project_id: params.project_id,
                            name: params.name,
                            connection: params.connection,
                            queues: options.queues,
                            desired_instances: options.instances,
                            sleep_seconds: options.sleep_seconds,
                            tries: options.tries,
                            timeout_seconds: options.timeout_seconds,
                            graceful_stop_seconds: options.graceful_stop_seconds,
                            crash_restart: options.crash_restart,
                            code_restart: CodeRestartPolicy::Never,
                            notifications: false,
                            enabled: true,
                            revision: Revision(1),
                        };
                        store.create_worker_profile(&profile)?;
                        Ok(profile)
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::ListProfiles(params) => {
                require_project(&store, params.id).map_err(worker_error)?;
                encode(store.list_worker_profiles(params.id).map_err(store_error)?)
            }
            Self::ListProcesses(params) => {
                require_project(&store, params.id).map_err(worker_error)?;
                encode(supervisor.status(&store, params.id).map_err(worker_error)?)
            }
            Self::Logs(params) => {
                require_project(&store, params.id).map_err(worker_error)?;
                encode(supervisor.logs(params.id))
            }
            Self::Launch(params, restart) => {
                let method = if restart {
                    "project.restart"
                } else {
                    "project.start"
                };
                let key = request_key(method, &params)?;
                let mut plan = None;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        supervisor.clean_previous(&store, params.id)?;
                        plan = supervisor.prepare(
                            &store,
                            params.id,
                            &params.php,
                            params.allow_horizon,
                            restart,
                        )?;
                        Ok(Intent {
                            id: params.id,
                            desired_state: DesiredState::Running,
                        })
                    })?;
                if let Some(plan) = plan {
                    supervisor.apply_start(params.id, plan);
                }
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Stop(params) => {
                let key = request_key("project.stop", &params)?;
                let mut apply = false;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        require_project(&store, params.id)?;
                        supervisor.validate_mutation()?;
                        supervisor.clean_previous(&store, params.id)?;
                        apply = true;
                        Ok(Intent {
                            id: params.id,
                            desired_state: DesiredState::Stopped,
                        })
                    })?;
                if apply {
                    supervisor.apply_stop(params.id);
                }
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
        }
    }
}

fn require_project(store: &Store, id: ProjectId) -> Result<(), supervisor::Error> {
    store.get_project(id)?.ok_or(StoreError::NotFound)?;
    Ok(())
}

fn worker_mutation<T: serde::Serialize + serde::de::DeserializeOwned>(
    store: &Store,
    id: Option<Uuid>,
    key: &str,
    work: impl FnOnce() -> Result<T, supervisor::Error>,
) -> Result<(Uuid, T), RpcError> {
    store.try_mutate(id, key, |_| work()).map_err(worker_error)
}

pub fn worker_error(error: supervisor::Error) -> RpcError {
    match error {
        supervisor::Error::Store(error) => store_error(error),
        supervisor::Error::Invalid(message) => RpcError::new(rpc::INVALID_PARAMS, message),
        supervisor::Error::Conflict(message) => RpcError::new(rpc::CONFLICT, message),
    }
}
