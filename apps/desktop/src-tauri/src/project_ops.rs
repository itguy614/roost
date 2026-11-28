use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use roost_core::{
    DesiredState, PhpProvider, Project, ProjectId, WorkerProfile,
    discovery::ResolvedPhp,
    worker_runtime::{ObservedState, ProcessStatus},
};
use roost_ipc::{
    AddParams, CONFLICT, Client, ClientError, INVALID_PARAMS, NOT_FOUND, RemoveParams,
    discovery::ResolvePhpParams,
    scheduler::{ConfigureSchedulerParams, SchedulerLaunchParams, SchedulerOptions},
    watch::{DisableWatchParams, EnableWatchParams, WatchStatus},
    workers::{
        AutostartParams, CreateWorkerParams, LaunchParams, ProjectParams, StopParams, WorkerOptions,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BridgeError;

const ACTION_DEADLINE: Duration = Duration::from_secs(12);

#[derive(Serialize)]
pub(crate) struct Detail {
    provider: PhpProvider,
    revision: roost_core::Revision,
    autostart: bool,
    watch_enabled: bool,
    connections: Option<roost_core::discovery::ConnectionSnapshot>,
    config_owner: roost_core::ConfigOwner,
    profiles: Option<Vec<WorkerProfile>>,
    workers: Option<Vec<Slot>>,
    scheduler: Option<Scheduler>,
    watch: Option<WatchStatus>,
    capabilities: Vec<&'static str>,
}

#[derive(Serialize)]
struct Scheduler {
    configured: bool,
    enabled: bool,
    desired_state: DesiredState,
    process: Option<Slot>,
}

#[derive(Serialize)]
struct Slot {
    id: Uuid,
    state: ObservedState,
    recovery_blocked: bool,
}

impl<P> From<ProcessStatus<P>> for Slot {
    fn from(process: ProcessStatus<P>) -> Self {
        Self {
            id: process.id,
            state: process.state,
            recovery_blocked: process.recovery_blocked,
        }
    }
}

pub(crate) async fn detail(client: &mut Client, project: &Project) -> Result<Detail, ClientError> {
    let params = ProjectParams { id: project.id };
    Ok(Detail {
        provider: project.provider,
        revision: project.revision,
        autostart: project.autostart,
        watch_enabled: project.watch_enabled,
        connections: if client.supports("queue.connections") {
            client.queue_connections(&params).await?
        } else {
            None
        },
        config_owner: project.config_owner,
        profiles: if client.supports("worker.profile.list") {
            Some(client.list_profiles(&params).await?)
        } else {
            None
        },
        workers: if client.supports("process.list") {
            Some(
                client
                    .list_processes(&params)
                    .await?
                    .into_iter()
                    .map(Slot::from)
                    .collect(),
            )
        } else {
            None
        },
        scheduler: if client.supports("scheduler.status") {
            let status = client.scheduler_status(&params).await?;
            Some(Scheduler {
                configured: status.profile.is_some(),
                enabled: status.profile.is_some_and(|p| p.enabled),
                desired_state: status.desired_state,
                process: status.process.map(Slot::from),
            })
        } else {
            None
        },
        watch: if client.supports("watch.status") {
            Some(client.watch_status(&params).await?)
        } else {
            None
        },
        capabilities: [
            "project.remove",
            "project.start",
            "project.stop",
            "project.restart",
            "project.setAutostart",
            "provider.resolvePhp",
            "worker.profile.create",
            "scheduler.configure",
            "scheduler.start",
            "scheduler.stop",
            "scheduler.restart",
            "watch.enable",
            "watch.disable",
            "queue.discover",
            "queue.suggestions",
        ]
        .into_iter()
        .filter(|method| client.supports(method))
        .collect(),
    })
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Action {
    Add {
        path: PathBuf,
    },
    Remove {
        id: ProjectId,
    },
    StartWorkers {
        id: ProjectId,
        ticket: Uuid,
        allow_horizon: bool,
    },
    RestartWorkers {
        id: ProjectId,
        ticket: Uuid,
        allow_horizon: bool,
    },
    StopWorkers {
        id: ProjectId,
    },
    ConfigureScheduler {
        id: ProjectId,
    },
    StartScheduler {
        id: ProjectId,
        ticket: Uuid,
    },
    RestartScheduler {
        id: ProjectId,
        ticket: Uuid,
    },
    StopScheduler {
        id: ProjectId,
    },
    SetAutostart {
        id: ProjectId,
        enabled: bool,
    },
    Watch {
        id: ProjectId,
        revision: roost_core::Revision,
        enabled: bool,
    },
    CreateProfile {
        id: ProjectId,
        name: String,
        connection: String,
        queues: Vec<String>,
        instances: u32,
    },
}

impl Action {
    pub(crate) fn launch(&self) -> Option<(ProjectId, Uuid)> {
        match *self {
            Self::StartWorkers { id, ticket, .. }
            | Self::RestartWorkers { id, ticket, .. }
            | Self::StartScheduler { id, ticket }
            | Self::RestartScheduler { id, ticket } => Some((id, ticket)),
            _ => None,
        }
    }

    fn method(&self) -> &'static str {
        match self {
            Self::Add { .. } => "project.add",
            Self::Remove { .. } => "project.remove",
            Self::StartWorkers { .. } => "project.start",
            Self::RestartWorkers { .. } => "project.restart",
            Self::StopWorkers { .. } => "project.stop",
            Self::ConfigureScheduler { .. } => "scheduler.configure",
            Self::StartScheduler { .. } => "scheduler.start",
            Self::RestartScheduler { .. } => "scheduler.restart",
            Self::StopScheduler { .. } => "scheduler.stop",
            Self::SetAutostart { .. } => "project.setAutostart",
            Self::Watch { enabled: true, .. } => "watch.enable",
            Self::Watch { enabled: false, .. } => "watch.disable",
            Self::CreateProfile { .. } => "worker.profile.create",
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ActionError {
    Connection { fault: BridgeError },
    Rejected { reason: &'static str },
    Uncertain,
}

#[derive(Serialize)]
pub(crate) struct Receipt {
    operation_id: Uuid,
    project_id: ProjectId,
}

fn action_error(error: ClientError) -> ActionError {
    match error {
        ClientError::Rpc(error) if error.code == CONFLICT => ActionError::Rejected {
            reason: if error.message
                == "Horizon is listed in Composer metadata; explicit confirmation is required"
            {
                "horizon"
            } else {
                "conflict"
            },
        },
        ClientError::Rpc(error) if error.code == INVALID_PARAMS => ActionError::Rejected {
            reason: "validation",
        },
        ClientError::Rpc(error) if error.code == NOT_FOUND => ActionError::Rejected {
            reason: "not_found",
        },
        ClientError::RequestTooLarge => ActionError::Rejected {
            reason: "validation",
        },
        _ => ActionError::Uncertain,
    }
}

pub(crate) async fn resolve(
    state: &Path,
    id: ProjectId,
    provider: Option<PhpProvider>,
) -> Result<ResolvedPhp, BridgeError> {
    let mut client = Client::connect(state).await?;
    if !client.supports("provider.resolvePhp") {
        return Err(BridgeError::Unsupported);
    }
    Ok(client
        .resolve_php(&ResolvePhpParams {
            id,
            provider,
            php: None,
        })
        .await?)
}

#[derive(Serialize)]
pub(crate) struct Discovery {
    connections: roost_core::discovery::ConnectionSnapshot,
    suggestions: Option<roost_core::discovery::QueueSuggestions>,
}

pub(crate) async fn discover(
    state: &Path,
    id: ProjectId,
    provider: Option<PhpProvider>,
) -> Result<Discovery, BridgeError> {
    let mut client = Client::connect(state).await?;
    if !client.supports("queue.discover") {
        return Err(BridgeError::Unsupported);
    }
    let connections = client
        .discover_queues(&ResolvePhpParams {
            id,
            provider,
            php: None,
        })
        .await?;
    let suggestions = if client.supports("queue.suggestions") {
        Some(
            client
                .suggest_queues(&roost_ipc::discovery::SuggestQueueParams {
                    id,
                    explicit: vec![],
                })
                .await?,
        )
    } else {
        None
    };
    Ok(Discovery {
        connections,
        suggestions,
    })
}

pub(crate) async fn execute(
    state: &Path,
    action: Action,
    key: Uuid,
    prepared_php: Option<PathBuf>,
) -> Result<Receipt, ActionError> {
    let mut client = Client::connect(state)
        .await
        .map_err(|error| ActionError::Connection {
            fault: error.into(),
        })?;
    if !client.supports(action.method()) {
        return Err(ActionError::Connection {
            fault: BridgeError::Unsupported,
        });
    }
    tokio::time::timeout(
        ACTION_DEADLINE,
        send(&mut client, action, key, prepared_php),
    )
    .await
    .map_err(|_| ActionError::Uncertain)?
}

async fn send(
    client: &mut Client,
    action: Action,
    key: Uuid,
    prepared_php: Option<PathBuf>,
) -> Result<Receipt, ActionError> {
    let method = action.method();
    let key = Some(key);
    let (operation_id, project_id) = match action {
        Action::Add { path } => {
            let reply = client
                .add_project(&AddParams {
                    path,
                    idempotency_key: key,
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::Remove { id } => {
            let reply = client
                .remove_project(&RemoveParams {
                    id,
                    idempotency_key: key,
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::StartWorkers {
            id, allow_horizon, ..
        }
        | Action::RestartWorkers {
            id, allow_horizon, ..
        } => {
            let request = LaunchParams {
                id,
                php: prepared_php.ok_or(ActionError::Rejected {
                    reason: "expired_launch",
                })?,
                allow_horizon,
                idempotency_key: key,
            };
            let reply = if method == "project.restart" {
                client.restart_project(&request).await
            } else {
                client.start_project(&request).await
            }
            .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::StopWorkers { id } => {
            let reply = client
                .stop_project(&StopParams {
                    id,
                    idempotency_key: key,
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::ConfigureScheduler { id } => {
            let reply = client
                .configure_scheduler(&ConfigureSchedulerParams {
                    id,
                    expected_revision: None,
                    options: SchedulerOptions::default(),
                    idempotency_key: key,
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.project_id)
        }
        Action::StartScheduler { id, .. } | Action::RestartScheduler { id, .. } => {
            let request = SchedulerLaunchParams {
                id,
                php: prepared_php.ok_or(ActionError::Rejected {
                    reason: "expired_launch",
                })?,
                idempotency_key: key,
            };
            let reply = if method == "scheduler.restart" {
                client.restart_scheduler(&request).await
            } else {
                client.start_scheduler(&request).await
            }
            .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::StopScheduler { id } => {
            let reply = client
                .stop_scheduler(&StopParams {
                    id,
                    idempotency_key: key,
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::SetAutostart { id, enabled } => {
            let reply = client
                .set_autostart(&AutostartParams {
                    id,
                    enabled,
                    idempotency_key: key,
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::Watch {
            id,
            revision,
            enabled,
        } => {
            let reply = if enabled {
                client
                    .enable_watch(&EnableWatchParams {
                        id,
                        expected_revision: revision,
                        rules: None,
                        idempotency_key: key,
                    })
                    .await
            } else {
                client
                    .disable_watch(&DisableWatchParams {
                        id,
                        expected_revision: revision,
                        idempotency_key: key,
                    })
                    .await
            }
            .map_err(action_error)?;
            (reply.operation_id, reply.result.id)
        }
        Action::CreateProfile {
            id,
            name,
            connection,
            queues,
            instances,
        } => {
            let reply = client
                .create_worker(&CreateWorkerParams {
                    project_id: id,
                    name,
                    connection,
                    idempotency_key: key,
                    options: WorkerOptions {
                        queues,
                        instances,
                        ..WorkerOptions::default()
                    },
                })
                .await
                .map_err(action_error)?;
            (reply.operation_id, reply.result.project_id)
        }
    };
    Ok(Receipt {
        operation_id,
        project_id,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        process::{Child, Command, Stdio},
    };

    use super::*;

    struct Daemon(Child);
    impl Drop for Daemon {
        fn drop(&mut self) {
            // Send SIGTERM to this test's owned daemon. It will perform guardian cleanup.
            let _ = Command::new("/bin/kill")
                .args(["-TERM", &self.0.id().to_string()])
                .status();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn launch_payload_rejects_renderer_supplied_php() {
        let project = Uuid::new_v4();
        let ticket = Uuid::new_v4();
        let action: Action = serde_json::from_value(serde_json::json!({
            "kind":"restart_workers","id":project,"ticket":ticket,"allow_horizon":false
        }))
        .unwrap();
        assert_eq!(action.method(), "project.restart");
        assert_eq!(action.launch(), Some((ProjectId(project), ticket)));
        assert!(
            serde_json::from_value::<Action>(serde_json::json!({
                "kind":"start_scheduler","id":project,"ticket":ticket,"php":"/untrusted"
            }))
            .is_err()
        );
    }

    async fn wait_for<F, Fut>(mut ready: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !ready().await {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("fixture condition did not settle");
    }

    #[tokio::test]
    #[ignore = "requires ROOST_SMOKE_DAEMON and ROOST_SMOKE_WORKER executables"]
    async fn real_daemon_registration_replay_and_independent_worker_scheduler_intents() {
        let root = tempfile::Builder::new()
            .prefix("roost-desk-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let state = root.path().join("state");
        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let php = bin.join("php");
        fs::copy(std::env::var_os("ROOST_SMOKE_WORKER").unwrap(), &php).unwrap();
        fs::set_permissions(&php, fs::Permissions::from_mode(0o700)).unwrap();
        let _daemon = Daemon(
            Command::new(std::env::var_os("ROOST_SMOKE_DAEMON").unwrap())
                .args(["--state-dir", state.to_str().unwrap()])
                .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );

        wait_for(|| async { Client::connect(&state).await.is_ok() }).await;
        let path = root.path().join("Dispatch");
        fs::create_dir(&path).unwrap();
        let invalid = execute(
            &state,
            Action::Add { path: path.clone() },
            Uuid::new_v4(),
            None,
        )
        .await;
        assert!(matches!(
            invalid,
            Err(ActionError::Rejected {
                reason: "validation"
            })
        ));
        fs::write(path.join("artisan"), "graceful").unwrap();
        let registration_key = Uuid::new_v4();
        let first = execute(
            &state,
            Action::Add { path: path.clone() },
            registration_key,
            None,
        )
        .await
        .unwrap();
        let replayed = execute(&state, Action::Add { path }, registration_key, None)
            .await
            .unwrap();
        let id = first.project_id;
        assert_eq!(replayed.operation_id, registration_key);
        assert_eq!(replayed.project_id, id);

        execute(
            &state,
            Action::CreateProfile {
                id,
                name: "Default".into(),
                connection: "database".into(),
                queues: vec!["critical".into(), "default".into()],
                instances: 1,
            },
            Uuid::new_v4(),
            None,
        )
        .await
        .unwrap();
        execute(
            &state,
            Action::ConfigureScheduler { id },
            Uuid::new_v4(),
            None,
        )
        .await
        .unwrap();
        let resolved = resolve(&state, id, Some(PhpProvider::System))
            .await
            .unwrap();
        assert_eq!(resolved.path, php);
        execute(
            &state,
            Action::StartWorkers {
                id,
                ticket: Uuid::new_v4(),
                allow_horizon: false,
            },
            Uuid::new_v4(),
            Some(resolved.path.clone()),
        )
        .await
        .unwrap();
        execute(
            &state,
            Action::StartScheduler {
                id,
                ticket: Uuid::new_v4(),
            },
            Uuid::new_v4(),
            Some(resolved.path.clone()),
        )
        .await
        .unwrap();
        wait_for(|| async {
            let mut client = Client::connect(&state).await.unwrap();
            let params = ProjectParams { id };
            client
                .list_processes(&params)
                .await
                .unwrap()
                .iter()
                .any(|slot| slot.state == ObservedState::Running)
                && client
                    .scheduler_status(&params)
                    .await
                    .unwrap()
                    .process
                    .is_some_and(|slot| slot.state == ObservedState::Running)
        })
        .await;
        let mut cli = Client::connect(&state).await.unwrap();
        let params = ProjectParams { id };
        let old_scheduler = cli
            .scheduler_status(&params)
            .await
            .unwrap()
            .process
            .unwrap()
            .identity
            .unwrap()
            .pid;
        let old_worker = cli.list_processes(&params).await.unwrap()[0]
            .identity
            .unwrap()
            .pid;
        execute(
            &state,
            Action::RestartWorkers {
                id,
                ticket: Uuid::new_v4(),
                allow_horizon: false,
            },
            Uuid::new_v4(),
            Some(resolved.path),
        )
        .await
        .unwrap();
        wait_for(|| async {
            Client::connect(&state)
                .await
                .unwrap()
                .list_processes(&ProjectParams { id })
                .await
                .unwrap()
                .iter()
                .any(|p| {
                    p.state == ObservedState::Running
                        && p.identity.is_some_and(|p| p.pid != old_worker)
                })
        })
        .await;
        execute(&state, Action::StopWorkers { id }, Uuid::new_v4(), None)
            .await
            .unwrap();
        assert_eq!(
            cli.get_project(&roost_ipc::GetParams { id })
                .await
                .unwrap()
                .desired_state,
            DesiredState::Stopped
        );
        let scheduler = cli.scheduler_status(&params).await.unwrap();
        assert_eq!(scheduler.desired_state, DesiredState::Running);
        assert_eq!(
            scheduler.process.unwrap().identity.unwrap().pid,
            old_scheduler
        );
        execute(&state, Action::StopScheduler { id }, Uuid::new_v4(), None)
            .await
            .unwrap();
        wait_for(|| async {
            let mut client = Client::connect(&state).await.unwrap();
            client
                .list_processes(&ProjectParams { id })
                .await
                .unwrap()
                .iter()
                .all(|p| p.state == ObservedState::Stopped)
                && client
                    .scheduler_status(&ProjectParams { id })
                    .await
                    .unwrap()
                    .process
                    .is_none_or(|p| p.state == ObservedState::Stopped)
        })
        .await;
        let original = cli.get_project(&roost_ipc::GetParams { id }).await.unwrap();
        execute(
            &state,
            Action::Watch {
                id,
                revision: original.revision,
                enabled: true,
            },
            Uuid::new_v4(),
            None,
        )
        .await
        .unwrap();
        let stale = execute(
            &state,
            Action::Watch {
                id,
                revision: original.revision,
                enabled: false,
            },
            Uuid::new_v4(),
            None,
        )
        .await;
        assert!(matches!(
            stale,
            Err(ActionError::Rejected { reason: "conflict" })
        ));
        let current = cli.get_project(&roost_ipc::GetParams { id }).await.unwrap();
        assert!(current.watch_enabled);
        execute(
            &state,
            Action::Watch {
                id,
                revision: current.revision,
                enabled: false,
            },
            Uuid::new_v4(),
            None,
        )
        .await
        .unwrap();
        execute(&state, Action::Remove { id }, Uuid::new_v4(), None)
            .await
            .unwrap();
        assert!(cli.list_projects().await.unwrap().is_empty());
    }
}
