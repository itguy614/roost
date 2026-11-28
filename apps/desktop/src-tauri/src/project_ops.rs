use std::{
    fs::Metadata,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use roost_core::{
    DesiredState, PhpProvider, Project, ProjectId, WorkerProfile,
    discovery::ResolvedPhp,
    worker_runtime::{ObservedState, ProcessStatus},
};
use roost_ipc::{
    AddParams, CONFLICT, Client, ClientError, INVALID_PARAMS, NOT_FOUND, RemoveParams,
    discovery::ResolvePhpParams,
    mutations::{LaunchFence, ReceiptParams},
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
        .filter(|method| {
            if *method == "provider.resolvePhp" {
                client.supports(method) && client.supports("project.launchFence")
            } else {
                client.supports(method)
                    && (method.starts_with("queue.") || client.supports("mutation.receipt"))
            }
        })
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
        revision: roost_core::Revision,
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
        revision: roost_core::Revision,
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

#[derive(Debug, PartialEq, Eq)]
struct ExecutableStamp {
    device: u64,
    inode: u64,
    bytes: u64,
    changed: (i64, i64),
    modified: (i64, i64),
}

impl From<Metadata> for ExecutableStamp {
    fn from(meta: Metadata) -> Self {
        Self {
            device: meta.dev(),
            inode: meta.ino(),
            bytes: meta.len(),
            changed: (meta.ctime(), meta.ctime_nsec()),
            modified: (meta.mtime(), meta.mtime_nsec()),
        }
    }
}

pub(crate) struct PreparedLaunch {
    fence: LaunchFence,
    ticket: Uuid,
    php: PathBuf,
    stamp: ExecutableStamp,
    prepared: Instant,
}

impl PreparedLaunch {
    pub(crate) fn ticket(&self) -> Uuid {
        self.ticket
    }

    fn runtime(&self, id: ProjectId, ticket: Uuid) -> Result<(&Path, &LaunchFence), ActionError> {
        if self.fence.project_id == id
            && self.ticket == ticket
            && self.prepared.elapsed() < Duration::from_secs(60)
            && std::fs::metadata(&self.php)
                .ok()
                .is_some_and(|meta| ExecutableStamp::from(meta) == self.stamp)
        {
            Ok((&self.php, &self.fence))
        } else {
            Err(ActionError::Rejected {
                reason: "expired_launch",
            })
        }
    }
}

pub(crate) async fn prepare(
    state: &Path,
    id: ProjectId,
    provider: Option<PhpProvider>,
) -> Result<PreparedLaunch, BridgeError> {
    let mut client = Client::connect(state).await?;
    if !client.supports("provider.resolvePhp") || !client.supports("project.launchFence") {
        return Err(BridgeError::Unsupported);
    }
    let fence = client.launch_fence(&ProjectParams { id }).await?;
    let ResolvedPhp { path, .. } = client
        .resolve_php(&ResolvePhpParams {
            id,
            provider,
            php: None,
        })
        .await?;
    let stamp = std::fs::metadata(&path)
        .map_err(|_| BridgeError::Unavailable)?
        .into();
    if client.launch_fence(&ProjectParams { id }).await? != fence {
        return Err(BridgeError::Unavailable);
    }
    Ok(PreparedLaunch {
        fence,
        ticket: Uuid::new_v4(),
        php: path,
        stamp,
        prepared: Instant::now(),
    })
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

pub(crate) struct WireAction {
    request: ReceiptParams,
    project: Option<ProjectId>,
}

impl WireAction {
    pub(crate) fn key(&self) -> Uuid {
        self.request.operation_id
    }

    pub(crate) fn new(
        action: Action,
        key: Uuid,
        prepared: Option<&PreparedLaunch>,
    ) -> Result<Self, ActionError> {
        let method = action.method();
        let runtime = action
            .launch()
            .map(|(id, ticket)| {
                prepared
                    .ok_or(ActionError::Rejected {
                        reason: "expired_launch",
                    })?
                    .runtime(id, ticket)
            })
            .transpose()?;
        let idempotency_key = Some(key);
        let (project, params) = match action {
            Action::Add { path } => (
                None,
                serde_json::to_value(AddParams {
                    path,
                    idempotency_key,
                }),
            ),
            Action::Remove { id, revision } => (
                Some(id),
                serde_json::to_value(RemoveParams {
                    id,
                    idempotency_key,
                    expected_revision: Some(revision),
                }),
            ),
            Action::StartWorkers {
                id, allow_horizon, ..
            }
            | Action::RestartWorkers {
                id, allow_horizon, ..
            } => {
                let (php, fence) = runtime.expect("worker launch resolved above");
                (
                    Some(id),
                    serde_json::to_value(LaunchParams {
                        id,
                        php: php.to_owned(),
                        allow_horizon,
                        idempotency_key,
                        fence: Some(fence.clone()),
                    }),
                )
            }
            Action::StopWorkers { id } | Action::StopScheduler { id } => (
                Some(id),
                serde_json::to_value(StopParams {
                    id,
                    idempotency_key,
                }),
            ),
            Action::ConfigureScheduler { id } => (
                Some(id),
                serde_json::to_value(ConfigureSchedulerParams {
                    id,
                    expected_revision: None,
                    options: SchedulerOptions::default(),
                    idempotency_key,
                }),
            ),
            Action::StartScheduler { id, .. } | Action::RestartScheduler { id, .. } => {
                let (php, fence) = runtime.expect("scheduler launch resolved above");
                (
                    Some(id),
                    serde_json::to_value(SchedulerLaunchParams {
                        id,
                        php: php.to_owned(),
                        idempotency_key,
                        fence: Some(fence.clone()),
                    }),
                )
            }
            Action::SetAutostart {
                id,
                revision,
                enabled,
            } => (
                Some(id),
                serde_json::to_value(AutostartParams {
                    id,
                    enabled,
                    idempotency_key,
                    expected_revision: Some(revision),
                }),
            ),
            Action::Watch {
                id,
                revision,
                enabled: true,
            } => (
                Some(id),
                serde_json::to_value(EnableWatchParams {
                    id,
                    expected_revision: revision,
                    rules: None,
                    idempotency_key,
                }),
            ),
            Action::Watch {
                id,
                revision,
                enabled: false,
            } => (
                Some(id),
                serde_json::to_value(DisableWatchParams {
                    id,
                    expected_revision: revision,
                    idempotency_key,
                }),
            ),
            Action::CreateProfile {
                id,
                name,
                connection,
                queues,
                instances,
            } => (
                Some(id),
                serde_json::to_value(CreateWorkerParams {
                    project_id: id,
                    name,
                    connection,
                    idempotency_key,
                    options: WorkerOptions {
                        queues,
                        instances,
                        ..WorkerOptions::default()
                    },
                }),
            ),
        };
        Ok(Self {
            request: ReceiptParams {
                operation_id: key,
                method: method.into(),
                params: params.map_err(|_| ActionError::Rejected {
                    reason: "validation",
                })?,
            },
            project,
        })
    }

    pub(crate) async fn submit(&self, state: &Path) -> Result<Receipt, ActionError> {
        let mut client = Client::connect(state)
            .await
            .map_err(|error| ActionError::Connection {
                fault: error.into(),
            })?;
        if !client.supports(&self.request.method) || !client.supports("mutation.receipt") {
            return Err(ActionError::Connection {
                fault: BridgeError::Unsupported,
            });
        }
        let reply = tokio::time::timeout(
            ACTION_DEADLINE,
            client.submit_mutation(&self.request.method, &self.request.params),
        )
        .await
        .map_err(|_| ActionError::Uncertain)?
        .map_err(action_error)?;
        self.decode_receipt(reply)
    }

    pub(crate) async fn recover(&self, state: &Path) -> Result<Option<Receipt>, ActionError> {
        let mut client = Client::connect(state)
            .await
            .map_err(|error| ActionError::Connection {
                fault: error.into(),
            })?;
        if !client.supports("mutation.receipt") {
            return Err(ActionError::Connection {
                fault: BridgeError::Unsupported,
            });
        }
        client
            .mutation_receipt(&self.request)
            .await
            .map_err(action_error)?
            .map(|reply| self.decode_receipt(reply))
            .transpose()
    }

    fn decode_receipt(
        &self,
        reply: roost_ipc::Mutation<serde_json::Value>,
    ) -> Result<Receipt, ActionError> {
        if reply.operation_id != self.request.operation_id {
            return Err(ActionError::Uncertain);
        }
        let project_id = match self.project {
            Some(id) => id,
            None => serde_json::from_value(reply.result["id"].clone())
                .map_err(|_| ActionError::Uncertain)?,
        };
        Ok(Receipt {
            operation_id: reply.operation_id,
            project_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        process::{Child, Command, Stdio},
    };

    use super::*;

    async fn execute(
        state: &Path,
        action: Action,
        key: Uuid,
        prepared: Option<&PreparedLaunch>,
    ) -> Result<Receipt, ActionError> {
        WireAction::new(action, key, prepared)?.submit(state).await
    }

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

    fn start_daemon(state: &Path, bin: &Path) -> Daemon {
        Daemon(
            Command::new(std::env::var_os("ROOST_SMOKE_DAEMON").unwrap())
                .arg("--state-dir")
                .arg(state)
                .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    async fn drop_committed_reply(state: &Path, request: &WireAction) {
        use roost_ipc::{
            MAX_REQUEST, MAX_RESPONSE, endpoint::SOCKET_NAME, read_frame, write_frame,
        };
        use tokio::{
            io::BufReader,
            net::{UnixListener, UnixStream},
        };

        let proxy = state.with_extension("proxy");
        fs::create_dir(&proxy).unwrap();
        fs::set_permissions(&proxy, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = proxy.join(SOCKET_NAME);
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        let daemon_socket = state.join(SOCKET_NAME);
        let relay = tokio::spawn(async move {
            let (outbound, _) = listener.accept().await.unwrap();
            let mut outbound = BufReader::new(outbound);
            let mut daemon = BufReader::new(UnixStream::connect(daemon_socket).await.unwrap());
            let mut response = Vec::new();
            for forward_to_client in [true, false] {
                let frame = read_frame(&mut outbound, MAX_REQUEST)
                    .await
                    .unwrap()
                    .unwrap();
                write_frame(daemon.get_mut(), &frame).await.unwrap();
                response = read_frame(&mut daemon, MAX_RESPONSE)
                    .await
                    .unwrap()
                    .unwrap();
                if forward_to_client {
                    write_frame(outbound.get_mut(), &response).await.unwrap();
                }
            }
            // The real daemon's receipt has arrived at the relay. Close the
            // desktop connection before delivering any of the mutation reply.
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()
        });
        assert!(matches!(
            request.submit(&proxy).await,
            Err(ActionError::Uncertain)
        ));
        assert_eq!(
            relay.await.unwrap()["result"]["operation_id"],
            request.key().to_string()
        );
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
        let _daemon = start_daemon(&state, &bin);

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
        let resolved = prepare(&state, id, Some(PhpProvider::System))
            .await
            .unwrap();
        assert_eq!(resolved.php, php);
        execute(
            &state,
            Action::StartWorkers {
                id,
                ticket: resolved.ticket(),
                allow_horizon: false,
            },
            Uuid::new_v4(),
            Some(&resolved),
        )
        .await
        .unwrap();
        execute(
            &state,
            Action::StartScheduler {
                id,
                ticket: resolved.ticket(),
            },
            Uuid::new_v4(),
            Some(&resolved),
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
                ticket: resolved.ticket(),
                allow_horizon: false,
            },
            Uuid::new_v4(),
            Some(&resolved),
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
        let latest = cli.get_project(&roost_ipc::GetParams { id }).await.unwrap();
        execute(
            &state,
            Action::Remove {
                id,
                revision: latest.revision,
            },
            Uuid::new_v4(),
            None,
        )
        .await
        .unwrap();
        assert!(cli.list_projects().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires the built daemon and CLI alongside ROOST_SMOKE_WORKER"]
    async fn real_daemon_recovers_lost_reply_and_fences_unaccepted_mutations() {
        let root = tempfile::Builder::new()
            .prefix("roost-desk-receipt-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let state = root.path().join("state");
        let path = root.path().join("Dispatch Team");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("artisan"), "graceful").unwrap();
        let php = root.path().join("php");
        fs::copy(std::env::var_os("ROOST_SMOKE_WORKER").unwrap(), &php).unwrap();
        let daemon = start_daemon(&state, root.path());
        wait_for(|| async { Client::connect(&state).await.is_ok() }).await;

        let id = execute(&state, Action::Add { path }, Uuid::new_v4(), None)
            .await
            .unwrap()
            .project_id;
        let mut cli = Client::connect(&state).await.unwrap();
        let cli_path =
            PathBuf::from(std::env::var_os("ROOST_SMOKE_DAEMON").unwrap()).with_file_name("roost");
        let cli_state = state.clone();
        let configure_cli = tokio::task::spawn_blocking(move || {
            Command::new(cli_path)
                .arg("--state-dir")
                .arg(cli_state)
                .args([
                    "scheduler",
                    "configure",
                    &id.0.to_string(),
                    "--options-json",
                    r#"{"graceful_stop_seconds":27}"#,
                ])
                .output()
                .unwrap()
        });
        // The real CLI races a prepared desktop create with different options.
        let (cli_result, desktop_result) = tokio::join!(
            configure_cli,
            execute(
                &state,
                Action::ConfigureScheduler { id },
                Uuid::new_v4(),
                None
            ),
        );
        let cli_result = cli_result.unwrap();
        let cli_won = cli_result.status.success();
        assert_ne!(cli_won, desktop_result.is_ok(), "{cli_result:?}");
        if !cli_won {
            assert!(
                String::from_utf8_lossy(&cli_result.stderr).contains("revision"),
                "{cli_result:?}"
            );
        }
        let configured = cli
            .scheduler_status(&ProjectParams { id })
            .await
            .unwrap()
            .profile
            .unwrap();
        assert_eq!(
            configured.graceful_stop_seconds,
            if cli_won { 27 } else { 15 }
        );
        assert!(matches!(
            execute(
                &state,
                Action::ConfigureScheduler { id },
                Uuid::new_v4(),
                None
            )
            .await,
            Err(ActionError::Rejected { reason: "conflict" })
        ));
        assert_eq!(
            cli.scheduler_status(&ProjectParams { id })
                .await
                .unwrap()
                .profile
                .unwrap()
                .revision,
            configured.revision
        );

        cli.configure_scheduler(&ConfigureSchedulerParams {
            id,
            expected_revision: Some(configured.revision),
            options: SchedulerOptions {
                graceful_stop_seconds: 31,
                ..Default::default()
            },
            idempotency_key: Some(Uuid::new_v4()),
        })
        .await
        .unwrap();
        assert!(matches!(
            execute(
                &state,
                Action::ConfigureScheduler { id },
                Uuid::new_v4(),
                None
            )
            .await,
            Err(ActionError::Rejected { reason: "conflict" })
        ));
        assert_eq!(
            cli.scheduler_status(&ProjectParams { id })
                .await
                .unwrap()
                .profile
                .unwrap()
                .graceful_stop_seconds,
            31
        );

        let mut expired = prepare(&state, id, Some(PhpProvider::System))
            .await
            .unwrap();
        expired.prepared -= Duration::from_secs(61);
        assert!(matches!(
            WireAction::new(
                Action::StartScheduler {
                    id,
                    ticket: expired.ticket()
                },
                Uuid::new_v4(),
                Some(&expired)
            ),
            Err(ActionError::Rejected {
                reason: "expired_launch"
            })
        ));

        let prepared = prepare(&state, id, Some(PhpProvider::System))
            .await
            .unwrap();
        let launch = || Action::StartScheduler {
            id,
            ticket: prepared.ticket(),
        };
        let accepted = WireAction::new(launch(), Uuid::new_v4(), Some(&prepared)).unwrap();
        let unaccepted = WireAction::new(launch(), Uuid::new_v4(), Some(&prepared)).unwrap();
        drop_committed_reply(&state, &accepted).await;
        wait_for(|| async { accepted.recover(&state).await.unwrap().is_some() }).await;
        let receipt = accepted.recover(&state).await.unwrap().unwrap();
        assert_eq!(receipt.project_id, id);
        assert_eq!(receipt.operation_id, accepted.key());
        assert!(unaccepted.recover(&state).await.unwrap().is_none());
        let wrong_payload =
            WireAction::new(Action::StopScheduler { id }, accepted.key(), None).unwrap();
        assert!(matches!(
            wrong_payload.recover(&state).await,
            Err(ActionError::Rejected { reason: "conflict" })
        ));

        let revision = cli
            .get_project(&roost_ipc::GetParams { id })
            .await
            .unwrap()
            .revision;
        execute(
            &state,
            Action::SetAutostart {
                id,
                revision,
                enabled: true,
            },
            Uuid::new_v4(),
            None,
        )
        .await
        .unwrap();
        for action in [
            Action::SetAutostart {
                id,
                revision,
                enabled: false,
            },
            Action::Remove { id, revision },
        ] {
            assert!(matches!(
                execute(&state, action, Uuid::new_v4(), None).await,
                Err(ActionError::Rejected { reason: "conflict" })
            ));
        }
        assert!(
            cli.get_project(&roost_ipc::GetParams { id })
                .await
                .unwrap()
                .autostart
        );
        assert!(matches!(
            unaccepted.submit(&state).await,
            Err(ActionError::Rejected { reason: "conflict" })
        ));
        assert!(accepted.recover(&state).await.unwrap().is_some());

        execute(&state, Action::StopScheduler { id }, Uuid::new_v4(), None)
            .await
            .unwrap();
        let before_restart = prepare(&state, id, Some(PhpProvider::System))
            .await
            .unwrap();
        let prior_daemon = WireAction::new(
            Action::StartScheduler {
                id,
                ticket: before_restart.ticket(),
            },
            Uuid::new_v4(),
            Some(&before_restart),
        )
        .unwrap();
        fs::write(&php, "replaced runtime").unwrap();
        assert!(matches!(
            WireAction::new(
                Action::StartScheduler {
                    id,
                    ticket: before_restart.ticket()
                },
                Uuid::new_v4(),
                Some(&before_restart)
            ),
            Err(ActionError::Rejected {
                reason: "expired_launch"
            })
        ));
        drop(daemon);
        let _daemon = start_daemon(&state, root.path());
        wait_for(|| async { Client::connect(&state).await.is_ok() }).await;
        assert!(accepted.recover(&state).await.unwrap().is_some());
        assert!(prior_daemon.recover(&state).await.unwrap().is_none());
        assert!(matches!(
            prior_daemon.submit(&state).await,
            Err(ActionError::Rejected { reason: "conflict" })
        ));
        assert_eq!(
            Client::connect(&state)
                .await
                .unwrap()
                .scheduler_status(&ProjectParams { id })
                .await
                .unwrap()
                .desired_state,
            DesiredState::Stopped
        );
    }
}
