#![cfg(target_os = "macos")]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use roost_core::{
    CodeRestartPolicy, CrashRestartPolicy, DesiredState, Group, ProjectId, Revision,
    SharedWatchPath, WatchRules, WorkerProfileId,
    scheduler::{SchedulerIntent, SchedulerLease, SchedulerProcessStatus, SchedulerProfileId},
    worker_runtime::{
        DaemonStartup, LaunchFailure, LaunchIntent, ObservedState, ProjectIntent, StopCause,
        WorkerLeaseState, WorkerStatus,
    },
};
use roost_daemon::DaemonService;
use roost_ipc::{
    AddParams, CONFLICT, Client, ClientError, INVALID_PARAMS, RemoveParams,
    groups::{
        CreateGroupParams, DeleteGroupParams, GroupLaunchParams, GroupMembershipParams,
        GroupOptions, GroupOutcome, GroupParams, GroupProjectLaunch, GroupStopParams,
        UpdateGroupParams,
    },
    metrics::{HistoryParams, Resolution},
    scheduler::{ConfigureSchedulerParams, SchedulerLaunchParams, SchedulerOptions},
    watch::{CodeRestartParams, DisableWatchParams, EnableWatchParams, WatchState, WatchStatus},
    workers::{
        AutostartParams, CreateWorkerParams, LaunchParams, ProjectParams, StopParams, WorkerOptions,
    },
};
use roost_store::Store;
use tokio::{net::UnixStream, task::JoinHandle};
use uuid::Uuid;

const GUARDIAN: &str = env!("CARGO_BIN_EXE_roost-daemon");
const TOTAL_WAIT: Duration = Duration::from_secs(12);

struct Harness {
    root: PathBuf,
    db: PathBuf,
    php: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let root = Path::new("/private/tmp").join(format!("roost-m2-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let db = root.join("roost.sqlite3");
        Store::initialize(&db).unwrap();
        let php = root.join("php");
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        assert!(
            Command::new(rustc)
                .args(["--edition=2024", "tests/fixtures/queue_worker.rs", "-o"])
                .arg(&php)
                .status()
                .unwrap()
                .success()
        );
        Self { root, db, php }
    }

    fn app(&self, name: &str, mode: &str) -> PathBuf {
        let path = self.root.join(name);
        fs::create_dir(&path).unwrap();
        fs::write(path.join("artisan"), mode).unwrap();
        path
    }

    fn service(&self) -> Arc<DaemonService> {
        Arc::new(DaemonService::new(self.db.clone(), PathBuf::from(GUARDIAN)).unwrap())
    }

    async fn client(service: &Arc<DaemonService>) -> (Client, JoinHandle<()>) {
        let (client, server) = UnixStream::pair().unwrap();
        let service = service.clone();
        let task = tokio::spawn(async move { service.serve_client(server).await });
        (Client::negotiate(client).await.unwrap(), task)
    }

    fn poll(service: &Arc<DaemonService>) -> JoinHandle<()> {
        let service = service.clone();
        tokio::spawn(async move { service.poll_workers().await })
    }

    async fn daemon(&self) -> (OwnedChild, Client) {
        let dir = self.root.join("daemon");
        let mut child = OwnedChild(
            Command::new(GUARDIAN)
                .arg("--state-dir")
                .arg(&dir)
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let limit = Instant::now() + TOTAL_WAIT;
        loop {
            if let Ok(mut client) = Client::connect(&dir).await
                && client
                    .status()
                    .await
                    .is_ok_and(|status| status.pid == child.0.id())
            {
                return (child, client);
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "daemon exited during startup"
            );
            assert!(Instant::now() < limit, "daemon socket did not become ready");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn register(
        &self,
        client: &mut Client,
        app: &Path,
        instances: u32,
        crash_restart: CrashRestartPolicy,
    ) -> ProjectId {
        let project = client
            .add_project(&AddParams {
                path: app.into(),
                idempotency_key: None,
            })
            .await
            .unwrap()
            .result;
        client
            .create_worker(&CreateWorkerParams {
                project_id: project.id,
                name: "Queue Worker".into(),
                connection: "database".into(),
                options: WorkerOptions {
                    queues: vec!["critical".into(), "default".into()],
                    instances,
                    graceful_stop_seconds: 1,
                    crash_restart,
                    ..WorkerOptions::default()
                },
                idempotency_key: None,
            })
            .await
            .unwrap();
        project.id
    }

    fn launch(&self, id: ProjectId) -> LaunchParams {
        LaunchParams {
            id,
            php: self.php.clone(),
            allow_horizon: false,
            idempotency_key: Some(Uuid::new_v4()),
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

async fn until_status(
    client: &mut Client,
    id: ProjectId,
    matches: impl Fn(&[WorkerStatus]) -> bool,
) -> Vec<WorkerStatus> {
    let limit = Instant::now() + TOTAL_WAIT;
    loop {
        let statuses = client.list_processes(&ProjectParams { id }).await.unwrap();
        if matches(&statuses) {
            return statuses;
        }
        assert!(
            Instant::now() < limit,
            "unexpected worker status: {statuses:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn eventually(mut test: impl FnMut() -> bool) {
    let limit = Instant::now() + TOTAL_WAIT;
    loop {
        if test() {
            return;
        }
        assert!(Instant::now() < limit, "condition did not become true");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn all_in(state: ObservedState, count: usize) -> impl Fn(&[WorkerStatus]) -> bool {
    move |workers| workers.len() == count && workers.iter().all(|worker| worker.state == state)
}

fn exists(pid: u32) -> bool {
    // SAFETY: signal 0 checks for existence without sending a signal.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[tokio::test]
async fn concurrent_start_restart_stop_and_redacted_output() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    let service = h.service();
    let poll = Harness::poll(&service);
    let (mut a, _) = Harness::client(&service).await;
    let (mut b, _) = Harness::client(&service).await;
    let id = h
        .register(&mut a, &app, 2, CrashRestartPolicy::Always)
        .await;
    let lp = h.launch(id);
    let (first, second) = tokio::join!(a.start_project(&lp), b.start_project(&lp));
    assert_eq!(first.unwrap().operation_id, second.unwrap().operation_id);
    assert_eq!(
        a.list_profiles(&ProjectParams { id }).await.unwrap().len(),
        1
    );
    let workers = until_status(&mut a, id, all_in(ObservedState::Running, 2)).await;
    let old_pids: Vec<_> = workers
        .iter()
        .map(|worker| worker.identity.unwrap().pid)
        .collect();
    let denied = a
        .remove_project(&RemoveParams {
            id,
            idempotency_key: None,
        })
        .await;
    assert!(matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT));

    let timeout = Instant::now() + TOTAL_WAIT;
    loop {
        let output = a.tail_logs(&ProjectParams { id }).await.unwrap();
        if output.iter().any(|line| line.text == "worker ready")
            && output
                .iter()
                .any(|line| line.text.contains("line redacted"))
        {
            let encoded = serde_json::to_string(&output).unwrap();
            assert!(!encoded.contains("secret-fixture-output"));
            assert!(encoded.contains("worker ready"));
            assert!(encoded.contains("line redacted"));
            assert!(
                output
                    .iter()
                    .all(|entry| entry.newlines > 0 && entry.bytes > 0)
            );
            break;
        }
        assert!(Instant::now() < timeout);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    a.restart_project(&h.launch(id)).await.unwrap();
    let current = until_status(&mut a, id, |workers| {
        all_in(ObservedState::Running, 2)(workers)
            && workers
                .iter()
                .all(|worker| !old_pids.contains(&worker.identity.unwrap().pid))
    })
    .await;
    assert!(old_pids.into_iter().all(|pid| !exists(pid)));
    b.stop_project(&StopParams {
        id,
        idempotency_key: None,
    })
    .await
    .unwrap();
    let stopped = until_status(&mut a, id, all_in(ObservedState::Stopped, 2)).await;
    assert!(
        stopped
            .iter()
            .all(|worker| worker.desired_state == DesiredState::Stopped && worker.restarts == 0)
    );
    assert!(
        current
            .into_iter()
            .all(|worker| !exists(worker.identity.unwrap().pid))
    );
    a.remove_project(&RemoveParams {
        id,
        idempotency_key: None,
    })
    .await
    .unwrap();
    poll.abort();
}

#[tokio::test]
async fn unexpected_failures_back_off_and_manual_stop_cancels_retry() {
    let h = Harness::new();
    let app = h.app("app", "fail");
    let service = h.service();
    let poll = Harness::poll(&service);
    let (mut client, _) = Harness::client(&service).await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::OnFailure)
        .await;
    client.start_project(&h.launch(id)).await.unwrap();
    let failed = until_status(&mut client, id, all_in(ObservedState::Backoff, 1)).await;
    assert_eq!(failed[0].last_exit.unwrap().code, Some(7));
    assert!(
        failed[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("without stderr")
    );
    let retried = until_status(&mut client, id, |workers| {
        all_in(ObservedState::Backoff, 1)(workers) && workers[0].generation == 2
    })
    .await;
    assert_eq!(retried[0].restarts, 1);
    assert_eq!(retried[0].rapid_failures, 2);
    client
        .stop_project(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2250)).await;
    let stopped = until_status(&mut client, id, all_in(ObservedState::Stopped, 1)).await;
    assert_eq!(stopped[0].generation, 2);
    poll.abort();
}

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

async fn worker_pid(client: &mut Client, id: ProjectId) -> u32 {
    until_status(client, id, all_in(ObservedState::Running, 1)).await[0]
        .identity
        .unwrap()
        .pid
}

async fn until_watch(
    client: &mut Client,
    id: ProjectId,
    condition: impl Fn(&WatchStatus) -> bool,
) -> WatchStatus {
    let limit = Instant::now() + TOTAL_WAIT;
    loop {
        let status = client.watch_status(&ProjectParams { id }).await.unwrap();
        if condition(&status) {
            return status;
        }
        assert!(
            Instant::now() < limit,
            "unexpected watch status: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn descendant_pid(app: &Path) -> u32 {
    let mut pid = None;
    eventually(|| {
        pid = fs::read_to_string(app.join("descendant.pid"))
            .ok()
            .and_then(|text| text.parse().ok());
        pid.is_some()
    })
    .await;
    pid.unwrap()
}

async fn terminate(daemon: &mut OwnedChild) {
    assert!(daemon.0.try_wait().unwrap().is_none());
    // SAFETY: daemon is our unreaped Child, and try_wait confirmed it was still running.
    assert_eq!(
        unsafe { libc::kill(daemon.0.id() as i32, libc::SIGTERM) },
        0
    );
    eventually(|| daemon.0.try_wait().unwrap().is_some()).await;
    assert!(daemon.0.wait().unwrap().success());
}

#[tokio::test]
async fn real_daemon_signals_recover_only_opted_in_intent_after_cleanup() {
    use std::os::unix::process::ExitStatusExt;

    let h = Harness::new();
    let app = h.app("app", "ignore");
    let other_app = h.app("other", "graceful");
    let mut bystander = OwnedChild(Command::new("/bin/sleep").arg("30").spawn().unwrap());
    let (mut daemon, mut client) = h.daemon().await;
    assert_eq!(
        client.status().await.unwrap().startup,
        Some(DaemonStartup::FirstStart)
    );
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let other = h
        .register(&mut client, &other_app, 1, CrashRestartPolicy::Never)
        .await;

    client
        .set_autostart(&AutostartParams {
            id,
            enabled: true,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let launch = h.launch(id);
    client.start_project(&launch).await.unwrap();
    client.start_project(&h.launch(other)).await.unwrap();
    let first = worker_pid(&mut client, id).await;
    let first_descendant = descendant_pid(&app).await;
    let other_worker = worker_pid(&mut client, other).await;

    terminate(&mut daemon).await;
    assert!(!exists(first) && !exists(other_worker));
    eventually(|| !exists(first_descendant)).await;
    fs::remove_file(app.join("descendant.pid")).unwrap();

    let (mut daemon, mut client) = h.daemon().await;
    assert_eq!(
        client.status().await.unwrap().startup,
        Some(DaemonStartup::AfterCleanShutdown)
    );
    let second = worker_pid(&mut client, id).await;
    let second_descendant = descendant_pid(&app).await;
    assert_ne!(first, second);
    let dormant = client
        .list_projects()
        .await
        .unwrap()
        .into_iter()
        .find(|project| project.id == other)
        .unwrap();
    assert_eq!(dormant.desired_state, DesiredState::Running);
    assert!(!dormant.autostart);
    assert!(
        client
            .list_processes(&ProjectParams { id: other })
            .await
            .unwrap()
            .is_empty()
    );
    fs::remove_file(app.join("descendant.pid")).unwrap();

    daemon.0.kill().unwrap();
    assert_eq!(daemon.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    let (mut daemon, mut client) = h.daemon().await;
    assert_eq!(
        client.status().await.unwrap().startup,
        Some(DaemonStartup::AfterInterruptedShutdown)
    );
    let denied = client.start_project(&h.launch(id)).await;
    assert!(
        matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("cleanup"))
    );
    let third = worker_pid(&mut client, id).await;
    assert_ne!(second, third);
    assert!(!exists(second));
    eventually(|| !exists(second_descendant)).await;
    assert!(bystander.0.try_wait().unwrap().is_none());

    client
        .stop_project(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let stopped = until_status(&mut client, id, all_in(ObservedState::Stopped, 1)).await;
    assert_eq!(
        stopped[0].last_exit.unwrap().stop_cause,
        Some(StopCause::ProjectStop)
    );
    terminate(&mut daemon).await;
    let (mut daemon, mut client) = h.daemon().await;
    // A successful, old start receipt acknowledges history and cannot re-apply intent.
    assert_eq!(
        client
            .start_project(&launch)
            .await
            .unwrap()
            .result
            .desired_state,
        DesiredState::Running
    );
    assert!(
        client
            .list_processes(&ProjectParams { id })
            .await
            .unwrap()
            .is_empty()
    );
    let stopped = client
        .list_projects()
        .await
        .unwrap()
        .into_iter()
        .find(|project| project.id == id)
        .unwrap();
    assert_eq!(stopped.desired_state, DesiredState::Stopped);
    assert!(stopped.autostart);
    terminate(&mut daemon).await;
}

#[tokio::test]
async fn horizon_requires_explicit_confirmation_and_unconfirmed_lease_blocks_start() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    fs::write(
        app.join("composer.json"),
        r#"{"require":{"laravel/horizon":"* "}}"#,
    )
    .unwrap();
    let service = h.service();
    let (mut client, serve) = Harness::client(&service).await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Always)
        .await;
    let denied = client.start_project(&h.launch(id)).await;
    assert!(
        matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("Horizon"))
    );

    let store = Store::open(&h.db).unwrap();
    let profile = store.list_worker_profiles(id).unwrap().remove(0);
    let lease = roost_core::worker_runtime::WorkerLease {
        id: Uuid::new_v4(),
        project_id: id,
        profile_id: profile.id,
        slot: 0,
        state: WorkerLeaseState::Reserved,
    };
    store.reserve_process(&lease).unwrap();
    store
        .cancel_unclaimed_processes::<roost_core::WorkerProfileId>()
        .unwrap();
    assert!(store.claim_process(lease.clone()).is_err());
    store.reserve_process(&lease).unwrap();
    store.claim_process(lease.clone()).unwrap();
    assert!(store.claim_process(lease.clone()).is_err());
    store
        .cancel_unclaimed_processes::<roost_core::WorkerProfileId>()
        .unwrap();
    assert!(matches!(
        store
            .process_lease::<roost_core::WorkerProfileId>(lease.id)
            .unwrap()
            .unwrap()
            .state,
        WorkerLeaseState::Claimed
    ));
    let mut params = h.launch(id);
    params.allow_horizon = true;
    let denied = client.start_project(&params).await;
    assert!(
        matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("recovery"))
    );
    assert!(client.list_processes(&ProjectParams { id }).await.unwrap()[0].recovery_blocked);

    client
        .set_autostart(&AutostartParams {
            id,
            enabled: true,
            idempotency_key: None,
        })
        .await
        .unwrap();
    store
        .set_project_intent(
            id,
            &ProjectIntent::Running(LaunchIntent {
                php: h.php.clone(),
                allow_horizon: true,
            }),
        )
        .unwrap();
    drop(client);
    serve.await.unwrap();
    drop(service);
    let recovered = h.service();
    let poll = Harness::poll(&recovered);
    let (mut client, _) = Harness::client(&recovered).await;
    let blocked = until_status(&mut client, id, |workers| {
        workers.iter().any(|worker| worker.recovery_blocked)
    })
    .await;
    assert_eq!(blocked[0].desired_state, DesiredState::Running);
    assert!(blocked[0].identity.is_none());
    assert_eq!(
        store
            .project_leases::<roost_core::WorkerProfileId>(id)
            .unwrap()
            .len(),
        1
    );

    client
        .stop_project(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    assert_eq!(store.project_intent(id).unwrap(), ProjectIntent::Stopped);
    poll.abort();
}

#[tokio::test]
async fn invalid_executable_is_reported_without_a_shell_fallback() {
    use std::os::unix::fs::PermissionsExt;

    let h = Harness::new();
    let app = h.app("app", "graceful");
    let invalid = h.root.join("invalid-php");
    fs::write(&invalid, "touch should-not-exist\n").unwrap();
    fs::set_permissions(&invalid, fs::Permissions::from_mode(0o700)).unwrap();
    let service = h.service();
    let poll = Harness::poll(&service);
    let (mut client, _) = Harness::client(&service).await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Always)
        .await;
    let mut launch = h.launch(id);
    launch.php = invalid;
    client.start_project(&launch).await.unwrap();
    let failed = until_status(&mut client, id, all_in(ObservedState::Failed, 1)).await;
    assert_eq!(failed[0].generation, 1);
    assert!(failed[0].last_exit.is_none());
    assert_eq!(failed[0].launch_failure, Some(LaunchFailure::SpawnRejected));
    assert!(!failed[0].recovery_blocked);
    assert!(!app.join("should-not-exist").exists());
    poll.abort();
}

#[tokio::test]
async fn keeps_useful_crash_output_across_reopen_and_explains_clean_exit() {
    let h = Harness::new();
    let trace = h.app("trace", "trace_fail");
    let clean = h.app("clean", "clean_exit");
    let service = h.service();
    let poll = Harness::poll(&service);
    let (mut client, serve) = Harness::client(&service).await;
    let crashed = h
        .register(&mut client, &trace, 1, CrashRestartPolicy::Never)
        .await;
    let finished = h
        .register(&mut client, &clean, 1, CrashRestartPolicy::OnFailure)
        .await;
    client.start_project(&h.launch(crashed)).await.unwrap();
    client.start_project(&h.launch(finished)).await.unwrap();

    let failed = until_status(&mut client, crashed, all_in(ObservedState::Failed, 1)).await;
    assert!(failed[0].reason.as_ref().unwrap().contains("status 7"));
    let stopped = until_status(&mut client, finished, all_in(ObservedState::Stopped, 1)).await;
    assert_eq!(stopped[0].desired_state, DesiredState::Running);
    assert_eq!(stopped[0].last_exit.unwrap().code, Some(0));
    assert!(
        stopped[0]
            .reason
            .as_ref()
            .unwrap()
            .contains("exited cleanly")
    );
    assert!(
        stopped[0]
            .reason
            .as_ref()
            .unwrap()
            .contains("restart policy")
    );
    assert!(
        client
            .recent_crashes(&ProjectParams { id: finished })
            .await
            .unwrap()
            .is_empty()
    );

    poll.abort();
    let _ = poll.await;
    drop(client);
    serve.await.unwrap();
    drop(service);
    let service = h.service();
    let (mut client, _) = Harness::client(&service).await;
    let reports = client
        .recent_crashes(&ProjectParams { id: crashed })
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].exit.code, Some(7));
    for (id, crashes) in [(crashed, 1), (finished, 0)] {
        let summary = client.project_metrics(&ProjectParams { id }).await.unwrap();
        assert_eq!(summary["lifecycle"]["crashes"], crashes);
    }
    let report = serde_json::to_string(&reports[0]).unwrap();
    assert!(report.contains("RuntimeException: queue connection unavailable"));
    assert!(report.contains("#0 src/Queue.php(20): connect()"));
    assert!(report.contains("line redacted"));
    assert!(!report.contains("secret-fixture-output"));
}

fn scheduler_config(id: ProjectId, policy: CrashRestartPolicy) -> ConfigureSchedulerParams {
    ConfigureSchedulerParams {
        id,
        expected_revision: None,
        options: SchedulerOptions {
            enabled: true,
            graceful_stop_seconds: 1,
            crash_restart: policy,
        },
        idempotency_key: Some(Uuid::new_v4()),
    }
}

fn scheduler_launch(h: &Harness, id: ProjectId) -> SchedulerLaunchParams {
    SchedulerLaunchParams {
        id,
        php: h.php.clone(),
        idempotency_key: Some(Uuid::new_v4()),
    }
}

async fn until_scheduler(
    client: &mut Client,
    id: ProjectId,
    condition: impl Fn(&SchedulerProcessStatus) -> bool,
) -> SchedulerProcessStatus {
    let limit = Instant::now() + TOTAL_WAIT;
    loop {
        let process = client
            .scheduler_status(&ProjectParams { id })
            .await
            .unwrap()
            .process;
        if let Some(status) = process.as_ref().filter(|status| condition(status)) {
            return status.clone();
        }
        assert!(
            Instant::now() < limit,
            "unexpected scheduler status: {process:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn scheduler_ready(client: &mut Client, id: ProjectId, previous: Option<u32>) -> u32 {
    let status = until_scheduler(client, id, |process| {
        process.state == ObservedState::Running
            && process
                .identity
                .is_some_and(|identity| Some(identity.pid) != previous)
    })
    .await;
    let limit = Instant::now() + TOTAL_WAIT;
    loop {
        let lines = client.scheduler_logs(&ProjectParams { id }).await.unwrap();
        if lines
            .iter()
            .any(|line| line.instance_id == status.id && line.text == "scheduler ready")
        {
            return status.identity.unwrap().pid;
        }
        assert!(Instant::now() < limit, "scheduler did not emit readiness");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn concurrent_scheduler_requests_share_one_lease_and_queue_worker_intent_is_independent() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    let service = h.service();
    let _poll = Harness::poll(&service);
    let (mut a, _) = Harness::client(&service).await;
    let (mut b, _) = Harness::client(&service).await;
    let id = h.register(&mut a, &app, 1, CrashRestartPolicy::Never).await;
    let params = ProjectParams { id };
    let unconfigured = a.scheduler_status(&params).await.unwrap();
    assert!(unconfigured.profile.is_none());
    let denied = a.start_scheduler(&scheduler_launch(&h, id)).await;
    assert!(matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT));

    let config_a = scheduler_config(id, CrashRestartPolicy::Always);
    let config_b = scheduler_config(id, CrashRestartPolicy::Always);
    let (one, two) = tokio::join!(
        a.configure_scheduler(&config_a),
        b.configure_scheduler(&config_b)
    );
    let profile = match (one, two) {
        (Ok(result), Err(ClientError::Rpc(error))) | (Err(ClientError::Rpc(error)), Ok(result)) => {
            assert_eq!(error.code, CONFLICT);
            result.result
        }
        results => panic!("unexpected configure results: {results:?}"),
    };
    assert_eq!(profile.revision, Revision(1));
    assert!(a.scheduler_status(&params).await.unwrap().process.is_none());

    a.start_project(&h.launch(id)).await.unwrap();
    let worker = worker_pid(&mut a, id).await;
    let launch_a = scheduler_launch(&h, id);
    let launch_b = scheduler_launch(&h, id);
    let (one, two) = tokio::join!(a.start_scheduler(&launch_a), b.start_scheduler(&launch_b));
    let one = one.unwrap();
    assert_ne!(one.operation_id, two.unwrap().operation_id);
    assert_eq!(
        a.start_scheduler(&launch_a).await.unwrap().operation_id,
        one.operation_id
    );

    let first = scheduler_ready(&mut a, id, None).await;
    let store = Store::open(&h.db).unwrap();
    assert_eq!(
        store
            .project_leases::<SchedulerProfileId>(id)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .project_leases::<roost_core::WorkerProfileId>(id)
            .unwrap()
            .len(),
        1
    );

    let denied = a
        .configure_scheduler(&ConfigureSchedulerParams {
            expected_revision: Some(profile.revision),
            ..scheduler_config(id, CrashRestartPolicy::Never)
        })
        .await;
    assert!(matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT));

    a.restart_scheduler(&scheduler_launch(&h, id))
        .await
        .unwrap();
    let second = scheduler_ready(&mut a, id, Some(first)).await;
    assert!(!exists(first));
    assert!(app.join(format!("scheduler-stopped-{first}")).is_file());
    assert_eq!(worker_pid(&mut a, id).await, worker);

    a.stop_project(&StopParams {
        id,
        idempotency_key: None,
    })
    .await
    .unwrap();
    until_status(&mut a, id, all_in(ObservedState::Stopped, 1)).await;
    assert_eq!(scheduler_ready(&mut a, id, None).await, second);
    let denied = a
        .remove_project(&RemoveParams {
            id,
            idempotency_key: None,
        })
        .await;
    assert!(matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT));

    a.stop_scheduler(&StopParams {
        id,
        idempotency_key: None,
    })
    .await
    .unwrap();
    let stopped =
        until_scheduler(&mut a, id, |status| status.state == ObservedState::Stopped).await;
    let exit = stopped.last_exit.unwrap();
    assert_eq!(exit.code, Some(0));
    assert_eq!(exit.stop_cause, Some(StopCause::ProjectStop));
    assert!(!exit.forced && !exists(second));
    assert!(
        store
            .project_leases::<SchedulerProfileId>(id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.scheduler_intent(id).unwrap(),
        SchedulerIntent::Stopped
    );
    assert!(a.scheduler_crashes(&params).await.unwrap().is_empty());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        a.scheduler_status(&params)
            .await
            .unwrap()
            .process
            .unwrap()
            .state,
        ObservedState::Stopped
    );

    let disabled = a
        .configure_scheduler(&ConfigureSchedulerParams {
            expected_revision: Some(profile.revision),
            options: SchedulerOptions {
                enabled: false,
                ..SchedulerOptions::default()
            },
            ..scheduler_config(id, CrashRestartPolicy::Never)
        })
        .await
        .unwrap()
        .result;
    assert_eq!(disabled.id, profile.id);
    assert_eq!(disabled.revision, Revision(2));
    let denied = a.start_scheduler(&scheduler_launch(&h, id)).await;
    assert!(matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    let mut project = store.get_project(id).unwrap().unwrap();
    project.config_owner = roost_core::ConfigOwner::File;
    store.update_project(project).unwrap();
    let denied = a
        .configure_scheduler(&ConfigureSchedulerParams {
            expected_revision: Some(disabled.revision),
            ..scheduler_config(id, CrashRestartPolicy::Never)
        })
        .await;
    assert!(
        matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("file-owned"))
    );
}

#[tokio::test]
async fn real_daemon_recovery_waits_for_scheduler_group_and_honors_saved_stop() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    fs::write(app.join("scheduler-mode"), "ignore").unwrap();
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let params = ProjectParams { id };
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Always))
        .await
        .unwrap();
    client
        .set_autostart(&AutostartParams {
            id,
            enabled: true,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let launch = scheduler_launch(&h, id);
    client.start_scheduler(&launch).await.unwrap();
    let old = scheduler_ready(&mut client, id, None).await;
    let descendant = descendant_pid(&app).await;

    daemon.0.kill().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(daemon.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    let (mut daemon, mut client) = h.daemon().await;
    assert_eq!(
        client.status().await.unwrap().startup,
        Some(DaemonStartup::AfterInterruptedShutdown)
    );
    let blocked = client.start_scheduler(&scheduler_launch(&h, id)).await;
    assert!(
        matches!(blocked, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("cleanup"))
    );
    let replacement = scheduler_ready(&mut client, id, Some(old)).await;
    assert!(!exists(old));
    eventually(|| !exists(descendant)).await;
    let store = Store::open(&h.root.join("daemon/roost.sqlite3")).unwrap();
    assert_eq!(
        store
            .project_leases::<SchedulerProfileId>(id)
            .unwrap()
            .len(),
        1
    );
    assert!(client.list_processes(&params).await.unwrap().is_empty());

    client
        .stop_scheduler(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    until_scheduler(&mut client, id, |status| {
        status.state == ObservedState::Stopped
    })
    .await;
    assert!(!exists(replacement));
    terminate(&mut daemon).await;
    let (mut daemon, mut client) = h.daemon().await;
    assert_eq!(
        client
            .start_scheduler(&launch)
            .await
            .unwrap()
            .result
            .desired_state,
        DesiredState::Running
    );
    let dormant = client.scheduler_status(&params).await.unwrap();
    assert_eq!(dormant.desired_state, DesiredState::Stopped);
    assert!(dormant.process.is_none());
    terminate(&mut daemon).await;
}

#[tokio::test]
async fn scheduler_restart_after_terminal_failure_waits_for_lease_confirmation() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    let mode = app.join("scheduler-mode");
    fs::write(&mode, "fail").unwrap();
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let params = ProjectParams { id };
    let profile = client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
        .await
        .unwrap()
        .result;
    client
        .start_scheduler(&scheduler_launch(&h, id))
        .await
        .unwrap();
    let failed = until_scheduler(&mut client, id, |status| {
        status.state == ObservedState::Failed
    })
    .await;
    assert_eq!(failed.last_exit.unwrap().code, Some(7));
    assert!(failed.identity.is_none());
    assert!(failed.retry_in_ms.is_none());
    assert!(!failed.recovery_blocked);
    let store = Store::open(&h.root.join("daemon/roost.sqlite3")).unwrap();
    assert!(
        store
            .project_leases::<SchedulerProfileId>(id)
            .unwrap()
            .is_empty()
    );

    // Inject an outstanding guardian claim while the daemon retains the
    // terminal runtime. The claim has no child, so no process can leak.
    let lease = SchedulerLease {
        id: Uuid::new_v4(),
        project_id: id,
        profile_id: profile.id,
        slot: 0,
        state: WorkerLeaseState::Reserved,
    };
    store.reserve_process(&lease).unwrap();
    let pending = store.claim_process(lease).unwrap();
    fs::write(&mode, "graceful").unwrap();
    let restart = scheduler_launch(&h, id);
    let blocked = client.restart_scheduler(&restart).await;
    assert!(
        matches!(blocked, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("cleanup"))
    );
    let retained = client
        .scheduler_status(&params)
        .await
        .unwrap()
        .process
        .unwrap();
    assert_eq!(retained.id, failed.id);
    assert_eq!(retained.state, ObservedState::Failed);
    assert_eq!(retained.generation, failed.generation);
    assert_eq!(
        store.project_leases::<SchedulerProfileId>(id).unwrap(),
        vec![pending.clone()]
    );

    // Publish the terminal record a guardian would write after confirming
    // that it cancelled before spawning. Retry the exact rejected request.
    store
        .record_process_state(
            pending.clone(),
            WorkerLeaseState::LaunchFailure {
                failure: LaunchFailure::Cancelled,
            },
        )
        .unwrap();
    client.restart_scheduler(&restart).await.unwrap();
    let pid = scheduler_ready(&mut client, id, None).await;
    let status = client
        .scheduler_status(&params)
        .await
        .unwrap()
        .process
        .unwrap();
    assert_ne!(status.id, failed.id);
    assert_eq!(status.generation, 1);
    let [replacement] = store
        .project_leases::<SchedulerProfileId>(id)
        .unwrap()
        .try_into()
        .unwrap();
    assert_ne!(replacement.id, pending.id);
    assert_eq!(
        replacement.state,
        WorkerLeaseState::Running {
            identity: status.identity.unwrap(),
        }
    );
    assert_eq!(client.scheduler_crashes(&params).await.unwrap().len(), 1);
    terminate(&mut daemon).await;
    assert!(!exists(pid));
}

#[tokio::test]
async fn real_daemon_recovers_scheduler_and_queue_worker_for_the_same_project() {
    use std::os::unix::process::ExitStatusExt;

    let h = Harness::new();
    let app = h.app("app", "graceful");
    fs::write(app.join("scheduler-mode"), "ignore").unwrap();
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
        .await
        .unwrap();
    client
        .set_autostart(&AutostartParams {
            id,
            enabled: true,
            idempotency_key: None,
        })
        .await
        .unwrap();
    client.start_project(&h.launch(id)).await.unwrap();
    client
        .start_scheduler(&scheduler_launch(&h, id))
        .await
        .unwrap();
    let old_worker = worker_pid(&mut client, id).await;
    let old_scheduler = scheduler_ready(&mut client, id, None).await;
    let old_descendant = descendant_pid(&app).await;
    assert!(exists(old_worker) && exists(old_scheduler) && exists(old_descendant));
    let store = Store::open(&h.root.join("daemon/roost.sqlite3")).unwrap();
    let [old_worker_lease] = store
        .project_leases::<WorkerProfileId>(id)
        .unwrap()
        .try_into()
        .unwrap();
    let [old_scheduler_lease] = store
        .project_leases::<SchedulerProfileId>(id)
        .unwrap()
        .try_into()
        .unwrap();
    let worker_intent = store.project_intent(id).unwrap();
    let scheduler_intent = store.scheduler_intent(id).unwrap();
    assert!(matches!(worker_intent, ProjectIntent::Running(_)));
    assert!(matches!(scheduler_intent, SchedulerIntent::Running { .. }));

    fs::remove_file(app.join("descendant.pid")).unwrap();
    daemon.0.kill().unwrap();
    assert_eq!(daemon.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    let (mut daemon, mut client) = h.daemon().await;
    assert_eq!(
        client.status().await.unwrap().startup,
        Some(DaemonStartup::AfterInterruptedShutdown)
    );
    let worker = worker_pid(&mut client, id).await;
    let scheduler = scheduler_ready(&mut client, id, Some(old_scheduler)).await;
    let descendant = descendant_pid(&app).await;
    assert_ne!(worker, old_worker);
    assert_ne!(descendant, old_descendant);
    assert_ne!(worker, scheduler);
    assert!(!exists(old_worker) && !exists(old_scheduler));
    eventually(|| !exists(old_descendant)).await;
    assert!(exists(worker) && exists(scheduler) && exists(descendant));
    let [worker_lease] = store
        .project_leases::<WorkerProfileId>(id)
        .unwrap()
        .try_into()
        .unwrap();
    let [scheduler_lease] = store
        .project_leases::<SchedulerProfileId>(id)
        .unwrap()
        .try_into()
        .unwrap();
    assert_ne!(worker_lease.id, old_worker_lease.id);
    assert_ne!(scheduler_lease.id, old_scheduler_lease.id);
    for (state, pid) in [
        (worker_lease.state, worker),
        (scheduler_lease.state, scheduler),
    ] {
        assert!(matches!(state, WorkerLeaseState::Running { identity } if identity.pid == pid));
    }
    assert_eq!(store.project_intent(id).unwrap(), worker_intent);
    assert_eq!(store.scheduler_intent(id).unwrap(), scheduler_intent);

    terminate(&mut daemon).await;
    assert!(!exists(worker) && !exists(scheduler));
    eventually(|| !exists(descendant)).await;
}

#[tokio::test]
async fn scheduler_crash_backoff_and_diagnostics_do_not_enter_worker_history() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    fs::write(app.join("scheduler-mode"), "trace_fail").unwrap();
    let service = h.service();
    let _poll = Harness::poll(&service);
    let (mut client, _) = Harness::client(&service).await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let params = ProjectParams { id };
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::OnFailure))
        .await
        .unwrap();
    client
        .start_scheduler(&scheduler_launch(&h, id))
        .await
        .unwrap();
    let crashed = until_scheduler(&mut client, id, |status| {
        status.state == ObservedState::Backoff && status.generation == 2
    })
    .await;
    assert_eq!(crashed.last_exit.unwrap().code, Some(7));
    let reports = client.scheduler_crashes(&params).await.unwrap();
    assert_eq!(reports.len(), 2);
    assert!(
        reports[0]
            .output
            .iter()
            .any(|line| line.text.contains("#0 src/Queue.php"))
    );
    assert!(
        !serde_json::to_string(&reports)
            .unwrap()
            .contains("secret-fixture-output")
    );
    assert!(client.recent_crashes(&params).await.unwrap().is_empty());
    client
        .stop_scheduler(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let stopped = until_scheduler(&mut client, id, |status| {
        status.state == ObservedState::Stopped
    })
    .await;
    assert_eq!(stopped.generation, 2);
}

#[tokio::test]
async fn atomic_saves_and_deletions_restart_only_opted_in_workers_in_the_owning_project() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    let nested = h.app("app/nested", "graceful");
    let file = app.join("Example.php");
    let nested_file = nested.join("Example.php");
    fs::write(&file, "parent original").unwrap();
    fs::write(&nested_file, "child original").unwrap();
    fs::create_dir(app.join("vendor")).unwrap();
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let child = h
        .register(&mut client, &nested, 1, CrashRestartPolicy::Never)
        .await;
    let pinned = client
        .create_worker(&CreateWorkerParams {
            project_id: id,
            name: "Pinned worker".into(),
            connection: "database".into(),
            options: WorkerOptions {
                queues: vec!["critical".into(), "default".into()],
                graceful_stop_seconds: 1,
                crash_restart: CrashRestartPolicy::Never,
                ..WorkerOptions::default()
            },
            idempotency_key: None,
        })
        .await
        .unwrap()
        .result;
    let mut watched = Vec::new();
    for project in [id, child] {
        let profile = client
            .list_profiles(&ProjectParams { id: project })
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.name == "Queue Worker")
            .unwrap();
        client
            .set_code_restart(&CodeRestartParams {
                id: profile.id,
                expected_revision: profile.revision,
                policy: CodeRestartPolicy::OnChange,
                idempotency_key: None,
            })
            .await
            .unwrap();
        watched.push(profile.id);
        let status = client
            .watch_status(&ProjectParams { id: project })
            .await
            .unwrap();
        client
            .enable_watch(&EnableWatchParams {
                id: project,
                expected_revision: status.revision,
                rules: None,
                idempotency_key: None,
            })
            .await
            .unwrap();
        until_watch(&mut client, project, |status| {
            status.state == WatchState::Watching
        })
        .await;
        client.start_project(&h.launch(project)).await.unwrap();
    }
    let opted = watched[0];
    let old = until_status(&mut client, id, all_in(ObservedState::Running, 2)).await;
    let old_pid = |profile| {
        old.iter()
            .find(|s| s.profile_id == profile)
            .unwrap()
            .identity
            .unwrap()
            .pid
    };
    let child_pid = worker_pid(&mut client, child).await;
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
        .await
        .unwrap();
    client
        .start_scheduler(&scheduler_launch(&h, id))
        .await
        .unwrap();
    let scheduler = scheduler_ready(&mut client, id, None).await;
    fs::write(app.join("vendor/excluded.php"), "ignored").unwrap();
    let temporary = app.join("editor.tmp");
    fs::write(&temporary, "atomically replaced PHP").unwrap();
    fs::rename(temporary, &file).unwrap();
    let running_after_save = until_status(&mut client, id, |workers| {
        all_in(ObservedState::Running, 2)(workers)
            && workers
                .iter()
                .any(|worker| worker.profile_id == opted && worker.generation == 2)
    })
    .await;
    let replacement = running_after_save
        .iter()
        .find(|s| s.profile_id == opted)
        .unwrap()
        .identity
        .unwrap()
        .pid;
    assert_ne!(replacement, old_pid(opted));
    assert!(!exists(old_pid(opted)));
    assert!(
        running_after_save
            .iter()
            .any(|s| s.profile_id == pinned.id && s.identity.unwrap().pid == old_pid(pinned.id))
    );
    assert_eq!(worker_pid(&mut client, child).await, child_pid);
    assert_eq!(scheduler_ready(&mut client, id, None).await, scheduler);

    fs::remove_file(nested_file).unwrap();
    let nested_replacement = until_status(&mut client, child, |workers| {
        all_in(ObservedState::Running, 1)(workers) && workers[0].generation == 2
    })
    .await[0]
        .identity
        .unwrap()
        .pid;
    assert_ne!(nested_replacement, child_pid);
    assert!(!exists(child_pid));
    let parent = client.list_processes(&ProjectParams { id }).await.unwrap();
    assert!(
        parent
            .iter()
            .any(|s| s.profile_id == opted && s.identity.unwrap().pid == replacement)
    );

    fs::write(&file, "pending manual stop").unwrap();
    until_watch(&mut client, id, |status| status.pending).await;
    client
        .stop_project(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    until_status(&mut client, id, all_in(ObservedState::Stopped, 2)).await;
    until_watch(&mut client, id, |status| !status.pending).await;
    let statuses = client.list_processes(&ProjectParams { id }).await.unwrap();
    assert!(all_in(ObservedState::Stopped, 2)(&statuses));
    let store = Store::open(&h.root.join("daemon/roost.sqlite3")).unwrap();
    assert_eq!(store.project_intent(id).unwrap(), ProjectIntent::Stopped);
    assert!(
        store
            .project_leases::<WorkerProfileId>(id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(scheduler_ready(&mut client, id, None).await, scheduler);
    terminate(&mut daemon).await;
}

#[tokio::test]
async fn watch_configuration_is_revisioned_durable_and_file_ownership_is_enforced() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let params = ProjectParams { id };
    let disabled = client.watch_status(&params).await.unwrap();
    assert_eq!(disabled.state, WatchState::Disabled);
    let invalid = client
        .enable_watch(&EnableWatchParams {
            id,
            expected_revision: disabled.revision,
            rules: Some(WatchRules {
                include: vec!["../*.php".into()],
                exclude: vec![],
            }),
            idempotency_key: None,
        })
        .await;
    assert!(matches!(invalid, Err(ClientError::Rpc(error)) if error.code == INVALID_PARAMS));
    let enable = EnableWatchParams {
        id,
        expected_revision: disabled.revision,
        rules: Some(WatchRules {
            include: vec!["config/**/*.php".into()],
            exclude: vec![],
        }),
        idempotency_key: Some(Uuid::new_v4()),
    };
    let receipt = client.enable_watch(&enable).await.unwrap();
    assert_eq!(
        client.enable_watch(&enable).await.unwrap().operation_id,
        receipt.operation_id
    );
    let stale = client
        .disable_watch(&DisableWatchParams {
            id,
            expected_revision: disabled.revision,
            idempotency_key: None,
        })
        .await;
    assert!(matches!(stale, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    terminate(&mut daemon).await;

    let (mut daemon, mut client) = h.daemon().await;
    let watching = until_watch(&mut client, id, |status| {
        status.state == WatchState::Watching
    })
    .await;
    assert_eq!(watching.revision, receipt.result.revision);
    assert_eq!(watching.rules, receipt.result.watch_rules);
    assert!(!watching.pending);
    assert!(client.list_processes(&params).await.unwrap().is_empty());
    let mut project = client
        .disable_watch(&DisableWatchParams {
            id,
            expected_revision: watching.revision,
            idempotency_key: None,
        })
        .await
        .unwrap()
        .result;
    assert_eq!(
        client.watch_status(&params).await.unwrap().state,
        WatchState::Disabled
    );
    project.config_owner = roost_core::ConfigOwner::File;
    let store = Store::open(&h.root.join("daemon/roost.sqlite3")).unwrap();
    let project = store.update_project(project).unwrap();
    let denied = client
        .enable_watch(&EnableWatchParams {
            id,
            expected_revision: project.revision,
            rules: None,
            idempotency_key: None,
        })
        .await;
    assert!(
        matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("file-owned"))
    );
    let profile = client.list_profiles(&params).await.unwrap().remove(0);
    let denied = client
        .set_code_restart(&CodeRestartParams {
            id: profile.id,
            expected_revision: profile.revision,
            policy: CodeRestartPolicy::OnChange,
            idempotency_key: None,
        })
        .await;
    assert!(
        matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("file-owned"))
    );
    terminate(&mut daemon).await;
}

async fn group_members(client: &mut Client, root: &Path, members: &[ProjectId]) -> Group {
    let mut group = client
        .create_group(&CreateGroupParams {
            name: "Apps".into(),
            options: GroupOptions {
                root_path: Some(root.into()),
                ..GroupOptions::default()
            },
            idempotency_key: None,
        })
        .await
        .unwrap()
        .result;
    for &project_id in members {
        let params = GroupMembershipParams {
            id: group.id,
            project_id,
            expected_revision: group.revision,
            idempotency_key: Some(Uuid::new_v4()),
        };
        let added = client.add_project_to_group(&params).await.unwrap();
        let replay = client.add_project_to_group(&params).await.unwrap();
        assert_eq!(replay.operation_id, added.operation_id);
        group = added.result.group;
    }
    group
}

#[tokio::test]
async fn group_membership_and_shared_paths_are_revisioned_and_durable() {
    let h = Harness::new();
    let app = h.app("app", "graceful");
    let other_app = h.app("other", "graceful");
    let shared = h.root.join("shared");
    fs::create_dir(&shared).unwrap();
    fs::create_dir(shared.join("nested")).unwrap();
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let other = h
        .register(&mut client, &other_app, 1, CrashRestartPolicy::Never)
        .await;
    let group = group_members(&mut client, &h.root, &[id]).await;
    let rival = group_members(&mut client, &h.root, &[]).await;
    let denied = client
        .add_project_to_group(&GroupMembershipParams {
            id: rival.id,
            project_id: id,
            expected_revision: rival.revision,
            idempotency_key: None,
        })
        .await;
    assert!(matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    let stale = client
        .add_project_to_group(&GroupMembershipParams {
            id: group.id,
            project_id: other,
            expected_revision: Revision(1),
            idempotency_key: None,
        })
        .await;
    assert!(matches!(stale, Err(ClientError::Rpc(error)) if error.code == CONFLICT));

    let update = |path: &str, projects: Vec<ProjectId>| UpdateGroupParams {
        id: group.id,
        name: "Monorepo".into(),
        expected_revision: group.revision,
        options: GroupOptions {
            root_path: group.root_path.clone(),
            shared_paths: vec![SharedWatchPath {
                path: path.into(),
                projects,
            }],
            ..GroupOptions::default()
        },
        idempotency_key: None,
    };
    let escapes = client.update_group(&update("../shared", vec![id])).await;
    assert!(matches!(escapes, Err(ClientError::Rpc(error)) if error.code == INVALID_PARAMS));
    std::os::unix::fs::symlink(&shared, h.root.join("link")).unwrap();
    let symlink = client.update_group(&update("link", vec![id])).await;
    assert!(matches!(symlink, Err(ClientError::Rpc(error)) if error.code == INVALID_PARAMS));
    let outside = client.update_group(&update("shared", vec![other])).await;
    assert!(matches!(outside, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    for (first, second) in [("shared", "shared/nested"), ("shared/nested", "shared")] {
        let mut overlap = update(first, vec![id]);
        overlap.options.shared_paths.push(SharedWatchPath {
            path: second.into(),
            projects: vec![id],
        });
        let denied = client.update_group(&overlap).await;
        assert!(
            matches!(denied, Err(ClientError::Rpc(error)) if error.code == CONFLICT && error.message.contains("overlap"))
        );
        assert_eq!(
            client
                .get_group(&GroupParams { id: group.id })
                .await
                .unwrap()
                .group,
            group
        );
    }
    let group = client
        .update_group(&update("shared", vec![id]))
        .await
        .unwrap()
        .result;
    let removal = GroupMembershipParams {
        id: group.id,
        project_id: id,
        expected_revision: group.revision,
        idempotency_key: None,
    };
    let referenced = client.remove_project_from_group(&removal).await;
    assert!(matches!(referenced, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    let nonempty = client
        .delete_group(&DeleteGroupParams {
            id: group.id,
            expected_revision: group.revision,
            idempotency_key: None,
        })
        .await;
    assert!(matches!(nonempty, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    terminate(&mut daemon).await;

    let (mut daemon, mut client) = h.daemon().await;
    let details = client
        .get_group(&GroupParams { id: group.id })
        .await
        .unwrap();
    assert_eq!(details.group, group);
    assert_eq!(details.members.len(), 1);
    assert_eq!(details.members[0].project.id, id);
    let cleared = client
        .update_group(&UpdateGroupParams {
            id: group.id,
            name: group.name,
            expected_revision: group.revision,
            options: GroupOptions::default(),
            idempotency_key: None,
        })
        .await
        .unwrap()
        .result;
    let empty = client
        .remove_project_from_group(&GroupMembershipParams {
            expected_revision: cleared.revision,
            ..removal
        })
        .await
        .unwrap()
        .result;
    assert!(empty.members.is_empty());
    client
        .delete_group(&DeleteGroupParams {
            id: group.id,
            expected_revision: empty.group.revision,
            idempotency_key: None,
        })
        .await
        .unwrap();
    assert_eq!(client.list_groups().await.unwrap(), [rival]);
    terminate(&mut daemon).await;
}

#[tokio::test]
async fn group_lifecycle_reports_partial_failure_and_shared_edits_restart_only_mapped_workers() {
    let h = Harness::new();
    let app = h.app("alpha", "graceful");
    let other_app = h.app("beta", "graceful");
    let shared = other_app.join("shared");
    fs::create_dir(&shared).unwrap();
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let other = h
        .register(&mut client, &other_app, 1, CrashRestartPolicy::Never)
        .await;
    let group = group_members(&mut client, &h.root, &[id, other]).await;
    let group = client
        .update_group(&UpdateGroupParams {
            id: group.id,
            name: "Monorepo".into(),
            expected_revision: group.revision,
            options: GroupOptions {
                root_path: group.root_path.clone(),
                shared_paths: vec![SharedWatchPath {
                    path: "beta/shared".into(),
                    projects: vec![id],
                }],
                ..GroupOptions::default()
            },
            idempotency_key: None,
        })
        .await
        .unwrap()
        .result;
    for project in [id, other] {
        let params = ProjectParams { id: project };
        let profile = client.list_profiles(&params).await.unwrap().remove(0);
        client
            .set_code_restart(&CodeRestartParams {
                id: profile.id,
                expected_revision: profile.revision,
                policy: CodeRestartPolicy::OnChange,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let status = client.watch_status(&params).await.unwrap();
        client
            .enable_watch(&EnableWatchParams {
                id: project,
                expected_revision: status.revision,
                rules: None,
                idempotency_key: None,
            })
            .await
            .unwrap();
    }
    until_watch(&mut client, id, |status| {
        status.state == WatchState::Watching
            && status.shared.len() == 1
            && status.shared[0].state == WatchState::Watching
    })
    .await;
    assert!(
        until_watch(&mut client, other, |status| status.state
            == WatchState::Watching)
        .await
        .shared
        .is_empty()
    );
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
        .await
        .unwrap();
    let plan = |other_php| GroupLaunchParams {
        id: group.id,
        expected_revision: group.revision,
        idempotency_key: Some(Uuid::new_v4()),
        launches: vec![
            GroupProjectLaunch {
                id,
                php: h.php.clone(),
                allow_horizon: false,
            },
            GroupProjectLaunch {
                id: other,
                php: other_php,
                allow_horizon: false,
            },
        ],
    };
    let partial = plan(h.root.join("missing-php"));
    let start = client.start_group(&partial).await.unwrap();
    assert!(matches!(
        start.result.projects[0].outcome,
        GroupOutcome::Accepted {
            workers: true,
            scheduler: true
        }
    ));
    assert!(matches!(
        start.result.projects[1].outcome,
        GroupOutcome::Failed { .. }
    ));
    assert_eq!(
        client.start_group(&partial).await.unwrap().operation_id,
        start.operation_id
    );
    assert!(
        client
            .list_processes(&ProjectParams { id: other })
            .await
            .unwrap()
            .is_empty()
    );
    let first = worker_pid(&mut client, id).await;
    let first_scheduler = scheduler_ready(&mut client, id, None).await;
    let restart = plan(h.php.clone());
    let restarted = client.restart_group(&restart).await.unwrap();
    assert!(
        restarted
            .result
            .projects
            .iter()
            .all(|entry| matches!(entry.outcome, GroupOutcome::Accepted { .. }))
    );
    let scheduler = scheduler_ready(&mut client, id, Some(first_scheduler)).await;
    let worker = worker_pid(&mut client, id).await;
    let other_worker = worker_pid(&mut client, other).await;
    assert_ne!(worker, first);
    assert!(!exists(first) && !exists(first_scheduler));

    let file = shared.join("Watched.php");
    fs::write(&file, "<?php // shared source").unwrap();
    let replacement = until_status(&mut client, id, |workers| {
        all_in(ObservedState::Running, 1)(workers) && workers[0].identity.unwrap().pid != worker
    })
    .await[0]
        .identity
        .unwrap()
        .pid;
    assert!(!exists(worker));
    assert_eq!(worker_pid(&mut client, other).await, other_worker);
    assert_eq!(scheduler_ready(&mut client, id, None).await, scheduler);

    let other_params = ProjectParams { id: other };
    let enabled = client.watch_status(&other_params).await.unwrap();
    client
        .disable_watch(&DisableWatchParams {
            id: other,
            expected_revision: enabled.revision,
            idempotency_key: None,
        })
        .await
        .unwrap();
    client
        .update_group(&UpdateGroupParams {
            id: group.id,
            name: group.name.clone(),
            expected_revision: group.revision,
            options: GroupOptions {
                root_path: group.root_path.clone(),
                shared_paths: vec![SharedWatchPath {
                    path: "beta/shared".into(),
                    projects: vec![id, other],
                }],
                ..GroupOptions::default()
            },
            idempotency_key: None,
        })
        .await
        .unwrap();
    let opted_out = client.watch_status(&other_params).await.unwrap();
    assert_eq!(opted_out.state, WatchState::Disabled);
    assert!(!opted_out.pending);
    assert_eq!(opted_out.shared.len(), 1);
    let mapping = &opted_out.shared[0];
    assert_eq!(mapping.group_id, group.id);
    assert_eq!(mapping.path, Path::new("beta/shared"));
    assert_eq!(mapping.state, WatchState::Disabled);
    assert!(!mapping.pending && mapping.problem.is_none());
    assert_eq!(
        client
            .watch_status(&ProjectParams { id })
            .await
            .unwrap()
            .shared[0]
            .state,
        WatchState::Watching
    );

    fs::write(&file, "<?php // another shared change").unwrap();
    until_watch(&mut client, id, |status| {
        status.shared.iter().any(|mapping| mapping.pending)
    })
    .await;
    client
        .stop_project(&StopParams {
            id,
            idempotency_key: None,
        })
        .await
        .unwrap();
    until_status(&mut client, id, all_in(ObservedState::Stopped, 1)).await;
    until_watch(&mut client, id, |status| {
        status.shared.iter().all(|mapping| !mapping.pending)
    })
    .await;
    assert!(all_in(ObservedState::Stopped, 1)(
        &client.list_processes(&ProjectParams { id }).await.unwrap()
    ));
    assert_eq!(worker_pid(&mut client, other).await, other_worker);
    client
        .stop_group(&GroupStopParams {
            id: group.id,
            idempotency_key: Some(Uuid::new_v4()),
        })
        .await
        .unwrap();
    until_status(&mut client, other, all_in(ObservedState::Stopped, 1)).await;
    until_scheduler(&mut client, id, |status| {
        status.state == ObservedState::Stopped
    })
    .await;
    assert!(!exists(replacement) && !exists(other_worker) && !exists(scheduler));
    assert_eq!(
        client.restart_group(&restart).await.unwrap().operation_id,
        restarted.operation_id
    );
    let details = client
        .get_group(&GroupParams { id: group.id })
        .await
        .unwrap();
    assert!(
        details
            .members
            .iter()
            .all(|member| member.project.desired_state == DesiredState::Stopped)
    );
    assert_eq!(
        details.members[0].scheduler_desired_state,
        Some(DesiredState::Stopped)
    );
    terminate(&mut daemon).await;
}

#[tokio::test]
async fn group_start_reserves_combined_capacity_before_applying_any_runtime_plan() {
    let h = Harness::new();
    let app = h.app("first", "graceful");
    let other_app = h.app("second", "graceful");
    let service = h.service();
    let (mut client, _task) = Harness::client(&service).await;
    // IPC is live, but without the polling loop no guardians are spawned.
    let id = h
        .register(&mut client, &app, 16, CrashRestartPolicy::Never)
        .await;
    let other = h
        .register(&mut client, &other_app, 16, CrashRestartPolicy::Never)
        .await;
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
        .await
        .unwrap();
    let group = group_members(&mut client, &h.root, &[id, other]).await;
    let launch = GroupLaunchParams {
        id: group.id,
        expected_revision: group.revision,
        launches: [id, other]
            .into_iter()
            .map(|id| GroupProjectLaunch {
                id,
                php: h.php.clone(),
                allow_horizon: false,
            })
            .collect(),
        idempotency_key: None,
    };
    let mut results = client
        .start_group(&launch)
        .await
        .unwrap()
        .result
        .projects
        .into_iter();
    assert!(matches!(
        results.next().unwrap().outcome,
        GroupOutcome::Accepted {
            workers: true,
            scheduler: true
        }
    ));
    assert!(
        matches!(results.next().unwrap().outcome, GroupOutcome::Failed { error } if error.code == CONFLICT && error.message.contains("capacity"))
    );
    let view = client
        .get_group(&GroupParams { id: group.id })
        .await
        .unwrap();
    assert_eq!(view.members[0].project.desired_state, DesiredState::Running);
    assert_eq!(
        view.members[0].scheduler_desired_state,
        Some(DesiredState::Running)
    );
    assert_eq!(view.members[1].project.desired_state, DesiredState::Stopped);
}

#[tokio::test]
async fn group_launch_rolls_back_all_intents_when_a_late_write_fails() {
    let h = Harness::new();
    let first = h.app("first", "graceful");
    let second = h.app("second", "graceful");
    let service = h.service();
    // Observe installed plans over IPC without starting guardians.
    let (mut client, _task) = Harness::client(&service).await;
    let first = h
        .register(&mut client, &first, 1, CrashRestartPolicy::Never)
        .await;
    let second = h
        .register(&mut client, &second, 1, CrashRestartPolicy::Never)
        .await;
    for id in [first, second] {
        client
            .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
            .await
            .unwrap();
    }
    let group = group_members(&mut client, &h.root, &[first, second]).await;
    let key = Uuid::new_v4();
    let launch = GroupLaunchParams {
        id: group.id,
        expected_revision: group.revision,
        launches: [first, second]
            .into_iter()
            .map(|id| GroupProjectLaunch {
                id,
                php: h.php.clone(),
                allow_horizon: false,
            })
            .collect(),
        idempotency_key: Some(key),
    };
    let sql = |statement: &str| {
        let output = Command::new("/usr/bin/sqlite3")
            .arg(&h.db)
            .arg(statement)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    // Each trigger confirms the earlier writes are visible inside the transaction
    // before aborting the late scheduler write or the receipt write.
    for trigger in [
        format!(
            "CREATE TRIGGER test_late_write BEFORE INSERT ON scheduler_intents
                 WHEN NEW.project_id = '{}' AND
                     (SELECT count(*) FROM project_intents) = 2 AND
                     (SELECT count(*) FROM scheduler_intents) = 1
                 BEGIN SELECT RAISE(ABORT, 'test scheduler intent failure'); END;",
            second.0
        ),
        format!(
            "CREATE TRIGGER test_late_write BEFORE INSERT ON mutation_receipts
                 WHEN NEW.id = '{key}' AND
                     (SELECT count(*) FROM project_intents) = 2 AND
                     (SELECT count(*) FROM scheduler_intents) = 2
                 BEGIN SELECT RAISE(ABORT, 'test receipt failure'); END;"
        ),
    ] {
        sql(&trigger);
        let failed = client.start_group(&launch).await;
        assert!(matches!(failed, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
        let store = Store::open(&h.db).unwrap();
        for id in [first, second] {
            assert_eq!(store.project_intent(id).unwrap(), ProjectIntent::Stopped);
            assert_eq!(
                store.scheduler_intent(id).unwrap(),
                SchedulerIntent::Stopped
            );
            let params = ProjectParams { id };
            assert!(client.list_processes(&params).await.unwrap().is_empty());
            assert!(
                client
                    .scheduler_status(&params)
                    .await
                    .unwrap()
                    .process
                    .is_none()
            );
        }
        sql("DROP TRIGGER test_late_write;");
    }
    let committed = client.start_group(&launch).await.unwrap();
    assert_eq!(committed.operation_id, key);
    assert_eq!(committed.result.projects.len(), 2);
    assert!(committed.result.projects.iter().all(|entry| matches!(
        entry.outcome,
        GroupOutcome::Accepted {
            workers: true,
            scheduler: true
        }
    )));
    let store = Store::open(&h.db).unwrap();
    for id in [first, second] {
        assert_eq!(
            store.project_intent(id).unwrap().desired(),
            DesiredState::Running
        );
        assert_eq!(
            store.scheduler_intent(id).unwrap().desired(),
            DesiredState::Running
        );
    }
    assert_eq!(client.start_group(&launch).await.unwrap().operation_id, key);
}

#[tokio::test]
async fn metrics_follow_owned_worker_and_scheduler_generations_and_page_history() {
    let h = Harness::new();
    let app = h.app("metrics", "graceful");
    let (mut daemon, mut client) = h.daemon().await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    let params = ProjectParams { id };
    let from = roost_core::unix_timestamp();
    client.start_project(&h.launch(id)).await.unwrap();
    client
        .configure_scheduler(&scheduler_config(id, CrashRestartPolicy::Never))
        .await
        .unwrap();
    client
        .start_scheduler(&scheduler_launch(&h, id))
        .await
        .unwrap();
    let deadline = Instant::now() + TOTAL_WAIT;
    let previous = loop {
        let current = client.process_metrics(&params).await.unwrap();
        let processes = current["processes"].as_array().unwrap();
        if processes.len() == 2
            && processes.iter().all(|process| {
                process["cpu_percent"]["state"] == "fresh"
                    && process["rss_bytes"]["value"]
                        .as_u64()
                        .is_some_and(|rss| rss > 0)
                    && process["uptime_seconds"]["value"].as_u64().is_some()
            })
        {
            break processes
                .iter()
                .find(|p| p["identity"]["kind"] == "worker")
                .unwrap()["identity"]
                .clone();
        }
        assert!(
            Instant::now() < deadline,
            "metrics were not collected: {current}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let summary = client.project_metrics(&params).await.unwrap();
    assert_eq!(summary["workers"]["running"], 1);
    assert_eq!(summary["scheduler"]["running"], 1);
    assert_eq!(summary["lifecycle"]["restarts"], 0);

    let mut history = HistoryParams {
        project_id: id,
        profile_id: None,
        resolution: Resolution::Raw,
        from,
        to: None,
        limit: Some(1),
        cursor: None,
    };
    let first_page = client.process_history(&history).await.unwrap();
    assert_eq!(first_page["has_more"], true);
    history.cursor = first_page["next_cursor"].as_str().map(str::to_owned);
    let second_page = client.process_history(&history).await.unwrap();
    assert_ne!(
        second_page["items"][0]["sequence"],
        first_page["items"][0]["sequence"]
    );
    history.resolution = Resolution::FiveMinutes;
    assert!(
        matches!(client.process_history(&history).await, Err(ClientError::Rpc(error)) if error.data.as_ref().is_some_and(|data| data["kind"] == "cursor_scope_mismatch"))
    );

    client.restart_project(&h.launch(id)).await.unwrap();
    let deadline = Instant::now() + TOTAL_WAIT;
    loop {
        let current = client.process_metrics(&params).await.unwrap();
        if let Some(worker) = current["processes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["identity"]["kind"] == "worker" && p["identity"] != previous)
        {
            assert_eq!(worker["cpu_percent"]["state"], "unknown", "{worker}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not obtain a new generation"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let summary = client.project_metrics(&params).await.unwrap();
    assert_eq!(summary["lifecycle"]["restarts"], 1);
    assert_eq!(summary["lifecycle"]["crashes"], 0);
    terminate(&mut daemon).await;

    let (mut daemon, mut client) = h.daemon().await;
    history.resolution = Resolution::Raw;
    let resumed = client.process_history(&history).await.unwrap();
    assert_eq!(resumed["items"], second_page["items"]);
    assert_eq!(resumed["next_cursor"], second_page["next_cursor"]);
    assert_eq!(
        client.project_metrics(&params).await.unwrap()["lifecycle"]["restarts"],
        1
    );
    terminate(&mut daemon).await;
}
