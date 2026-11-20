#![cfg(target_os = "macos")]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use roost_core::{
    CrashRestartPolicy, DesiredState, ProjectId,
    worker_runtime::{
        DaemonStartup, LaunchFailure, LaunchIntent, ObservedState, ProjectIntent, StopCause,
        WorkerLeaseState, WorkerStatus,
    },
};
use roost_daemon::DaemonService;
use roost_ipc::{
    AddParams, CONFLICT, Client, ClientError, RemoveParams,
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
    store.reserve_worker(&lease).unwrap();
    store.cancel_unclaimed_workers().unwrap();
    assert!(store.claim_worker(lease.clone()).is_err());
    store.reserve_worker(&lease).unwrap();
    store.claim_worker(lease.clone()).unwrap();
    assert!(store.claim_worker(lease.clone()).is_err());
    store.cancel_unclaimed_workers().unwrap();
    assert!(matches!(
        store.worker_lease(lease.id).unwrap().unwrap().state,
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
    assert_eq!(store.project_worker_leases(id).unwrap().len(), 1);

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
    let report = serde_json::to_string(&reports[0]).unwrap();
    assert!(report.contains("RuntimeException: queue connection unavailable"));
    assert!(report.contains("#0 src/Queue.php(20): connect()"));
    assert!(report.contains("line redacted"));
    assert!(!report.contains("secret-fixture-output"));
}
