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
    worker_runtime::{ObservedState, WorkerLeaseState, WorkerStatus},
};
use roost_daemon::DaemonService;
use roost_ipc::{
    AddParams, CONFLICT, Client, ClientError, RemoveParams,
    workers::{CreateWorkerParams, LaunchParams, ProjectParams, StopParams, WorkerOptions},
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
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("roost-m2-{}", Uuid::new_v4()));
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
        if output.len() >= 2 {
            let encoded = serde_json::to_string(&output).unwrap();
            assert!(!encoded.contains("secret-fixture-output"));
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

#[tokio::test]
async fn sigkill_daemon_owner_cleans_worker_group_before_relaunch() {
    // Run the same service behind a connected socket in a real subprocess.
    // The parent owns this subprocess and can safely simulate a daemon crash.
    if let Some(db) = std::env::var_os("ROOST_TEST_OWNER_DB").map(PathBuf::from) {
        let id = ProjectId(
            Uuid::parse_str(&std::env::var("ROOST_TEST_OWNER_PROJECT").unwrap()).unwrap(),
        );
        let root = db.parent().unwrap();
        let service = Arc::new(DaemonService::new(db.clone(), PathBuf::from(GUARDIAN)).unwrap());
        let _poll = Harness::poll(&service);
        let (mut client, _) = Harness::client(&service).await;
        client
            .start_project(&LaunchParams {
                id,
                php: root.join("php"),
                allow_horizon: false,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let running = until_status(&mut client, id, all_in(ObservedState::Running, 1)).await;
        let worker = running[0].identity.unwrap().pid;
        eventually(|| root.join("app/descendant.pid").exists()).await;
        fs::write(root.join("owner.ready.tmp"), worker.to_string()).unwrap();
        fs::rename(root.join("owner.ready.tmp"), root.join("owner.ready")).unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        panic!("test owner was not interrupted");
    }

    let h = Harness::new();
    let app = h.app("app", "ignore");
    let service = h.service();
    let (mut client, serve) = Harness::client(&service).await;
    let id = h
        .register(&mut client, &app, 1, CrashRestartPolicy::Never)
        .await;
    drop(client);
    serve.await.unwrap();
    drop(service);

    let mut bystander = OwnedChild(Command::new("/bin/sleep").arg("30").spawn().unwrap());
    let mut owner = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sigkill_daemon_owner_cleans_worker_group_before_relaunch",
            ])
            .env("ROOST_TEST_OWNER_DB", &h.db)
            .env("ROOST_TEST_OWNER_PROJECT", id.0.to_string())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    eventually(|| h.root.join("owner.ready").exists()).await;
    let worker: u32 = fs::read_to_string(h.root.join("owner.ready"))
        .unwrap()
        .parse()
        .unwrap();
    let descendant: u32 = fs::read_to_string(app.join("descendant.pid"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(exists(worker) && exists(descendant));

    owner.0.kill().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(owner.0.wait().unwrap().signal(), Some(libc::SIGKILL));

    let replacement = h.service();
    let (mut client, _) = Harness::client(&replacement).await;
    let first = client.start_project(&h.launch(id)).await;
    assert!(matches!(first, Err(ClientError::Rpc(error)) if error.code == CONFLICT));
    let store = Store::open(&h.db).unwrap();
    eventually(|| store.project_worker_leases(id).unwrap().iter().any(|lease| {
        matches!(lease.state, WorkerLeaseState::Exited { exit } if exit.forced && exit.requested_stop)
    })).await;
    assert!(!exists(worker));
    // A descendant can briefly remain as a zombie awaiting the reaper.
    eventually(|| !exists(descendant)).await;
    assert!(bystander.0.try_wait().unwrap().is_none());
    assert_eq!(
        client.list_projects().await.unwrap()[0].desired_state,
        DesiredState::Stopped
    );
    client.start_project(&h.launch(id)).await.unwrap();
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
    let (mut client, _) = Harness::client(&service).await;
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
    assert!(!failed[0].recovery_blocked);
    assert!(!app.join("should-not-exist").exists());
    poll.abort();
}
