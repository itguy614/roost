#![cfg(target_os = "macos")]

use std::{fs, path::PathBuf, process::Command, sync::Arc, time::Duration};

use roost_core::{
    PhpProvider,
    discovery::{PhpSource, SuggestionSource},
};
use roost_daemon::DaemonService;
use roost_ipc::{
    AddParams, Client, ClientError, DISCOVERY_BUSY, DISCOVERY_OUTPUT_LIMIT, DISCOVERY_TIMEOUT,
    DISCOVERY_UNAVAILABLE, INVALID_PARAMS,
    discovery::{ResolvePhpParams, SuggestQueueParams},
    workers::{CreateWorkerParams, ProjectParams, WorkerOptions},
};
use roost_store::Store;
use tokio::net::UnixStream;
use uuid::Uuid;

struct Harness {
    root: PathBuf,
    php: PathBuf,
    service: Arc<DaemonService>,
}

impl Harness {
    fn new() -> Self {
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("roost-discovery-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let db = root.join("roost.sqlite3");
        Store::initialize(&db).unwrap();
        let php = root.join("php");
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        assert!(
            Command::new(rustc)
                .args(["--edition=2024", "tests/fixtures/discovery_php.rs", "-o"])
                .arg(&php)
                .status()
                .unwrap()
                .success()
        );
        let service = Arc::new(
            DaemonService::new(db, PathBuf::from(env!("CARGO_BIN_EXE_roost-daemon"))).unwrap(),
        );
        Self { root, php, service }
    }

    async fn client(&self) -> Client {
        let (client, server) = UnixStream::pair().unwrap();
        let service = self.service.clone();
        tokio::spawn(async move { service.serve_client(server).await });
        Client::negotiate(client).await.unwrap()
    }

    async fn project(&self, client: &mut Client, name: &str, mode: &str) -> ResolvePhpParams {
        let app = self.root.join(name);
        for directory in ["vendor", "bootstrap", "app"] {
            fs::create_dir_all(app.join(directory)).unwrap();
        }
        fs::write(app.join("artisan"), mode).unwrap();
        fs::write(app.join("vendor/autoload.php"), "").unwrap();
        fs::write(app.join("bootstrap/app.php"), "").unwrap();
        fs::write(
            app.join("vendor/ignored.php"),
            "<?php Queue::pushOn('vendor-ignored', $job);",
        )
        .unwrap();
        fs::write(
            app.join("app/Jobs.php"),
            "<?php Queue::pushOn('low', $job); $job->onQueue($tenant);",
        )
        .unwrap();
        let id = client
            .add_project(&AddParams {
                path: app,
                idempotency_key: None,
            })
            .await
            .unwrap()
            .result
            .id;
        ResolvePhpParams {
            id,
            provider: None,
            php: Some(self.php.clone()),
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn discovers_allowlisted_connections_and_suggestions_through_ipc() {
    let h = Harness::new();
    let mut client = h.client().await;
    let request = h.project(&mut client, "ok", "ok").await;
    let params = ProjectParams { id: request.id };
    assert_eq!(
        client.list_providers().await.unwrap(),
        [PhpProvider::Herd, PhpProvider::System]
    );
    let detection = client.detect_providers(&params).await.unwrap();
    assert_eq!(detection.preferred, PhpProvider::Herd);
    assert_eq!(
        client.resolve_php(&request).await.unwrap().source,
        PhpSource::Override
    );
    assert!(client.queue_connections(&params).await.unwrap().is_none());

    let discovered = client.discover_queues(&request).await.unwrap();
    assert_eq!(discovered.default.as_deref(), Some("database"));
    assert_eq!(
        discovered
            .connections
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["database", "email_redis"]
    );
    assert!(
        !serde_json::to_string(&discovered)
            .unwrap()
            .contains("private-fixture-value")
    );
    assert_eq!(
        client
            .queue_connections(&params)
            .await
            .unwrap()
            .unwrap()
            .connections,
        discovered.connections
    );
    assert!(client.list_profiles(&params).await.unwrap().is_empty());

    client
        .create_worker(&CreateWorkerParams {
            project_id: request.id,
            name: "Fixture".into(),
            connection: "database".into(),
            options: WorkerOptions {
                queues: vec!["high".into(), "low".into()],
                ..WorkerOptions::default()
            },
            idempotency_key: None,
        })
        .await
        .unwrap();
    let suggestions = client
        .suggest_queues(&SuggestQueueParams {
            id: request.id,
            explicit: vec!["explicit".into()],
        })
        .await
        .unwrap();
    assert!(!suggestions.complete && suggestions.dynamic_references);
    assert_eq!(suggestions.configured_profiles[0].queues, ["high", "low"]);
    assert_eq!(
        suggestions
            .suggestions
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["explicit", "high", "low"]
    );
    assert_eq!(
        suggestions.suggestions[2].sources,
        [
            SuggestionSource::StaticSource,
            SuggestionSource::WorkerProfile
        ]
    );

    let conflicting = ResolvePhpParams {
        provider: Some(PhpProvider::System),
        ..request
    };
    let error = client.resolve_php(&conflicting).await.unwrap_err();
    assert!(matches!(error, ClientError::Rpc(error) if error.code == INVALID_PARAMS));
}

#[tokio::test]
async fn rejects_oversize_failed_and_invalid_output_without_reflecting_it() {
    let h = Harness::new();
    let mut client = h.client().await;
    for (mode, code) in [
        ("oversize", DISCOVERY_OUTPUT_LIMIT),
        ("fail", DISCOVERY_UNAVAILABLE),
        ("invalid", DISCOVERY_UNAVAILABLE),
    ] {
        let request = h.project(&mut client, mode, mode).await;
        let error = client.discover_queues(&request).await.unwrap_err();
        assert!(!error.to_string().contains("private-fixture-value"));
        assert!(matches!(error, ClientError::Rpc(error) if error.code == code));
        assert!(
            client
                .queue_connections(&ProjectParams { id: request.id })
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn limits_slow_discovery_without_blocking_daemon_status() {
    let h = Harness::new();
    let mut first = h.client().await;
    let mut second = h.client().await;
    let mut third = h.client().await;
    let request = h.project(&mut third, "slow", "hang").await;
    let active = async {
        tokio::join!(
            first.discover_queues(&request),
            second.discover_queues(&request)
        )
    };
    let message = async {
        // Take retries up to one second to avoid assuming scheduling order.
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(third.resolve_php(&request).await, Err(ClientError::Rpc(error)) if error.code == DISCOVERY_BUSY) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            third.status().await.unwrap()
        }).await.unwrap();
        assert!(result.pid > 0);
    };
    let ((one, two), ()) = tokio::join!(active, message);
    for result in [one, two] {
        assert!(matches!(result, Err(ClientError::Rpc(error)) if error.code == DISCOVERY_TIMEOUT));
    }
    let descendants = fs::read_dir(h.root.join("slow"))
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().is_some_and(|extension| extension == "pid")).then(|| {
                fs::read_to_string(path)
                    .unwrap()
                    .parse::<libc::pid_t>()
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(descendants.len(), 2);
    tokio::time::timeout(Duration::from_secs(2), async {
        // Signal zero only checks presence. Roost is responsible for cleanup.
        while descendants
            .iter()
            .any(|&pid| unsafe { libc::kill(pid, 0) == 0 })
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("discovery left a live subprocess in its process group");
}
