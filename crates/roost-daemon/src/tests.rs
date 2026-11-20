use std::os::unix::fs::PermissionsExt;

use roost_ipc::Client;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use super::*;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("roost-protocol-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        Self(fs::canonicalize(root).unwrap())
    }

    fn app(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::create_dir(&path).unwrap();
        fs::write(path.join("artisan"), "<?php").unwrap();
        path
    }

    fn service(&self) -> Arc<DaemonService> {
        let db = self.0.join("test.sqlite3");
        Store::initialize(&db).unwrap();
        Arc::new(DaemonService::new(db, std::env::current_exe().unwrap()).unwrap())
    }
}

fn pair(service: &Arc<DaemonService>) -> UnixStream {
    let (server, client) = UnixStream::pair().unwrap();
    endpoint::verify_peer(&server).unwrap();
    tokio::spawn(serve(server, service.clone()));
    client
}

async fn connect(service: &Arc<DaemonService>) -> Client {
    Client::negotiate(pair(service)).await.unwrap()
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn send(client: &mut BufReader<UnixStream>, request: Value) -> Value {
    rpc::write_frame(client.get_mut(), &serde_json::to_vec(&request).unwrap())
        .await
        .unwrap();
    receive(client).await
}

async fn receive(client: &mut BufReader<UnixStream>) -> Value {
    let frame = timeout(rpc::IO_TIMEOUT, rpc::read_frame(client, rpc::MAX_RESPONSE))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_slice(&frame).unwrap()
}

fn request(id: u32, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn error(response: Value, code: i32, id: Value) {
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], id);
    assert_eq!(response["error"]["code"], code);
    assert!(response["error"]["message"].is_string());
}

#[tokio::test]
async fn negotiates_and_serves_project_lifecycle_across_restart() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let mut first = connect(&service).await;
    let mut second = connect(&service).await;

    let status = first.status().await.unwrap();
    assert_eq!(status.pid, std::process::id());
    assert_eq!(status.protocol_version, rpc::VERSION);
    assert!(first.list_projects().await.unwrap().is_empty());

    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let invalid_encoding = first
        .add_project(&AddParams {
            path: PathBuf::from(OsString::from_vec(vec![b'/', 0xff])),
            idempotency_key: None,
        })
        .await;
    assert!(matches!(invalid_encoding, Err(rpc::ClientError::Json(_))));

    let params = AddParams {
        path: fixture.app("Laravel"),
        idempotency_key: Some(Uuid::new_v4()),
    };
    let result = first.add_project(&params).await.unwrap();
    let project = result.result;
    assert_eq!(
        second.list_projects().await.unwrap(),
        std::slice::from_ref(&project)
    );
    assert_eq!(
        second
            .get_project(&GetParams { id: project.id })
            .await
            .unwrap(),
        project
    );
    let duplicate = second
        .add_project(&AddParams {
            path: params.path.clone(),
            idempotency_key: None,
        })
        .await;
    assert!(matches!(duplicate, Err(rpc::ClientError::Rpc(e)) if e.code == rpc::CONFLICT));
    let invalid = second
        .add_project(&AddParams {
            path: fixture.0.clone(),
            idempotency_key: None,
        })
        .await;
    assert!(matches!(invalid, Err(rpc::ClientError::Rpc(e)) if e.code == rpc::INVALID_PARAMS));
    drop(first);
    drop(second);

    let service = fixture.service();
    let mut client = connect(&service).await;
    let replay = client.add_project(&params).await.unwrap();
    assert_eq!(replay.operation_id, result.operation_id);
    assert_eq!(replay.result, project);

    let conflict = client
        .remove_project(&RemoveParams {
            id: project.id,
            idempotency_key: params.idempotency_key,
        })
        .await;
    assert!(matches!(conflict, Err(rpc::ClientError::Rpc(e)) if e.code == rpc::CONFLICT));
    let remove = RemoveParams {
        id: project.id,
        idempotency_key: Some(Uuid::new_v4()),
    };
    let removed = client.remove_project(&remove).await.unwrap();
    assert_eq!(removed.result.id, project.id);
    assert_eq!(
        client.remove_project(&remove).await.unwrap().operation_id,
        removed.operation_id
    );
    let missing = client.get_project(&GetParams { id: project.id }).await;
    assert!(matches!(missing, Err(rpc::ClientError::Rpc(e)) if e.code == rpc::NOT_FOUND));
    let missing = client
        .remove_project(&RemoveParams {
            idempotency_key: None,
            ..remove
        })
        .await;
    assert!(matches!(missing, Err(rpc::ClientError::Rpc(e)) if e.code == rpc::NOT_FOUND));
    assert!(client.list_projects().await.unwrap().is_empty());
}

#[tokio::test]
async fn validates_protocol_frames_and_keeps_serving_after_bad_input() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let mut raw = BufReader::new(pair(&service));
    error(
        send(&mut raw, request(1, "daemon.status", json!({}))).await,
        rpc::HANDSHAKE_REQUIRED,
        json!(1),
    );
    let mismatch = send(
        &mut raw,
        request(2, "daemon.hello", json!({"protocol_version": 99})),
    )
    .await;
    assert_eq!(mismatch["error"]["data"]["supported"], json!([1]));
    error(mismatch, rpc::VERSION_MISMATCH, json!(2));
    assert_eq!(
        send(
            &mut raw,
            request(3, "daemon.hello", json!({"protocol_version": 1}))
        )
        .await["result"]["capabilities"],
        json!(rpc::METHODS)
    );
    error(
        send(
            &mut raw,
            json!({"jsonrpc": "1.0", "method": "project.list"}),
        )
        .await,
        rpc::INVALID_REQUEST,
        Value::Null,
    );
    error(
        send(&mut raw, request(4, "project.get", json!({"id": "bad"}))).await,
        rpc::INVALID_PARAMS,
        json!(4),
    );
    error(
        send(&mut raw, request(5, "project.list", json!([]))).await,
        rpc::INVALID_PARAMS,
        json!(5),
    );
    error(
        send(&mut raw, request(6, "project.read", json!({}))).await,
        rpc::METHOD_NOT_FOUND,
        json!(6),
    );
    rpc::write_frame(raw.get_mut(), b"{ broken").await.unwrap();
    error(receive(&mut raw).await, rpc::PARSE_ERROR, Value::Null);
    rpc::write_frame(
        raw.get_mut(),
        br#"{"jsonrpc":"2.0","method":"daemon.status"}"#,
    )
    .await
    .unwrap();
    assert!(
        send(&mut raw, request(7, "daemon.status", json!({}))).await["result"]["pid"].is_number()
    );

    let mut oversized = BufReader::new(pair(&service));
    let mut exact =
        serde_json::to_vec(&request(8, "daemon.hello", json!({"protocol_version": 1}))).unwrap();
    exact.resize(rpc::MAX_REQUEST, b' ');
    rpc::write_frame(oversized.get_mut(), &exact).await.unwrap();
    assert_eq!(
        receive(&mut oversized).await["result"]["protocol_version"],
        1
    );
    oversized
        .get_mut()
        .write_all(&vec![b'x'; rpc::MAX_REQUEST + 1])
        .await
        .unwrap();
    error(
        receive(&mut oversized).await,
        rpc::FRAME_TOO_LARGE,
        Value::Null,
    );
    assert!(
        rpc::read_frame(&mut oversized, rpc::MAX_RESPONSE)
            .await
            .unwrap()
            .is_none()
    );

    let mut truncated = BufReader::new(pair(&service));
    truncated.get_mut().write_all(b"{}").await.unwrap();
    truncated.get_mut().shutdown().await.unwrap();
    error(receive(&mut truncated).await, rpc::PARSE_ERROR, Value::Null);
    assert_eq!(
        connect(&service)
            .await
            .status()
            .await
            .unwrap()
            .protocol_version,
        rpc::VERSION
    );
}

#[tokio::test]
async fn concurrent_clients_coalesce_retries_and_preserve_independent_writes() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let paths = (0..8)
        .map(|index| fixture.app(&format!("app-{index}")))
        .collect::<Vec<_>>();
    let key = Uuid::new_v4();
    let mut clients = tokio::task::JoinSet::new();
    for index in 0..16 {
        let service = service.clone();
        let path = paths[index % paths.len()].clone();
        clients.spawn(async move {
            let mut client = connect(&service).await;
            let result = client
                .add_project(&AddParams {
                    path,
                    idempotency_key: (index % 8 == 0).then_some(key),
                })
                .await;
            if index % 8 == 0 {
                let receipt = result.unwrap();
                assert_eq!(receipt.operation_id, key);
                Some(receipt.result.id)
            } else {
                assert!(matches!(
                    result,
                    Ok(_)
                        | Err(rpc::ClientError::Rpc(rpc::RpcError {
                            code: rpc::CONFLICT,
                            ..
                        }))
                ));
                None
            }
        });
    }
    let mut retries = Vec::new();
    while let Some(result) = clients.join_next().await {
        retries.extend(result.unwrap());
    }
    assert_eq!(retries.len(), 2);
    assert_eq!(retries[0], retries[1]);
    let mut client = connect(&service).await;
    assert_eq!(client.list_projects().await.unwrap().len(), paths.len());
}
