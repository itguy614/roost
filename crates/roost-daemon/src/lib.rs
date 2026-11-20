mod backoff;
pub mod guardian;
mod supervisor;
mod worker_rpc;

use std::{
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
            net::UnixListener as StdListener,
        },
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use roost_core::{Project, worker_runtime::DaemonStartup};
use roost_ipc::{
    self as rpc, AddParams, GetParams, Hello, HelloParams, Mutation, RemoveParams, Removed,
    RpcError, Status, endpoint,
};
use roost_store::{Error as StoreError, Store};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use supervisor::Supervisor;
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    sync::Semaphore,
    time::timeout,
};
use worker_rpc::{WorkerCall, worker_error};

const MAX_CLIENTS: usize = 32;

pub struct Daemon {
    listener: UnixListener,
    _socket: SocketGuard,
    _lock: File,
    service: Arc<DaemonService>,
}

impl Daemon {
    pub async fn bind(state_dir: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if endpoint::effective_uid() == 0 {
            return Err(io::Error::other("run Roost as a regular user").into());
        }
        let dir = endpoint::private_directory(state_dir, true)?;
        let lock = lock_instance(&dir.join("daemon.lock"))?;
        let path = dir.join(endpoint::SOCKET_NAME);
        match endpoint::owned_socket(&path) {
            Ok(_) => match timeout(rpc::IO_TIMEOUT, UnixStream::connect(&path)).await? {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "IPC socket is already in use",
                    )
                    .into());
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(&path)?
                }
                Err(error) => return Err(error.into()),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let db = dir.join("roost.sqlite3");
        drop(private_file(&db)?);
        Store::initialize(&db)?;
        let listener = StdListener::bind(&path)?;
        let metadata = endpoint::owned_socket(&path)?;
        let socket = SocketGuard {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        fs::set_permissions(&socket.path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let listener = UnixListener::from_std(listener)?;
        let service = Arc::new(DaemonService::new(db, std::env::current_exe()?)?);
        Ok(Self {
            listener,
            _socket: socket,
            _lock: lock,
            service,
        })
    }

    pub async fn run(&self) -> io::Result<()> {
        tokio::select! {
            result = self.accept() => result,
            () = self.service.poll_workers() => unreachable!("worker polling does not terminate"),
        }
    }

    pub async fn shutdown(&self, mark_clean: bool) {
        let service = self.service.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let Ok(mut supervisor) = service.workers.lock() else {
                return;
            };
            supervisor.shutdown();
        })
        .await;
        let deadline = Instant::now() + Duration::from_secs(65);
        while Instant::now() < deadline {
            let service = self.service.clone();
            let running = tokio::task::spawn_blocking(move || {
                let Ok(mut supervisor) = service.workers.lock() else {
                    return false;
                };
                let _ = supervisor.tick();
                if supervisor.has_children() {
                    return true;
                }
                if mark_clean && !Store::open(&service.db).and_then(|store| store.finish_daemon_run()).unwrap_or(false) {
                    tracing::warn!("daemon shutdown left unresolved worker ownership or could not record completion");
                }
                false
            })
            .await
            .unwrap_or(false);
            if !running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn accept(&self) -> io::Result<()> {
        let capacity = Arc::new(Semaphore::new(MAX_CLIENTS));
        loop {
            let permit = capacity
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore remains open");
            let (stream, _) = match self.listener.accept().await {
                Ok(connection) => connection,
                Err(error) if error.kind() == io::ErrorKind::ConnectionAborted => continue,
                Err(error) => return Err(error),
            };
            let service = self.service.clone();
            tokio::spawn(async move {
                let _permit = permit;
                service.serve_client(stream).await;
            });
        }
    }
}

fn private_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.uid() != endpoint::effective_uid() || meta.mode() & 0o777 != 0o600 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "daemon state files must be private regular files",
        ));
    }
    Ok(file)
}

fn lock_instance(path: &Path) -> io::Result<File> {
    let file = private_file(path)?;
    // SAFETY: the descriptor stays open via File for the lifetime of the daemon.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == -1 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::WouldBlock {
            Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "Roost daemon already owns this state directory",
            ))
        } else {
            Err(error)
        };
    }
    Ok(file)
}

struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(meta) = endpoint::owned_socket(&self.path)
            && (meta.dev(), meta.ino()) == (self.device, self.inode)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Daemon application service shared by the listener and connected transports.
pub struct DaemonService {
    db: PathBuf,
    started: Instant,
    startup: DaemonStartup,
    workers: Mutex<Supervisor>,
}

impl DaemonService {
    /// Open the service after taking an exclusive state-directory lock and migrating SQLite.
    pub fn new(db: PathBuf, binary: PathBuf) -> Result<Self, StoreError> {
        let store = Store::open(&db)?;
        store.cancel_unclaimed_workers()?;
        store.recover_completed_workers()?;
        let intents = store.autostart_intents()?;
        let startup = store.begin_daemon_run()?;
        Ok(Self {
            workers: Mutex::new(Supervisor::new(binary, db.clone(), intents)),
            db,
            started: Instant::now(),
            startup,
        })
    }

    pub async fn serve_client(self: &Arc<Self>, stream: UnixStream) {
        if let Err(error) = endpoint::verify_peer(&stream) {
            tracing::warn!(kind = ?error.kind(), "IPC peer rejected");
            return;
        }
        serve(stream, self.clone()).await;
    }

    pub async fn poll_workers(self: &Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut failed = false;
        loop {
            interval.tick().await;
            let service = self.clone();
            let result = tokio::task::spawn_blocking(move || {
                service
                    .workers
                    .lock()
                    .is_ok_and(|mut workers| workers.tick().is_ok())
            })
            .await
            .unwrap_or(false);
            if failed == result {
                if result {
                    tracing::info!("worker polling recovered");
                } else {
                    tracing::error!("worker polling could not complete");
                }
                failed = !result;
            }
        }
    }
}

async fn serve(stream: UnixStream, service: Arc<DaemonService>) {
    let mut reader = BufReader::new(stream);
    let mut negotiated = false;
    loop {
        let frame = timeout(
            rpc::IO_TIMEOUT,
            rpc::read_frame(&mut reader, rpc::MAX_REQUEST),
        )
        .await;
        let response = match frame {
            Ok(Ok(Some(frame))) => handle(&frame, &service, &mut negotiated).await,
            Ok(Err(rpc::FrameError::TooLarge)) => {
                let error = RpcError::new(rpc::FRAME_TOO_LARGE, "request exceeds 65536 bytes");
                let _ = send(&mut reader, error_response(Value::Null, error)).await;
                return;
            }
            Ok(Err(rpc::FrameError::Truncated)) => {
                let error = RpcError::new(rpc::PARSE_ERROR, "request is missing a newline");
                let _ = send(&mut reader, error_response(Value::Null, error)).await;
                return;
            }
            _ => return,
        };
        if let Some(response) = response
            && send(&mut reader, response).await.is_err()
        {
            return;
        }
    }
}

async fn send(reader: &mut BufReader<UnixStream>, response: Value) -> io::Result<()> {
    let bytes = serde_json::to_vec(&response)?;
    let bytes = if bytes.len() > rpc::MAX_RESPONSE {
        serde_json::to_vec(&error_response(
            response["id"].clone(),
            RpcError::new(rpc::FRAME_TOO_LARGE, "response exceeds 4194304 bytes"),
        ))?
    } else {
        bytes
    };
    timeout(rpc::IO_TIMEOUT, rpc::write_frame(reader.get_mut(), &bytes)).await?
}

async fn handle(
    frame: &[u8],
    service: &Arc<DaemonService>,
    negotiated: &mut bool,
) -> Option<Value> {
    let request: Value = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(_) => {
            return Some(error_response(
                Value::Null,
                RpcError::new(rpc::PARSE_ERROR, "invalid JSON"),
            ));
        }
    };
    let id = request.get("id");
    let method = request.get("method").and_then(Value::as_str);
    let valid_id = id.is_none_or(|id| id.is_null() || id.is_string() || id.is_number());
    let Some(method) = method.filter(|_| request["jsonrpc"] == "2.0" && valid_id) else {
        return Some(error_response(
            Value::Null,
            RpcError::new(rpc::INVALID_REQUEST, "expected one JSON-RPC 2.0 request"),
        ));
    };
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    let result = dispatch(service, negotiated, method, params).await;
    id.map(|id| match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(error) => error_response(id.clone(), error),
    })
}

async fn dispatch(
    service: &Arc<DaemonService>,
    negotiated: &mut bool,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    if method == "daemon.hello" {
        *negotiated = false;
        let params: HelloParams = decode(params)?;
        if params.protocol_version != rpc::VERSION {
            let mut error = RpcError::new(rpc::VERSION_MISMATCH, "unsupported protocol version");
            error.data = Some(json!({"supported": [rpc::VERSION]}));
            return Err(error);
        }
        *negotiated = true;
        return encode(Hello {
            protocol_version: rpc::VERSION,
            capabilities: rpc::METHODS
                .iter()
                .map(|method| (*method).to_owned())
                .collect(),
        });
    }
    if !*negotiated {
        return Err(RpcError::new(
            rpc::HANDSHAKE_REQUIRED,
            "call daemon.hello first",
        ));
    }
    if method == "daemon.status" {
        decode::<Empty>(params)?;
        return encode(Status {
            startup: Some(service.startup),
            pid: std::process::id(),
            uptime_seconds: service.started.elapsed().as_secs(),
            protocol_version: rpc::VERSION,
        });
    }
    let service = service.clone();
    let methods = [
        "project.list",
        "project.get",
        "project.add",
        "project.remove",
    ];
    let job = if methods.contains(&method) {
        let call = ProjectCall::decode(method, params)?;
        tokio::task::spawn_blocking(move || call.execute(&service))
    } else {
        let call = WorkerCall::decode(method, params)?;
        tokio::task::spawn_blocking(move || call.execute(&service))
    };
    job.await.map_err(|_| {
        tracing::error!("IPC database task did not complete");
        internal_error()
    })?
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

enum ProjectCall {
    List,
    Get(GetParams),
    Add(AddParams),
    Remove(RemoveParams),
}

impl ProjectCall {
    fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "project.list" => {
                decode::<Empty>(params)?;
                Ok(Self::List)
            }
            "project.get" => Ok(Self::Get(decode(params)?)),
            "project.add" => Ok(Self::Add(decode(params)?)),
            "project.remove" => Ok(Self::Remove(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let store = Store::open(&service.db).map_err(store_error)?;
        match self {
            Self::List => {
                let _workers = service.workers.lock().map_err(|_| internal_error())?;
                let mut projects = store.list_projects().map_err(store_error)?;
                for project in &mut projects {
                    project.desired_state = store
                        .project_intent(project.id)
                        .map_err(store_error)?
                        .desired();
                }
                encode(projects)
            }
            Self::Get(params) => {
                let _workers = service.workers.lock().map_err(|_| internal_error())?;
                let mut project = store
                    .get_project(params.id)
                    .map_err(store_error)?
                    .ok_or_else(|| store_error(StoreError::NotFound))?;
                project.desired_state = store
                    .project_intent(project.id)
                    .map_err(store_error)?
                    .desired();
                encode(project)
            }
            Self::Add(params) => {
                if !params.path.is_absolute() {
                    return Err(RpcError::new(
                        rpc::INVALID_PARAMS,
                        "project path must be absolute",
                    ));
                }
                let request = request_key("project.add", &params)?;
                let (operation_id, result) = store
                    .mutate(params.idempotency_key, &request, |store| {
                        let project = Project::register(&params.path)?;
                        store.create_project(&project)?;
                        Ok(project)
                    })
                    .map_err(store_error)?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Remove(params) => {
                let request = request_key("project.remove", &params)?;
                let mut workers = service.workers.lock().map_err(|_| internal_error())?;
                workers
                    .clean_previous(&store, params.id)
                    .map_err(worker_error)?;
                let mut apply = false;
                let (operation_id, result) = store
                    .try_mutate::<_, supervisor::Error>(params.idempotency_key, &request, |store| {
                        workers.validate_config_change(store, params.id)?;
                        store.delete_project(params.id)?;
                        apply = true;
                        Ok(Removed { id: params.id })
                    })
                    .map_err(worker_error)?;
                if apply {
                    workers.remove(params.id);
                }
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
        }
    }
}

fn request_key(method: &str, params: &impl Serialize) -> Result<String, RpcError> {
    serde_json::to_string(&(method, params)).map_err(|_| internal_error())
}

fn decode<T: DeserializeOwned>(params: Value) -> Result<T, RpcError> {
    if !params.is_object() {
        return Err(RpcError::new(
            rpc::INVALID_PARAMS,
            "params must be an object",
        ));
    }
    serde_json::from_value(params)
        .map_err(|_| RpcError::new(rpc::INVALID_PARAMS, "invalid fields for this method"))
}

fn encode(result: impl Serialize) -> Result<Value, RpcError> {
    serde_json::to_value(result).map_err(|_| internal_error())
}

fn error_response(id: Value, error: RpcError) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": error})
}

fn internal_error() -> RpcError {
    RpcError::new(rpc::INTERNAL_ERROR, "daemon could not complete the request")
}

fn store_error(error: StoreError) -> RpcError {
    match error {
        StoreError::Validation(error) => RpcError::new(rpc::INVALID_PARAMS, error.to_string()),
        StoreError::NotFound => RpcError::new(rpc::NOT_FOUND, "project not found"),
        StoreError::Duplicate => {
            RpcError::new(rpc::CONFLICT, "project directory is already registered")
        }
        StoreError::IdempotencyConflict | StoreError::RevisionConflict | StoreError::Reference => {
            RpcError::new(rpc::CONFLICT, error.to_string())
        }
        StoreError::Sql(ref error) if error.code & 0xff == 5 || error.code & 0xff == 6 => {
            RpcError::new(rpc::INTERNAL_ERROR, "database is busy; retry the request")
        }
        _ => {
            tracing::error!("database operation failed");
            internal_error()
        }
    }
}

#[cfg(test)]
mod tests;
