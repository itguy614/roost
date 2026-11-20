pub mod discovery;
pub mod endpoint;
pub mod workers;

use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use roost_core::{Project, ProjectId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::timeout,
};
use uuid::Uuid;

pub const VERSION: u32 = 1;
pub const METHODS: &[&str] = &[
    "daemon.hello",
    "daemon.status",
    "provider.list",
    "provider.detect",
    "provider.resolvePhp",
    "queue.connections",
    "queue.discover",
    "queue.suggestions",
    "project.list",
    "project.add",
    "project.get",
    "project.remove",
    "project.start",
    "project.setAutostart",
    "worker.crashes",
    "project.stop",
    "project.restart",
    "worker.profile.create",
    "worker.profile.list",
    "process.list",
    "log.tail",
];
pub const MAX_REQUEST: usize = 64 * 1024;
pub const MAX_RESPONSE: usize = 4 * 1024 * 1024;
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);

pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;
pub const HANDSHAKE_REQUIRED: i32 = -32000;
pub const VERSION_MISMATCH: i32 = -32001;
pub const FRAME_TOO_LARGE: i32 = -32002;
pub const NOT_FOUND: i32 = -32003;
pub const CONFLICT: i32 = -32004;

pub const DISCOVERY_UNAVAILABLE: i32 = -32005;
pub const DISCOVERY_TIMEOUT: i32 = -32006;
pub const DISCOVERY_OUTPUT_LIMIT: i32 = -32007;
pub const DISCOVERY_BUSY: i32 = -32008;

#[derive(Debug, Serialize, Deserialize, thiserror::Error)]
#[error("{message} (RPC {code})")]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloParams {
    pub protocol_version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    #[serde(default)]
    pub startup: Option<roost_core::worker_runtime::DaemonStartup>,
    pub pid: u32,
    pub uptime_seconds: u64,
    pub protocol_version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddParams {
    pub path: PathBuf,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetParams {
    pub id: ProjectId,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveParams {
    pub id: ProjectId,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Mutation<T> {
    pub operation_id: Uuid,
    pub result: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Removed {
    pub id: ProjectId,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("message exceeds the size limit")]
    TooLarge,
    #[error("message ended before a newline")]
    Truncated,
}

pub async fn read_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
    limit: usize,
) -> Result<Option<Vec<u8>>, FrameError> {
    let mut frame = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(FrameError::Truncated)
            };
        }
        let end = chunk.iter().position(|&byte| byte == b'\n');
        let take = end.unwrap_or(chunk.len());
        if take > limit - frame.len() {
            return Err(FrameError::TooLarge);
        }
        frame.extend_from_slice(&chunk[..take]);
        reader.consume(take + usize::from(end.is_some()));
        if end.is_some() {
            return Ok(Some(frame));
        }
    }
}

pub async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), message: &[u8]) -> io::Result<()> {
    writer.write_all(message).await?;
    writer.write_all(b"\n").await
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("IPC connection failed: {0}")]
    Io(#[from] io::Error),
    #[error("IPC deadline elapsed")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("IPC frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("IPC JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("invalid IPC response")]
    Response,
    #[error("IPC request exceeds the size limit")]
    RequestTooLarge,
}

pub struct Client {
    stream: BufReader<UnixStream>,
    next_id: u64,
}

impl Client {
    pub async fn connect(state_dir: &Path) -> Result<Self, ClientError> {
        let dir = endpoint::private_directory(state_dir, false)?;
        let path = dir.join(endpoint::SOCKET_NAME);
        endpoint::verify_socket(&path)?;
        let stream = timeout(IO_TIMEOUT, UnixStream::connect(path)).await??;
        Self::negotiate(stream).await
    }

    /// Authenticate and negotiate an already-connected Unix stream.
    pub async fn negotiate(stream: UnixStream) -> Result<Self, ClientError> {
        endpoint::verify_peer(&stream)?;
        let mut client = Self {
            stream: BufReader::new(stream),
            next_id: 1,
        };
        let hello: Hello = client
            .request(
                "daemon.hello",
                &HelloParams {
                    protocol_version: VERSION,
                },
            )
            .await?;
        if hello.protocol_version != VERSION {
            return Err(ClientError::Response);
        }
        Ok(client)
    }

    pub async fn status(&mut self) -> Result<Status, ClientError> {
        self.request("daemon.status", &json!({})).await
    }

    pub async fn list_projects(&mut self) -> Result<Vec<Project>, ClientError> {
        self.request("project.list", &json!({})).await
    }

    pub async fn add_project(
        &mut self,
        params: &AddParams,
    ) -> Result<Mutation<Project>, ClientError> {
        self.request("project.add", params).await
    }

    pub async fn get_project(&mut self, params: &GetParams) -> Result<Project, ClientError> {
        self.request("project.get", params).await
    }

    pub async fn remove_project(
        &mut self,
        params: &RemoveParams,
    ) -> Result<Mutation<Removed>, ClientError> {
        self.request("project.remove", params).await
    }

    async fn request<R: DeserializeOwned>(
        &mut self,
        method: &str,
        params: &impl Serialize,
    ) -> Result<R, ClientError> {
        #[derive(Serialize)]
        struct Request<'a, P> {
            jsonrpc: &'static str,
            id: u64,
            method: &'a str,
            params: &'a P,
        }

        let id = self.next_id;
        self.next_id += 1;
        let message = serde_json::to_vec(&Request {
            jsonrpc: "2.0",
            id,
            method,
            params,
        })?;
        if message.len() > MAX_REQUEST {
            return Err(ClientError::RequestTooLarge);
        }
        timeout(IO_TIMEOUT, write_frame(self.stream.get_mut(), &message)).await??;
        let frame = timeout(IO_TIMEOUT, read_frame(&mut self.stream, MAX_RESPONSE))
            .await??
            .ok_or(ClientError::Response)?;
        let response: Value = serde_json::from_slice(&frame)?;
        if response["jsonrpc"] != "2.0" || response["id"] != id {
            return Err(ClientError::Response);
        }
        match (response.get("result"), response.get("error")) {
            (Some(result), None) => Ok(serde_json::from_value(result.clone())?),
            (None, Some(error)) => Err(serde_json::from_value::<RpcError>(error.clone())?.into()),
            _ => Err(ClientError::Response),
        }
    }
}

pub fn state_dir() -> io::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "HOME must be set to an absolute path",
            )
        })?;
    if cfg!(target_os = "macos") {
        Ok(home.join("Library/Application Support/Roost"))
    } else {
        Ok(home.join(".roost"))
    }
}
