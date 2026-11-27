use std::{io, path::Path, time::Duration};

use roost_core::{DesiredState, Project, ProjectId};
use roost_ipc::{Client, ClientError, VERSION_MISMATCH, workers::ProjectParams};
use serde::{Deserialize, Serialize};

use crate::BridgeError;

const PAGE_SIZE: usize = 8;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Serialize)]
pub(crate) struct Dashboard {
    daemon: roost_ipc::Status,
    directory: Vec<ProjectLink>,
    projects: Vec<ProjectRow>,
    selected: Option<ProjectRow>,
    page: usize,
    total: usize,
    pages: usize,
}

#[derive(Serialize)]
struct ProjectLink {
    id: ProjectId,
    name: String,
}

#[derive(Clone, Serialize)]
struct ProjectRow {
    id: ProjectId,
    name: String,
    path: String,
    desired_state: DesiredState,
    observed: Observed,
}

#[derive(Clone, Serialize)]
#[serde(tag = "state", content = "summary", rename_all = "snake_case")]
enum Observed {
    Available(ProjectSummary),
    Unsupported,
    Unavailable,
}

#[derive(Clone, Deserialize, Serialize)]
struct ProjectSummary {
    workers: Counts,
    scheduler: Counts,
}

#[derive(Clone, Deserialize, Serialize)]
struct Counts {
    desired: u64,
    running: u64,
    unresolved: u64,
}

pub(crate) async fn read(
    state_dir: &Path,
    page: u32,
    selected_id: Option<ProjectId>,
) -> Result<Dashboard, BridgeError> {
    tokio::time::timeout(SNAPSHOT_TIMEOUT, fetch(state_dir, page, selected_id))
        .await
        .map_err(|_| BridgeError::Timeout)?
}

async fn fetch(
    state_dir: &Path,
    page: u32,
    selected_id: Option<ProjectId>,
) -> Result<Dashboard, BridgeError> {
    let mut client = Client::connect(state_dir).await.map_err(|error| {
        if matches!(
            &error,
            ClientError::Io(error)
                if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused)
        ) {
            BridgeError::Offline
        } else {
            BridgeError::from(error)
        }
    })?;
    if !client.supports("daemon.status") || !client.supports("project.list") {
        return Err(BridgeError::Unsupported);
    }
    let daemon = client.status().await?;
    let projects = client.list_projects().await?;
    let total = projects.len();
    let pages = total.div_ceil(PAGE_SIZE).max(1);
    let page = (page as usize).min(pages - 1);
    let directory = projects
        .iter()
        .map(|project| ProjectLink {
            id: project.id,
            name: project.name.clone(),
        })
        .collect();
    let mut rows = Vec::with_capacity(PAGE_SIZE);
    for project in projects.iter().skip(page * PAGE_SIZE).take(PAGE_SIZE) {
        rows.push(read_row(&mut client, project).await?);
    }
    let selected = if let Some(id) = selected_id {
        if let Some(row) = rows.iter().find(|row| row.id == id) {
            Some(row.clone())
        } else if let Some(project) = projects.iter().find(|project| project.id == id) {
            Some(read_row(&mut client, project).await?)
        } else {
            None
        }
    } else {
        None
    };
    Ok(Dashboard {
        daemon,
        directory,
        projects: rows,
        selected,
        page,
        total,
        pages,
    })
}

async fn read_row(client: &mut Client, project: &Project) -> Result<ProjectRow, BridgeError> {
    let id = project.id;
    let observed = if client.supports("metrics.project.summary") {
        match client.project_metrics(&ProjectParams { id }).await {
            Ok(data) => Observed::Available(
                serde_json::from_value(data).map_err(|_| BridgeError::InvalidResponse)?,
            ),
            Err(ClientError::Rpc(_)) => Observed::Unavailable,
            Err(error) => return Err(error.into()),
        }
    } else {
        Observed::Unsupported
    };
    Ok(ProjectRow {
        id,
        name: project.name.clone(),
        path: project.path.to_string_lossy().into_owned(),
        desired_state: project.desired_state,
        observed,
    })
}

impl From<ClientError> for BridgeError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Rpc(ref error) if error.code == VERSION_MISMATCH => Self::Incompatible,
            ClientError::Timeout(_) => Self::Timeout,
            ClientError::Io(ref error) if error.kind() == io::ErrorKind::PermissionDenied => {
                Self::AccessDenied
            }
            ClientError::Io(_) | ClientError::Frame(roost_ipc::FrameError::Io(_)) => {
                Self::Disconnected
            }
            ClientError::Frame(_) | ClientError::Json(_) | ClientError::Response => {
                Self::InvalidResponse
            }
            ClientError::Rpc(_) | ClientError::RequestTooLarge => Self::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use roost_ipc::{read_frame, write_frame};
    use serde_json::{Value, json};
    use tokio::{io::BufReader, net::UnixListener};

    use super::*;

    fn private_listener() -> (tempfile::TempDir, UnixListener) {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let private = roost_ipc::endpoint::private_directory(root.path(), false).unwrap();
        let socket = private.join(roost_ipc::endpoint::SOCKET_NAME);
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        (root, listener)
    }

    async fn fixture(reply: impl Fn(&str) -> Value + Send + 'static) -> tempfile::TempDir {
        let (root, listener) = private_listener();
        tokio::spawn(async move {
            let mut client = BufReader::new(listener.accept().await.unwrap().0);
            while let Ok(Some(frame)) = read_frame(&mut client, 65_536).await {
                let request: Value = serde_json::from_slice(&frame).unwrap();
                let mut response = reply(request["method"].as_str().unwrap());
                response["jsonrpc"] = json!("2.0");
                response["id"] = request["id"].clone();
                if write_frame(client.get_mut(), &serde_json::to_vec(&response).unwrap())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        root
    }

    #[tokio::test]
    async fn negotiates_and_reports_a_missing_optional_capability_without_querying_it() {
        let root = fixture(|method| match method {
            "daemon.hello" => json!({"result":{"protocol_version":1,"capabilities":["daemon.status","project.list"]}}),
            "daemon.status" => json!({"result":{"pid":12,"uptime_seconds":1,"protocol_version":1}}),
            "project.list" => json!({"result":[
                {"id":"37b85742-357b-43ea-bf6f-708068293ac4","name":"Fixture","path":"/tmp/fixture",
                 "provider":"herd","desired_state":"stopped","autostart":false,"watch_enabled":false,
                 "watch_rules":{"include":[],"exclude":[]},"config_owner":"local","revision":1}
            ]}),
            _ => panic!("unadvertised request {method}"),
        }).await;
        let data = serde_json::to_value(read(root.path(), 99, None).await.unwrap()).unwrap();
        assert_eq!(data["page"], 0);
        assert_eq!(data["projects"][0]["observed"]["state"], "unsupported");
        assert_eq!(data["projects"][0]["desired_state"], "stopped");
    }

    #[tokio::test]
    async fn stable_directory_and_selected_project_are_independent_of_page() {
        let root = fixture(|method| match method {
            "daemon.hello" => json!({"result":{"protocol_version":1,"capabilities":["daemon.status","project.list","metrics.project.summary"]}}),
            "daemon.status" => json!({"result":{"pid":12,"uptime_seconds":1,"protocol_version":1}}),
            "project.list" => json!({"result":(1..=17).map(|n| json!({
                "id":format!("37b85742-357b-43ea-bf6f-{n:012}"), "name":format!("Project {n}"), "path":"/tmp/project",
                "provider":"herd","desired_state":"running","autostart":false,"watch_enabled":false,
                "watch_rules":{"include":[],"exclude":[]},"config_owner":"local","revision":1
            })).collect::<Vec<_>>() }),
            "metrics.project.summary" => json!({"result":{
                "workers":{"running":1,"desired":2,"unresolved":0},
                "scheduler":{"running":0,"desired":0,"unresolved":0}
            }}),
            _ => panic!("unexpected request {method}"),
        }).await;
        let selected: ProjectId =
            serde_json::from_value(json!("37b85742-357b-43ea-bf6f-000000000017")).unwrap();
        let result = read(root.path(), 1, Some(selected)).await.unwrap();
        assert_eq!(result.page, 1);
        assert_eq!(result.pages, 3);
        assert_eq!(result.directory.len(), 17);
        assert_eq!(result.projects.len(), 8);
        assert_eq!(result.projects[0].name, "Project 9");
        let chosen = result.selected.unwrap();
        assert_eq!(chosen.id, selected);
        assert!(
            matches!(chosen.observed, Observed::Available(summary) if summary.workers.running == 1 && summary.workers.desired == 2)
        );
    }

    #[tokio::test]
    async fn missing_socket_and_protocol_mismatch_are_distinct() {
        let absent = tempfile::tempdir().unwrap();
        assert!(matches!(
            read(&absent.path().join("missing"), 0, None).await,
            Err(BridgeError::Offline)
        ));
        let incompatible = fixture(|method| {
            assert_eq!(method, "daemon.hello");
            json!({"error":{"code":-32001,"message":"unsupported","data":{"supported":[2]}}})
        })
        .await;
        assert!(matches!(
            read(incompatible.path(), 0, None).await,
            Err(BridgeError::Incompatible)
        ));
    }

    #[tokio::test]
    async fn a_stalled_daemon_cannot_hold_the_dashboard_past_its_deadline() {
        let (root, listener) = private_listener();
        let hold = tokio::spawn(async move {
            let _stream = listener.accept().await.unwrap().0;
            std::future::pending::<()>().await;
        });
        assert!(matches!(
            read(root.path(), 0, None).await,
            Err(BridgeError::Timeout)
        ));
        hold.abort();
    }
}
