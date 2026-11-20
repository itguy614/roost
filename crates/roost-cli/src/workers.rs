use std::{ffi::OsString, io, path::PathBuf};

use roost_core::ProjectId;
use roost_ipc::{
    Client,
    workers::{
        AutostartParams, CreateWorkerParams, LaunchParams, ProjectParams, StopParams, WorkerOptions,
    },
};
use serde_json::Value;
use uuid::Uuid;

pub enum Command {
    Create(CreateWorkerParams),
    Profiles(ProjectParams),
    Processes(ProjectParams),
    Logs(ProjectParams),
    Crashes(ProjectParams),
    Autostart(AutostartParams),
    Start(LaunchParams),
    Stop(StopParams),
    Restart(LaunchParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, verb, rest @ ..] = args else {
            return Ok(None);
        };
        let command = match (noun.to_str(), verb.to_str(), rest) {
            (Some("project"), Some("start" | "restart"), [id, php, flags @ ..]) => {
                let allow_horizon = match flags {
                    [] => false,
                    [flag] if flag == "--allow-horizon" => true,
                    _ => return Err(io::Error::other("expected optional --allow-horizon")),
                };
                let params = LaunchParams {
                    id: parse_id(id)?,
                    php: absolute_utf8(php)?,
                    allow_horizon,
                    idempotency_key: Some(Uuid::new_v4()),
                };
                if verb == "start" {
                    Self::Start(params)
                } else {
                    Self::Restart(params)
                }
            }
            (Some("project"), Some("autostart"), [id, value]) => {
                let enabled = match value.to_str() {
                    Some("on") => true,
                    Some("off") => false,
                    _ => return Err(io::Error::other("autostart expects on or off")),
                };
                Self::Autostart(AutostartParams {
                    id: parse_id(id)?,
                    enabled,
                    idempotency_key: Some(Uuid::new_v4()),
                })
            }
            (Some("worker"), Some("crashes"), [id]) => {
                Self::Crashes(ProjectParams { id: parse_id(id)? })
            }
            (Some("project"), Some("stop"), [id]) => Self::Stop(StopParams {
                id: parse_id(id)?,
                idempotency_key: Some(Uuid::new_v4()),
            }),
            (Some("process"), Some("list"), [id]) => {
                Self::Processes(ProjectParams { id: parse_id(id)? })
            }
            (Some("log"), Some("tail"), [id]) => Self::Logs(ProjectParams { id: parse_id(id)? }),
            (Some("worker"), Some("profile"), [action, id]) if action == "list" => {
                Self::Profiles(ProjectParams { id: parse_id(id)? })
            }
            (Some("worker"), Some("profile"), [action, id, name, connection, arguments @ ..])
                if action == "create" =>
            {
                let mut options = WorkerOptions::default();
                match arguments {
                    [flag, json] if flag == "--options-json" => {
                        options = serde_json::from_str(utf8(json)?)
                            .map_err(|_| io::Error::other("invalid worker options JSON"))?;
                    }
                    queues => {
                        options.queues = queues
                            .iter()
                            .map(|queue| utf8(queue).map(str::to_owned))
                            .collect::<io::Result<_>>()?;
                    }
                }
                Self::Create(CreateWorkerParams {
                    project_id: parse_id(id)?,
                    name: utf8(name)?.to_owned(),
                    connection: utf8(connection)?.to_owned(),
                    options,
                    idempotency_key: Some(Uuid::new_v4()),
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(command))
    }

    pub async fn execute(self, client: &mut Client) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(match self {
            Self::Create(params) => serde_json::to_value(client.create_worker(&params).await?)?,
            Self::Profiles(params) => serde_json::to_value(client.list_profiles(&params).await?)?,
            Self::Processes(params) => serde_json::to_value(client.list_processes(&params).await?)?,
            Self::Autostart(params) => serde_json::to_value(client.set_autostart(&params).await?)?,
            Self::Crashes(params) => serde_json::to_value(client.recent_crashes(&params).await?)?,
            Self::Logs(params) => serde_json::to_value(client.tail_logs(&params).await?)?,
            Self::Start(params) => serde_json::to_value(client.start_project(&params).await?)?,
            Self::Stop(params) => serde_json::to_value(client.stop_project(&params).await?)?,
            Self::Restart(params) => serde_json::to_value(client.restart_project(&params).await?)?,
        })
    }
}

fn utf8(argument: &OsString) -> io::Result<&str> {
    argument
        .to_str()
        .ok_or_else(|| io::Error::other("argument must be valid UTF-8"))
}

fn absolute_utf8(argument: &OsString) -> io::Result<PathBuf> {
    let path = std::path::absolute(argument)?;
    if path.to_str().is_none() {
        return Err(io::Error::other("PHP path must be valid UTF-8"));
    }
    Ok(path)
}

fn parse_id(argument: &OsString) -> io::Result<ProjectId> {
    Uuid::parse_str(utf8(argument)?)
        .map(ProjectId)
        .map_err(|_| io::Error::other("project ID must be a UUID"))
}
