use std::{ffi::OsString, io};

use crate::discovery::{absolute_utf8, parse_id, selection_params, utf8};
use roost_ipc::{
    Client,
    discovery::ResolvePhpParams,
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
    Launch {
        php: ResolvePhpParams,
        restart: bool,
        allow_horizon: bool,
    },
    Stop(StopParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, verb, rest @ ..] = args else {
            return Ok(None);
        };
        let command = match (noun.to_str(), verb.to_str(), rest) {
            (Some("project"), Some("start" | "restart"), [id, options @ ..]) => {
                let (allow_horizon, options) = match options.split_last() {
                    Some((flag, rest)) if flag == "--allow-horizon" => (true, rest),
                    _ => (false, options),
                };
                let id = parse_id(id)?;
                let php = match options {
                    [path] if !path.to_string_lossy().starts_with("--") => ResolvePhpParams {
                        id,
                        provider: None,
                        php: Some(absolute_utf8(path)?),
                    },
                    options => selection_params(id, options)?,
                };
                Self::Launch {
                    php,
                    restart: verb == "restart",
                    allow_horizon,
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
            Self::Launch {
                php,
                restart,
                allow_horizon,
            } => {
                // Explicit paths retain the original IPC path. Default/provider
                // resolution is a separate request, before taking lifecycle locks.
                let path = match &php.php {
                    Some(path) => path.clone(),
                    None => client.resolve_php(&php).await?.path,
                };
                let params = LaunchParams {
                    id: php.id,
                    php: path,
                    allow_horizon,
                    idempotency_key: Some(Uuid::new_v4()),
                };
                let intent = if restart {
                    client.restart_project(&params).await?
                } else {
                    client.start_project(&params).await?
                };
                serde_json::to_value(intent)?
            }
            Self::Stop(params) => serde_json::to_value(client.stop_project(&params).await?)?,
        })
    }
}
