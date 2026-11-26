use std::{ffi::OsString, io};

use roost_core::Revision;
use roost_ipc::{
    Client,
    discovery::ResolvePhpParams,
    scheduler::{ConfigureSchedulerParams, SchedulerLaunchParams, SchedulerOptions},
    workers::{CrashParams, ProjectParams, StopParams},
};
use serde_json::Value;
use uuid::Uuid;

use crate::discovery::{absolute_utf8, parse_id, selection_params, utf8};

pub enum Command {
    Configure(ConfigureSchedulerParams),
    Status(ProjectParams),
    Launch {
        php: ResolvePhpParams,
        restart: bool,
    },
    Stop(StopParams),
    Logs(ProjectParams),
    Crashes(ProjectParams),
    Crash(CrashParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, verb, id, rest @ ..] = args else {
            return Ok(None);
        };
        if noun != "scheduler" {
            return Ok(None);
        }
        if verb == "crash"
            && id == "get"
            && let [report] = rest
        {
            return Ok(Some(Self::Crash(CrashParams {
                id: utf8(report)?
                    .parse()
                    .map_err(|_| io::Error::other("report ID must be a UUID"))?,
            })));
        }
        let id = parse_id(id)?;
        let params = ProjectParams { id };
        let command = match (verb.to_str(), rest) {
            (Some("status"), []) => Self::Status(params),
            (Some("logs"), []) => Self::Logs(params),
            (Some("crashes"), []) => Self::Crashes(params),
            (Some("stop"), []) => Self::Stop(StopParams {
                id,
                idempotency_key: Some(Uuid::new_v4()),
            }),
            (Some("start" | "restart"), rest) => {
                let php = match rest {
                    [path] if !path.to_string_lossy().starts_with("--") => ResolvePhpParams {
                        id,
                        provider: None,
                        php: Some(absolute_utf8(path)?),
                    },
                    rest => selection_params(id, rest)?,
                };
                Self::Launch {
                    php,
                    restart: verb == "restart",
                }
            }
            (Some("configure"), rest) => {
                let (expected_revision, rest) = match rest {
                    [flag, revision, rest @ ..] if flag == "--revision" => {
                        let revision = utf8(revision)?
                            .parse::<u32>()
                            .map_err(|_| io::Error::other("revision must be a positive integer"))?;
                        if revision == 0 {
                            return Err(io::Error::other("revision must be a positive integer"));
                        }
                        (Some(Revision(revision)), rest)
                    }
                    rest => (None, rest),
                };
                let options = match rest {
                    [] => SchedulerOptions::default(),
                    [flag, json] if flag == "--options-json" => {
                        serde_json::from_str(utf8(json)?)
                            .map_err(|_| io::Error::other("invalid scheduler options JSON"))?
                    }
                    _ => {
                        return Err(io::Error::other(
                            "expected optional --revision REV and --options-json JSON",
                        ));
                    }
                };
                Self::Configure(ConfigureSchedulerParams {
                    id,
                    expected_revision,
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
            Self::Configure(params) => {
                serde_json::to_value(client.configure_scheduler(&params).await?)?
            }
            Self::Status(params) => serde_json::to_value(client.scheduler_status(&params).await?)?,
            Self::Stop(params) => serde_json::to_value(client.stop_scheduler(&params).await?)?,
            Self::Logs(params) => serde_json::to_value(client.scheduler_logs(&params).await?)?,
            Self::Crash(params) => serde_json::to_value(client.scheduler_crash(&params).await?)?,
            Self::Crashes(params) => {
                serde_json::to_value(client.scheduler_crashes(&params).await?)?
            }
            Self::Launch { php, restart } => {
                let path = match &php.php {
                    Some(path) => path.clone(),
                    None => client.resolve_php(&php).await?.path,
                };
                let params = SchedulerLaunchParams {
                    id: php.id,
                    php: path,
                    idempotency_key: Some(Uuid::new_v4()),
                };
                let result = if restart {
                    client.restart_scheduler(&params).await?
                } else {
                    client.start_scheduler(&params).await?
                };
                serde_json::to_value(result)?
            }
        })
    }
}
