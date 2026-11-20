use std::{ffi::OsString, io, path::PathBuf};

use roost_core::{PhpProvider, ProjectId};
use roost_ipc::{
    Client,
    discovery::{ResolvePhpParams, SuggestQueueParams},
    workers::ProjectParams,
};
use serde_json::Value;
use uuid::Uuid;

pub enum Command {
    Providers,
    Detect(ProjectParams),
    Resolve(ResolvePhpParams),
    Connections(ProjectParams),
    Discover(ResolvePhpParams),
    Suggestions(SuggestQueueParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, verb, rest @ ..] = args else {
            return Ok(None);
        };
        let command = match (noun.to_str(), verb.to_str(), rest) {
            (Some("provider"), Some("list"), []) => Self::Providers,
            (Some("provider"), Some("detect"), [id]) => {
                Self::Detect(ProjectParams { id: parse_id(id)? })
            }
            (Some("provider"), Some("resolve-php"), [id, selection @ ..]) => {
                Self::Resolve(selection_params(parse_id(id)?, selection)?)
            }
            (Some("queue"), Some("connections"), [id]) => {
                Self::Connections(ProjectParams { id: parse_id(id)? })
            }
            (Some("queue"), Some("discover"), [id, selection @ ..]) => {
                Self::Discover(selection_params(parse_id(id)?, selection)?)
            }
            (Some("queue"), Some("suggestions"), [id, names @ ..]) => {
                Self::Suggestions(SuggestQueueParams {
                    id: parse_id(id)?,
                    explicit: names
                        .iter()
                        .map(|name| utf8(name).map(str::to_owned))
                        .collect::<io::Result<_>>()?,
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(command))
    }

    pub async fn execute(self, client: &mut Client) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(match self {
            Self::Providers => serde_json::to_value(client.list_providers().await?)?,
            Self::Detect(params) => serde_json::to_value(client.detect_providers(&params).await?)?,
            Self::Resolve(params) => serde_json::to_value(client.resolve_php(&params).await?)?,
            Self::Connections(params) => {
                serde_json::to_value(client.queue_connections(&params).await?)?
            }
            Self::Discover(params) => serde_json::to_value(client.discover_queues(&params).await?)?,
            Self::Suggestions(params) => {
                serde_json::to_value(client.suggest_queues(&params).await?)?
            }
        })
    }
}

pub fn selection_params(id: ProjectId, options: &[OsString]) -> io::Result<ResolvePhpParams> {
    let mut params = ResolvePhpParams {
        id,
        provider: None,
        php: None,
    };
    match options {
        [] => (),
        [flag, path] if flag == "--php" => params.php = Some(absolute_utf8(path)?),
        [flag, provider] if flag == "--provider" => {
            params.provider = Some(match provider.to_str() {
                Some("herd") => PhpProvider::Herd,
                Some("system") => PhpProvider::System,
                _ => return Err(io::Error::other("provider must be herd or system")),
            });
        }
        _ => {
            return Err(io::Error::other(
                "expected one of --php PATH or --provider herd|system",
            ));
        }
    }
    Ok(params)
}

pub fn utf8(argument: &OsString) -> io::Result<&str> {
    argument
        .to_str()
        .ok_or_else(|| io::Error::other("argument must be valid UTF-8"))
}

pub fn absolute_utf8(argument: &OsString) -> io::Result<PathBuf> {
    let path = std::path::absolute(argument)?;
    if path.to_str().is_none() {
        return Err(io::Error::other("PHP path must be valid UTF-8"));
    }
    Ok(path)
}

pub fn parse_id(argument: &OsString) -> io::Result<ProjectId> {
    Uuid::parse_str(utf8(argument)?)
        .map(ProjectId)
        .map_err(|_| io::Error::other("project ID must be a UUID"))
}
