use std::{ffi::OsString, io};

use roost_ipc::{
    Client, ClientError,
    metrics::{HistoryParams, Resolution},
    workers::ProjectParams,
};
use serde_json::Value;
use uuid::Uuid;

use crate::discovery::{parse_id, utf8};

pub enum Command {
    Current(ProjectParams),
    History(HistoryParams),
    Summary(ProjectParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, target, action, id, flags @ ..] = args else {
            return Ok(None);
        };
        if noun != "metrics" {
            return Ok(None);
        }
        let id = parse_id(id)?;
        match (target.to_str(), action.to_str(), flags) {
            (Some("process"), Some("current"), []) => Ok(Some(Self::Current(ProjectParams { id }))),
            (Some("project"), Some("summary"), []) => Ok(Some(Self::Summary(ProjectParams { id }))),
            (Some("process"), Some("history"), flags) => {
                let mut from = None;
                let mut params = HistoryParams {
                    project_id: id,
                    profile_id: None,
                    resolution: Resolution::Raw,
                    from: 0,
                    to: None,
                    limit: None,
                    cursor: None,
                };
                let mut pairs = flags.chunks_exact(2);
                for pair in &mut pairs {
                    let flag = utf8(&pair[0])?;
                    let value = utf8(&pair[1])?;
                    match flag {
                        "--from" => from = Some(number(value)?),
                        "--to" => params.to = Some(number(value)?),
                        "--limit" => params.limit = Some(number(value)?),
                        "--cursor" => params.cursor = Some(value.to_owned()),
                        "--profile" => {
                            params.profile_id = Some(
                                Uuid::parse_str(value)
                                    .map_err(|_| io::Error::other("profile ID must be a UUID"))?,
                            )
                        }
                        "--resolution" => {
                            params.resolution = match value {
                                "raw" => Resolution::Raw,
                                "5m" => Resolution::FiveMinutes,
                                _ => return Err(io::Error::other("resolution must be raw or 5m")),
                            }
                        }
                        _ => return Err(io::Error::other("unknown metrics history option")),
                    }
                }
                if !pairs.remainder().is_empty() {
                    return Err(io::Error::other("each history option requires a value"));
                }
                params.from =
                    from.ok_or_else(|| io::Error::other("history requires --from UNIX_SECONDS"))?;
                Ok(Some(Self::History(params)))
            }
            _ => Ok(None),
        }
    }

    pub async fn execute(self, client: &mut Client) -> Result<Value, ClientError> {
        match self {
            Self::Current(params) => client.process_metrics(&params).await,
            Self::History(params) => client.process_history(&params).await,
            Self::Summary(params) => client.project_metrics(&params).await,
        }
    }
}

fn number<T: std::str::FromStr>(value: &str) -> io::Result<T> {
    value
        .parse()
        .map_err(|_| io::Error::other("expected an unsigned integer"))
}
