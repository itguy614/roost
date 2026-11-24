use std::{ffi::OsString, io};

use roost_ipc::{
    Client, ClientError,
    metrics::Resolution,
    queue_metrics::{ConfigureQueueMetrics, QueueHistoryParams},
    workers::ProjectParams,
};
use serde_json::Value;
use uuid::Uuid;

use crate::discovery::{parse_id, utf8};

pub enum Command {
    Configure(ConfigureQueueMetrics),
    Current(ProjectParams),
    History(QueueHistoryParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, target, action, id, flags @ ..] = args else {
            return Ok(None);
        };
        if noun != "metrics" || target != "queue" {
            return Ok(None);
        }
        let id = parse_id(id)?;
        match (action.to_str(), flags) {
            (Some("enable" | "disable"), []) => Ok(Some(Self::Configure(ConfigureQueueMetrics {
                id,
                enabled: action == "enable",
                idempotency_key: Some(Uuid::new_v4()),
            }))),
            (Some("current"), []) => Ok(Some(Self::Current(ProjectParams { id }))),
            (Some("history"), flags) => {
                let mut from = None;
                let mut params = QueueHistoryParams {
                    project_id: id,
                    connection: None,
                    queue: None,
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
                        "--from" => from = Some(super::metrics::number(value)?),
                        "--to" => params.to = Some(super::metrics::number(value)?),
                        "--limit" => params.limit = Some(super::metrics::number(value)?),
                        "--cursor" => params.cursor = Some(value.into()),
                        "--connection" => params.connection = Some(value.into()),
                        "--queue" => params.queue = Some(value.into()),
                        "--resolution" => params.resolution = super::metrics::resolution(value)?,
                        _ => return Err(io::Error::other("unknown queue history option")),
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
            Self::Configure(params) => client.configure_queue_metrics(&params).await,
            Self::Current(params) => client.queue_metrics(&params).await,
            Self::History(params) => client.queue_history(&params).await,
        }
    }
}
