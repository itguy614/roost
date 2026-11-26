use std::{ffi::OsString, io};

use roost_core::{Revision, WorkerProfileId};
use roost_ipc::{
    Client,
    alerts::{AlertListParams, ThresholdProfileParams, ThresholdUpdateParams},
};
use serde_json::Value;
use uuid::Uuid;

use crate::discovery::{parse_id, utf8};

pub(super) enum Command {
    List(AlertListParams),
    Get(ThresholdProfileParams),
    Update(ThresholdUpdateParams),
}

impl Command {
    pub(super) fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [metrics, category, verb, id, rest @ ..] = args else {
            return Ok(None);
        };
        if metrics != "metrics" {
            return Ok(None);
        }
        let command = match (category.to_str(), verb.to_str()) {
            (Some("alerts"), Some("list")) => {
                let mut cursor = None;
                let mut limit = None;
                let mut options = rest.chunks_exact(2);
                for pair in &mut options {
                    match utf8(&pair[0])? {
                        "--cursor" if cursor.is_none() => cursor = Some(utf8(&pair[1])?.to_owned()),
                        "--limit" if limit.is_none() => limit = Some(number(&pair[1])?),
                        _ => {
                            return Err(io::Error::other("unknown or duplicate alert list option"));
                        }
                    }
                }
                if !options.remainder().is_empty() {
                    return Err(io::Error::other("alert list options require a value"));
                }
                Self::List(AlertListParams {
                    project_id: parse_id(id)?,
                    cursor,
                    limit,
                })
            }
            (Some("thresholds"), Some("get")) if rest.is_empty() => {
                Self::Get(ThresholdProfileParams {
                    profile_id: profile(id)?,
                })
            }
            (Some("thresholds"), Some("update")) => {
                let [revision, number_arg, rules, json, optional @ ..] = rest else {
                    return Err(io::Error::other(
                        "threshold update requires --revision REV --rules-json JSON",
                    ));
                };
                if revision != "--revision" || rules != "--rules-json" {
                    return Err(io::Error::other(
                        "threshold update requires --revision REV --rules-json JSON",
                    ));
                }
                let notifications = match optional {
                    [] => None,
                    [flag, value] if flag == "--notifications" && value == "on" => Some(true),
                    [flag, value] if flag == "--notifications" && value == "off" => Some(false),
                    _ => return Err(io::Error::other("expected --notifications on or off")),
                };
                Self::Update(ThresholdUpdateParams {
                    profile_id: profile(id)?,
                    expected_revision: Revision(number(number_arg)?),
                    rules: serde_json::from_str(utf8(json)?)
                        .map_err(|_| io::Error::other("invalid threshold rules JSON"))?,
                    notifications,
                    idempotency_key: Some(Uuid::new_v4()),
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(command))
    }

    pub(super) async fn execute(
        self,
        client: &mut Client,
    ) -> Result<Value, roost_ipc::ClientError> {
        match self {
            Self::List(params) => client.list_alerts(&params).await,
            Self::Get(params) => client.thresholds(&params).await,
            Self::Update(params) => client.update_thresholds(&params).await,
        }
    }
}

fn profile(value: &OsString) -> io::Result<WorkerProfileId> {
    utf8(value)?
        .parse::<Uuid>()
        .map(WorkerProfileId)
        .map_err(|_| io::Error::other("profile ID must be a UUID"))
}

fn number(value: &OsString) -> io::Result<u32> {
    utf8(value)?
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::other("expected a positive integer"))
}
