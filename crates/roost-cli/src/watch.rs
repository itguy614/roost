use std::{ffi::OsString, io, num::NonZeroU32};

use roost_core::{CodeRestartPolicy, Revision, WorkerProfileId};
use roost_ipc::{
    Client,
    watch::{CodeRestartParams, DisableWatchParams, EnableWatchParams},
    workers::ProjectParams,
};
use serde_json::Value;
use uuid::Uuid;

use crate::discovery::{parse_id, utf8};

pub enum Command {
    Status(ProjectParams),
    Enable(EnableWatchParams),
    Disable(DisableWatchParams),
    Policy(CodeRestartParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let command = match args {
            [noun, verb, id] if noun == "watch" && verb == "status" => {
                Self::Status(ProjectParams { id: parse_id(id)? })
            }
            [noun, verb, id, flag, rev, options @ ..]
                if noun == "watch" && verb == "enable" && flag == "--revision" =>
            {
                let rules = match options {
                    [] => None,
                    [flag, json] if flag == "--rules-json" => Some(
                        serde_json::from_str(utf8(json)?)
                            .map_err(|_| io::Error::other("invalid watch rules JSON"))?,
                    ),
                    _ => return Err(io::Error::other("expected optional --rules-json JSON")),
                };
                Self::Enable(EnableWatchParams {
                    id: parse_id(id)?,
                    expected_revision: revision(rev)?,
                    rules,
                    idempotency_key: Some(Uuid::new_v4()),
                })
            }
            [noun, verb, id, flag, rev]
                if noun == "watch" && verb == "disable" && flag == "--revision" =>
            {
                Self::Disable(DisableWatchParams {
                    id: parse_id(id)?,
                    expected_revision: revision(rev)?,
                    idempotency_key: Some(Uuid::new_v4()),
                })
            }
            [noun, kind, verb, id, policy, flag, rev]
                if noun == "worker"
                    && kind == "profile"
                    && verb == "set-code-restart"
                    && flag == "--revision" =>
            {
                let policy = match policy.to_str() {
                    Some("never") => CodeRestartPolicy::Never,
                    Some("on-change") => CodeRestartPolicy::OnChange,
                    _ => {
                        return Err(io::Error::other(
                            "code restart policy must be never or on-change",
                        ));
                    }
                };
                let id = Uuid::parse_str(utf8(id)?)
                    .map_err(|_| io::Error::other("worker profile ID must be a UUID"))?;
                Self::Policy(CodeRestartParams {
                    id: WorkerProfileId(id),
                    expected_revision: revision(rev)?,
                    policy,
                    idempotency_key: Some(Uuid::new_v4()),
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(command))
    }

    pub async fn execute(self, client: &mut Client) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(match self {
            Self::Status(params) => serde_json::to_value(client.watch_status(&params).await?)?,
            Self::Enable(params) => serde_json::to_value(client.enable_watch(&params).await?)?,
            Self::Disable(params) => serde_json::to_value(client.disable_watch(&params).await?)?,
            Self::Policy(params) => serde_json::to_value(client.set_code_restart(&params).await?)?,
        })
    }
}

fn revision(value: &OsString) -> io::Result<Revision> {
    utf8(value)?
        .parse::<NonZeroU32>()
        .map(|revision| Revision(revision.get()))
        .map_err(|_| io::Error::other("revision must be a positive integer"))
}
