use std::{collections::HashMap, ffi::OsString, io};

use roost_core::GroupId;
use roost_ipc::{
    Client, ClientError,
    groups::{
        CreateGroupParams, DeleteGroupParams, GroupLaunchParams, GroupMembershipParams,
        GroupOptions, GroupOutcome, GroupParams, GroupProjectLaunch, GroupStopParams,
        UpdateGroupParams,
    },
};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    discovery::{PhpSelection, parse_id, parse_selection, utf8},
    watch::revision,
};

pub enum Command {
    List,
    Get(GroupParams),
    Create(CreateGroupParams),
    Update(UpdateGroupParams),
    Delete(DeleteGroupParams),
    Membership(GroupMembershipParams, bool),
    Launch {
        id: GroupId,
        selection: PhpSelection,
        restart: bool,
        allow_horizon: bool,
    },
    Stop(GroupStopParams),
}

impl Command {
    pub fn parse(args: &[OsString]) -> io::Result<Option<Self>> {
        let [noun, action, rest @ ..] = args else {
            return Ok(None);
        };
        if noun != "group" {
            return Ok(None);
        }
        let key = Some(Uuid::new_v4());
        let command = match (action.to_str(), rest) {
            (Some("list"), []) => Self::List,
            (Some("get"), [id]) => Self::Get(GroupParams { id: group_id(id)? }),
            (Some("create"), [name, options @ ..]) => Self::Create(CreateGroupParams {
                name: utf8(name)?.into(),
                options: parse_options(options)?,
                idempotency_key: key,
            }),
            (Some("update"), [id, name, flag, rev, options @ ..])
                if flag == "--revision" && !options.is_empty() =>
            {
                Self::Update(UpdateGroupParams {
                    id: group_id(id)?,
                    expected_revision: revision(rev)?,
                    name: utf8(name)?.into(),
                    options: parse_options(options)?,
                    idempotency_key: key,
                })
            }
            (Some("delete"), [id, flag, rev]) if flag == "--revision" => {
                Self::Delete(DeleteGroupParams {
                    id: group_id(id)?,
                    expected_revision: revision(rev)?,
                    idempotency_key: key,
                })
            }
            (Some("add-project" | "remove-project"), [id, project, flag, rev])
                if flag == "--revision" =>
            {
                Self::Membership(
                    GroupMembershipParams {
                        id: group_id(id)?,
                        project_id: parse_id(project)?,
                        expected_revision: revision(rev)?,
                        idempotency_key: key,
                    },
                    action == "add-project",
                )
            }
            (Some("stop"), [id, options @ ..]) => {
                let expected_revision = match options {
                    [] => None,
                    [flag, rev] if flag == "--revision" => Some(revision(rev)?),
                    _ => return Err(io::Error::other("expected optional --revision REV")),
                };
                Self::Stop(GroupStopParams {
                    id: group_id(id)?,
                    expected_revision,
                    idempotency_key: key,
                })
            }
            (Some("start" | "restart"), [id, options @ ..]) => {
                let id = group_id(id)?;
                let (allow_horizon, options) = match options.split_last() {
                    Some((flag, rest)) if flag == "--allow-horizon" => (true, rest),
                    _ => (false, options),
                };
                Self::Launch {
                    id,
                    selection: parse_selection(options)?,
                    restart: action == "restart",
                    allow_horizon,
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(command))
    }

    pub async fn execute(
        self,
        client: &mut Client,
    ) -> Result<(Value, bool), Box<dyn std::error::Error>> {
        let result = match self {
            Self::List => serde_json::to_value(client.list_groups().await?)?,
            Self::Get(params) => serde_json::to_value(client.get_group(&params).await?)?,
            Self::Create(params) => serde_json::to_value(client.create_group(&params).await?)?,
            Self::Update(params) => serde_json::to_value(client.update_group(&params).await?)?,
            Self::Delete(params) => serde_json::to_value(client.delete_group(&params).await?)?,
            Self::Membership(params, add) => serde_json::to_value(if add {
                client.add_project_to_group(&params).await?
            } else {
                client.remove_project_from_group(&params).await?
            })?,
            Self::Stop(params) => serde_json::to_value(client.stop_group(&params).await?)?,
            Self::Launch {
                id,
                selection,
                restart,
                allow_horizon,
            } => {
                return launch(client, id, selection, restart, allow_horizon).await;
            }
        };
        Ok((result, false))
    }
}

async fn launch(
    client: &mut Client,
    id: GroupId,
    selection: PhpSelection,
    restart: bool,
    allow_horizon: bool,
) -> Result<(Value, bool), Box<dyn std::error::Error>> {
    let details = client.get_group(&GroupParams { id }).await?;
    let mut launches = Vec::new();
    let mut php_errors = HashMap::new();
    for member in details.members {
        let params = selection.for_project(member.project.id);
        let php = match params.php {
            Some(path) => path,
            None => match client.resolve_php(&params).await {
                Ok(result) => result.path,
                Err(ClientError::Rpc(error)) => {
                    php_errors.insert(params.id, error);
                    continue;
                }
                Err(error) => return Err(error.into()),
            },
        };
        launches.push(GroupProjectLaunch {
            id: params.id,
            php,
            allow_horizon,
            fence: None,
        });
    }
    let params = GroupLaunchParams {
        id,
        expected_revision: details.group.revision,
        launches,
        idempotency_key: Some(Uuid::new_v4()),
    };
    let mut result = if restart {
        client.restart_group(&params).await?
    } else {
        client.start_group(&params).await?
    };
    let mut failed = false;
    for entry in &mut result.result.projects {
        if let GroupOutcome::Failed { error } = &mut entry.outcome {
            failed = true;
            if let Some(php_error) = php_errors.remove(&entry.id) {
                *error = php_error;
            }
        }
    }
    Ok((serde_json::to_value(result)?, failed))
}

fn group_id(value: &OsString) -> io::Result<GroupId> {
    Uuid::parse_str(utf8(value)?)
        .map(GroupId)
        .map_err(|_| io::Error::other("group ID must be a UUID"))
}

fn parse_options(args: &[OsString]) -> io::Result<GroupOptions> {
    let mut options: GroupOptions = match args {
        [] => GroupOptions::default(),
        [flag, json] if flag == "--options-json" => serde_json::from_str(utf8(json)?)
            .map_err(|_| io::Error::other("invalid group options JSON"))?,
        _ => return Err(io::Error::other("expected --options-json JSON")),
    };
    if let Some(root) = &mut options.root_path {
        *root = std::path::absolute(&*root)?;
    }
    Ok(options)
}
