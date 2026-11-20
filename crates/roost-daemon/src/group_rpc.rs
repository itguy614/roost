mod lifecycle;

use roost_core::{ConfigOwner, Group, GroupId, Project, Revision, unix_timestamp};
use roost_ipc::{
    Mutation, RpcError,
    groups::{
        CreateGroupParams, DeleteGroupParams, GroupDetails, GroupLaunchParams, GroupMember,
        GroupMembershipParams, GroupOptions, GroupParams, GroupStopParams, UpdateGroupParams,
    },
};
use roost_store::{Error as StoreError, Store};
use serde_json::Value;

use crate::{
    DaemonService, Empty, decode, encode, internal_error, request_key, rpc, store_error,
    supervisor::Error,
    worker_rpc::{worker_error, worker_mutation},
};

pub(super) const MAX_SHARED_WATCHES: usize = 16;

pub(super) enum GroupCall {
    List,
    Get(GroupParams),
    Create(CreateGroupParams),
    Update(UpdateGroupParams),
    Delete(DeleteGroupParams),
    Membership(GroupMembershipParams, bool),
    Launch(GroupLaunchParams, bool),
    Stop(GroupStopParams),
}

impl GroupCall {
    pub(super) fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "group.list" => {
                decode::<Empty>(params)?;
                Ok(Self::List)
            }
            "group.get" => Ok(Self::Get(decode(params)?)),
            "group.create" => Ok(Self::Create(decode(params)?)),
            "group.update" => Ok(Self::Update(decode(params)?)),
            "group.delete" => Ok(Self::Delete(decode(params)?)),
            "group.addProject" => Ok(Self::Membership(decode(params)?, true)),
            "group.removeProject" => Ok(Self::Membership(decode(params)?, false)),
            "group.start" => Ok(Self::Launch(decode(params)?, false)),
            "group.restart" => Ok(Self::Launch(decode(params)?, true)),
            "group.stop" => Ok(Self::Stop(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub(super) fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        let store = Store::open(&service.db).map_err(store_error)?;
        let mut supervisor = service.workers.lock().map_err(|_| internal_error())?;
        if !matches!(self, Self::List | Self::Get(_)) {
            supervisor.validate_mutation().map_err(worker_error)?;
        }
        match self {
            Self::List => encode(store.list_groups().map_err(store_error)?),
            Self::Get(params) => encode(
                details(&store, group(&store, params.id).map_err(worker_error)?)
                    .map_err(store_error)?,
            ),
            Self::Create(params) => {
                let key = request_key("group.create", &params)?;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        let group = Group::new(params.name).map_err(StoreError::from)?;
                        let group = configure(&store, group, params.options)?;
                        store.create_group(&group)?;
                        Ok(group)
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Update(params) => {
                let key = request_key("group.update", &params)?;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        let mut group = at_revision(&store, params.id, params.expected_revision)?;
                        group.name = params.name;
                        let group = configure(&store, group, params.options)?;
                        Ok(store.update_group(group)?)
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Delete(params) => {
                let key = request_key("group.delete", &params)?;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        at_revision(&store, params.id, params.expected_revision)?;
                        store.delete_group(params.id)?;
                        Ok(GroupParams { id: params.id })
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Membership(params, add) => {
                let method = if add {
                    "group.addProject"
                } else {
                    "group.removeProject"
                };
                let key = request_key(method, &params)?;
                let (operation_id, result) =
                    worker_mutation(&store, params.idempotency_key, &key, || {
                        let group = at_revision(&store, params.id, params.expected_revision)?;
                        let project = store
                            .get_project(params.project_id)?
                            .ok_or(StoreError::NotFound)?;
                        membership(&store, group, project, add)
                    })?;
                encode(Mutation {
                    operation_id,
                    result,
                })
            }
            Self::Launch(params, restart) => {
                lifecycle::launch(&mut supervisor, &store, params, restart)
            }
            Self::Stop(params) => lifecycle::stop(&mut supervisor, &store, params),
        }
    }
}

fn group(store: &Store, id: GroupId) -> Result<Group, Error> {
    store
        .get_group(id)?
        .ok_or_else(|| StoreError::NotFound.into())
}

fn at_revision(store: &Store, id: GroupId, revision: Revision) -> Result<Group, Error> {
    let group = group(store, id)?;
    if group.revision != revision {
        return Err(StoreError::RevisionConflict.into());
    }
    Ok(group)
}

fn details(store: &Store, group: Group) -> Result<GroupDetails, StoreError> {
    let members = store
        .group_projects(group.id)?
        .into_iter()
        .map(|mut project| {
            project.desired_state = store.project_intent(project.id)?.desired();
            let scheduler_desired_state = if store.project_scheduler(project.id)?.is_some() {
                Some(store.scheduler_intent(project.id)?.desired())
            } else {
                None
            };
            Ok(GroupMember {
                project,
                scheduler_desired_state,
            })
        })
        .collect::<Result<_, StoreError>>()?;
    Ok(GroupDetails { group, members })
}

fn membership(
    store: &Store,
    mut group: Group,
    mut project: Project,
    add: bool,
) -> Result<GroupDetails, Error> {
    if project.config_owner == ConfigOwner::File {
        return Err(Error::Conflict("project group membership is file-owned"));
    }
    if add {
        match project.group_id {
            Some(id) if id == group.id => return Ok(details(store, group)?),
            Some(_) => {
                return Err(Error::Conflict(
                    "the project already belongs to another group",
                ));
            }
            None => project.group_id = Some(group.id),
        }
    } else {
        if project.group_id != Some(group.id) {
            return Err(Error::Conflict("the project does not belong to this group"));
        }
        if group
            .shared_paths
            .iter()
            .any(|mapping| mapping.projects.contains(&project.id))
        {
            return Err(Error::Conflict(
                "remove the project from shared watch targets first",
            ));
        }
        project.group_id = None;
    }
    store.update_project(project)?;
    group.updated_at = unix_timestamp().max(group.updated_at);
    let group = store.update_group(group)?;
    Ok(details(store, group)?)
}

fn configure(store: &Store, mut group: Group, options: GroupOptions) -> Result<Group, Error> {
    group.root_path = options
        .root_path
        .map(|root| {
            if !root.is_absolute() {
                return Err(Error::Invalid("group root must be an absolute directory"));
            }
            root.canonicalize()
                .ok()
                .filter(|path| path.is_dir())
                .ok_or(Error::Invalid("group root must be an existing directory"))
        })
        .transpose()?;
    group.watch_rules = options.watch_rules;
    group.shared_paths = options.shared_paths;
    group.updated_at = unix_timestamp().max(group.updated_at);
    group.validate().map_err(StoreError::from)?;
    roost_watch::Rules::compile(&group.watch_rules)
        .map_err(|_| Error::Invalid("watch rules must be valid project-relative globs"))?;
    if group.shared_paths.is_empty() {
        return Ok(group);
    }
    let root = group.root_path.as_ref().expect("validated group root");
    let projects = store.list_projects()?;
    let mut accepted_paths = Vec::new();
    for other in store
        .list_groups()?
        .into_iter()
        .filter(|other| other.id != group.id)
    {
        if let Some(root) = &other.root_path {
            accepted_paths.extend(
                other
                    .shared_paths
                    .iter()
                    .map(|mapping| root.join(&mapping.path)),
            );
        }
    }
    if accepted_paths.len() + group.shared_paths.len() > MAX_SHARED_WATCHES {
        return Err(Error::Conflict(
            "daemon shared watch capacity is 16 directories",
        ));
    }
    for mapping in &group.shared_paths {
        if roost_watch::excluded_by_default(&mapping.path) {
            return Err(Error::Invalid("shared watch path is excluded by default"));
        }
        let path = mapping.directory(root).map_err(StoreError::from)?;
        if projects.iter().any(|project| project.path == path) {
            return Err(Error::Conflict(
                "a shared directory cannot be a registered project root",
            ));
        }
        if accepted_paths
            .iter()
            .any(|other| path.starts_with(other) || other.starts_with(&path))
        {
            return Err(Error::Conflict("shared watch directories cannot overlap"));
        }
        if !mapping.projects.iter().all(|&id| {
            projects.iter().any(|project| {
                project.id == id
                    && project.group_id == Some(group.id)
                    && project.path.starts_with(root)
            })
        }) {
            return Err(Error::Conflict(
                "shared watch targets must be group members under the group root",
            ));
        }
        // Later mappings in this group must also be disjoint from this one.
        accepted_paths.push(path);
    }
    Ok(group)
}
