use std::path::PathBuf;

use roost_core::{
    DesiredState, Group, GroupId, Project, ProjectId, Revision, SharedWatchPath, WatchRules,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Client, ClientError, Mutation, RpcError};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupParams {
    pub id: GroupId,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GroupOptions {
    pub root_path: Option<PathBuf>,
    pub watch_rules: WatchRules,
    pub shared_paths: Vec<SharedWatchPath>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateGroupParams {
    pub name: String,
    #[serde(default)]
    pub options: GroupOptions,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGroupParams {
    pub id: GroupId,
    pub expected_revision: Revision,
    pub name: String,
    pub options: GroupOptions,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteGroupParams {
    pub id: GroupId,
    pub expected_revision: Revision,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupMembershipParams {
    pub id: GroupId,
    pub project_id: ProjectId,
    pub expected_revision: Revision,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupLaunchParams {
    pub id: GroupId,
    pub expected_revision: Revision,
    pub launches: Vec<GroupProjectLaunch>,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupProjectLaunch {
    pub id: ProjectId,
    pub php: PathBuf,
    #[serde(default)]
    pub allow_horizon: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<crate::mutations::LaunchFence>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupStopParams {
    pub id: GroupId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<Revision>,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupDetails {
    pub group: Group,
    pub members: Vec<GroupMember>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupMember {
    pub project: Project,
    pub scheduler_desired_state: Option<DesiredState>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupLifecycle {
    pub id: GroupId,
    pub projects: Vec<GroupProjectResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupProjectResult {
    pub id: ProjectId,
    #[serde(flatten)]
    pub outcome: GroupOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GroupOutcome {
    Accepted { workers: bool, scheduler: bool },
    Unchanged,
    Failed { error: RpcError },
}

impl Client {
    pub async fn list_groups(&mut self) -> Result<Vec<Group>, ClientError> {
        self.request("group.list", &serde_json::json!({})).await
    }

    pub async fn get_group(&mut self, params: &GroupParams) -> Result<GroupDetails, ClientError> {
        self.request("group.get", params).await
    }

    pub async fn create_group(
        &mut self,
        params: &CreateGroupParams,
    ) -> Result<Mutation<Group>, ClientError> {
        self.request("group.create", params).await
    }

    pub async fn update_group(
        &mut self,
        params: &UpdateGroupParams,
    ) -> Result<Mutation<Group>, ClientError> {
        self.request("group.update", params).await
    }

    pub async fn delete_group(
        &mut self,
        params: &DeleteGroupParams,
    ) -> Result<Mutation<GroupParams>, ClientError> {
        self.request("group.delete", params).await
    }

    pub async fn add_project_to_group(
        &mut self,
        params: &GroupMembershipParams,
    ) -> Result<Mutation<GroupDetails>, ClientError> {
        self.request("group.addProject", params).await
    }

    pub async fn remove_project_from_group(
        &mut self,
        params: &GroupMembershipParams,
    ) -> Result<Mutation<GroupDetails>, ClientError> {
        self.request("group.removeProject", params).await
    }

    pub async fn start_group(
        &mut self,
        params: &GroupLaunchParams,
    ) -> Result<Mutation<GroupLifecycle>, ClientError> {
        self.request("group.start", params).await
    }

    pub async fn restart_group(
        &mut self,
        params: &GroupLaunchParams,
    ) -> Result<Mutation<GroupLifecycle>, ClientError> {
        self.request("group.restart", params).await
    }

    pub async fn stop_group(
        &mut self,
        params: &GroupStopParams,
    ) -> Result<Mutation<GroupLifecycle>, ClientError> {
        self.request("group.stop", params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_group_payloads_keep_their_receipt_shape() {
        let id = Uuid::nil();
        let member = json!({"id": id, "php": "/usr/bin/php", "allow_horizon": false});
        let stop = json!({"id": id, "idempotency_key": id});
        let launch: GroupProjectLaunch = serde_json::from_value(member.clone()).unwrap();
        let params: GroupStopParams = serde_json::from_value(stop.clone()).unwrap();

        assert!(launch.fence.is_none());
        assert!(params.expected_revision.is_none());
        assert_eq!(serde_json::to_value(launch).unwrap(), member);
        assert_eq!(serde_json::to_value(params).unwrap(), stop);
    }
}
