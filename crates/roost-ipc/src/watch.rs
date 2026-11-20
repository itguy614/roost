use roost_core::{
    CodeRestartPolicy, Project, ProjectId, Revision, WatchRules, WorkerProfile, WorkerProfileId,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Client, ClientError, Mutation, workers::ProjectParams};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnableWatchParams {
    pub id: ProjectId,
    pub expected_revision: Revision,
    pub rules: Option<WatchRules>,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisableWatchParams {
    pub id: ProjectId,
    pub expected_revision: Revision,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeRestartParams {
    pub id: WorkerProfileId,
    pub expected_revision: Revision,
    pub policy: CodeRestartPolicy,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchState {
    Disabled,
    Initializing,
    Watching,
    Degraded,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WatchStatus {
    pub id: ProjectId,
    pub revision: Revision,
    pub state: WatchState,
    pub rules: WatchRules,
    pub pending: bool,
    pub problem: Option<String>,
}

impl Client {
    pub async fn watch_status(
        &mut self,
        params: &ProjectParams,
    ) -> Result<WatchStatus, ClientError> {
        self.request("watch.status", params).await
    }

    pub async fn enable_watch(
        &mut self,
        params: &EnableWatchParams,
    ) -> Result<Mutation<Project>, ClientError> {
        self.request("watch.enable", params).await
    }

    pub async fn disable_watch(
        &mut self,
        params: &DisableWatchParams,
    ) -> Result<Mutation<Project>, ClientError> {
        self.request("watch.disable", params).await
    }

    pub async fn set_code_restart(
        &mut self,
        params: &CodeRestartParams,
    ) -> Result<Mutation<WorkerProfile>, ClientError> {
        self.request("worker.profile.setCodeRestart", params).await
    }
}
