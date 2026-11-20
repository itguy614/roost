use std::path::PathBuf;

use roost_core::{
    CrashRestartPolicy, DesiredState, ProjectId, WorkerProfile,
    worker_runtime::{OutputSummary, WorkerStatus},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Client, ClientError, Mutation};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectParams {
    pub id: ProjectId,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchParams {
    pub id: ProjectId,
    pub php: PathBuf,
    #[serde(default)]
    pub allow_horizon: bool,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopParams {
    pub id: ProjectId,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Intent {
    pub id: ProjectId,
    pub desired_state: DesiredState,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateWorkerParams {
    pub project_id: ProjectId,
    pub name: String,
    pub connection: String,
    #[serde(default)]
    pub options: WorkerOptions,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerOptions {
    pub queues: Vec<String>,
    pub instances: u32,
    pub sleep_seconds: u32,
    pub tries: u32,
    pub timeout_seconds: u32,
    pub graceful_stop_seconds: u32,
    pub crash_restart: CrashRestartPolicy,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            queues: Vec::new(),
            instances: 1,
            sleep_seconds: 3,
            tries: 1,
            timeout_seconds: 60,
            graceful_stop_seconds: 15,
            crash_restart: CrashRestartPolicy::OnFailure,
        }
    }
}

impl Client {
    pub async fn create_worker(
        &mut self,
        params: &CreateWorkerParams,
    ) -> Result<Mutation<WorkerProfile>, ClientError> {
        self.request("worker.profile.create", params).await
    }

    pub async fn list_profiles(
        &mut self,
        params: &ProjectParams,
    ) -> Result<Vec<WorkerProfile>, ClientError> {
        self.request("worker.profile.list", params).await
    }

    pub async fn start_project(
        &mut self,
        params: &LaunchParams,
    ) -> Result<Mutation<Intent>, ClientError> {
        self.request("project.start", params).await
    }

    pub async fn restart_project(
        &mut self,
        params: &LaunchParams,
    ) -> Result<Mutation<Intent>, ClientError> {
        self.request("project.restart", params).await
    }

    pub async fn stop_project(
        &mut self,
        params: &StopParams,
    ) -> Result<Mutation<Intent>, ClientError> {
        self.request("project.stop", params).await
    }

    pub async fn list_processes(
        &mut self,
        params: &ProjectParams,
    ) -> Result<Vec<WorkerStatus>, ClientError> {
        self.request("process.list", params).await
    }

    pub async fn tail_logs(
        &mut self,
        params: &ProjectParams,
    ) -> Result<Vec<OutputSummary>, ClientError> {
        self.request("log.tail", params).await
    }
}
