use std::path::PathBuf;

use roost_core::{
    CrashRestartPolicy, ProjectId, Revision,
    scheduler::{SchedulerCrash, SchedulerProfile, SchedulerProfileId, SchedulerStatus},
    worker_runtime::OutputSummary,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Client, ClientError, Mutation,
    workers::{CrashDetail, CrashParams, Intent, ProjectParams, StopParams},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureSchedulerParams {
    pub id: ProjectId,
    pub expected_revision: Option<Revision>,
    #[serde(default)]
    pub options: SchedulerOptions,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerOptions {
    pub enabled: bool,
    pub crash_restart: CrashRestartPolicy,
    pub graceful_stop_seconds: u32,
}

impl Default for SchedulerOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            crash_restart: CrashRestartPolicy::OnFailure,
            graceful_stop_seconds: 15,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerLaunchParams {
    pub id: ProjectId,
    pub php: PathBuf,
    pub idempotency_key: Option<Uuid>,
}

impl Client {
    pub async fn configure_scheduler(
        &mut self,
        params: &ConfigureSchedulerParams,
    ) -> Result<Mutation<SchedulerProfile>, ClientError> {
        self.request("scheduler.configure", params).await
    }

    pub async fn scheduler_status(
        &mut self,
        params: &ProjectParams,
    ) -> Result<SchedulerStatus, ClientError> {
        self.request("scheduler.status", params).await
    }

    pub async fn start_scheduler(
        &mut self,
        params: &SchedulerLaunchParams,
    ) -> Result<Mutation<Intent>, ClientError> {
        self.request("scheduler.start", params).await
    }

    pub async fn restart_scheduler(
        &mut self,
        params: &SchedulerLaunchParams,
    ) -> Result<Mutation<Intent>, ClientError> {
        self.request("scheduler.restart", params).await
    }

    pub async fn stop_scheduler(
        &mut self,
        params: &StopParams,
    ) -> Result<Mutation<Intent>, ClientError> {
        self.request("scheduler.stop", params).await
    }

    pub async fn scheduler_logs(
        &mut self,
        params: &ProjectParams,
    ) -> Result<Vec<OutputSummary>, ClientError> {
        self.request("scheduler.logs", params).await
    }

    pub async fn scheduler_crash(
        &mut self,
        params: &CrashParams,
    ) -> Result<CrashDetail<SchedulerProfileId>, ClientError> {
        self.request("scheduler.crash.get", params).await
    }

    pub async fn scheduler_crashes(
        &mut self,
        params: &ProjectParams,
    ) -> Result<Vec<SchedulerCrash>, ClientError> {
        self.request("scheduler.crashes", params).await
    }
}
