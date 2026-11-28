use roost_core::{ProjectId, Revision, WorkerProfileId, scheduler::SchedulerProfileId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Client, ClientError, Mutation, workers::ProjectParams};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchFence {
    pub daemon_id: Uuid,
    pub project_id: ProjectId,
    pub project_revision: Revision,
    pub workers: Vec<(WorkerProfileId, Revision)>,
    pub scheduler: Option<(SchedulerProfileId, Revision)>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptParams {
    pub operation_id: Uuid,
    pub method: String,
    pub params: Value,
}

impl Client {
    pub async fn launch_fence(
        &mut self,
        params: &ProjectParams,
    ) -> Result<LaunchFence, ClientError> {
        self.request("project.launchFence", params).await
    }

    pub async fn mutation_receipt(
        &mut self,
        params: &ReceiptParams,
    ) -> Result<Option<Mutation<Value>>, ClientError> {
        self.request("mutation.receipt", params).await
    }

    /// Submit a typed caller's serialized mutation. This permits retaining the
    /// exact wire payload for receipt validation if its response is lost.
    pub async fn submit_mutation(
        &mut self,
        method: &str,
        params: &Value,
    ) -> Result<Mutation<Value>, ClientError> {
        self.request(method, params).await
    }
}
