use roost_core::{ProjectId, Revision, WorkerProfileId, alerts::Rule};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Client, ClientError};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdProfileParams {
    pub profile_id: WorkerProfileId,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdUpdateParams {
    pub profile_id: WorkerProfileId,
    pub expected_revision: Revision,
    pub rules: Vec<Rule>,
    pub notifications: Option<bool>,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertListParams {
    pub project_id: ProjectId,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl Client {
    pub async fn thresholds(
        &mut self,
        params: &ThresholdProfileParams,
    ) -> Result<Value, ClientError> {
        self.request("metrics.thresholds.get", params).await
    }

    pub async fn update_thresholds(
        &mut self,
        params: &ThresholdUpdateParams,
    ) -> Result<Value, ClientError> {
        self.request("metrics.thresholds.update", params).await
    }

    pub async fn list_alerts(&mut self, params: &AlertListParams) -> Result<Value, ClientError> {
        self.request("metrics.alerts.list", params).await
    }
}
