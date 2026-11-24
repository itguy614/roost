use roost_core::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Client, ClientError, metrics::Resolution, workers::ProjectParams};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureQueueMetrics {
    pub id: ProjectId,
    pub enabled: bool,
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHistoryParams {
    pub project_id: ProjectId,
    pub connection: Option<String>,
    pub queue: Option<String>,
    #[serde(default)]
    pub resolution: Resolution,
    pub from: u64,
    pub to: Option<u64>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl Client {
    pub async fn configure_queue_metrics(
        &mut self,
        params: &ConfigureQueueMetrics,
    ) -> Result<Value, ClientError> {
        self.request("metrics.queue.configure", params).await
    }

    pub async fn queue_metrics(&mut self, params: &ProjectParams) -> Result<Value, ClientError> {
        self.request("metrics.queue.current", params).await
    }

    pub async fn queue_history(
        &mut self,
        params: &QueueHistoryParams,
    ) -> Result<Value, ClientError> {
        self.request("metrics.queue.history", params).await
    }
}
