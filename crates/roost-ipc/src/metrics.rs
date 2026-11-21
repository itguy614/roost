use roost_core::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Client, ClientError, workers::ProjectParams};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Resolution {
    #[default]
    Raw,
    #[serde(rename = "5m")]
    FiveMinutes,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryParams {
    pub project_id: ProjectId,
    pub profile_id: Option<Uuid>,
    #[serde(default)]
    pub resolution: Resolution,
    pub from: u64,
    pub to: Option<u64>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl Client {
    pub async fn process_metrics(&mut self, params: &ProjectParams) -> Result<Value, ClientError> {
        self.request("metrics.process.current", params).await
    }

    pub async fn process_history(&mut self, params: &HistoryParams) -> Result<Value, ClientError> {
        self.request("metrics.process.history", params).await
    }

    pub async fn project_metrics(&mut self, params: &ProjectParams) -> Result<Value, ClientError> {
        self.request("metrics.project.summary", params).await
    }
}
