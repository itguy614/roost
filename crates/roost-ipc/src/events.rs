use roost_core::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Client, ClientError};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventHistoryParams {
    pub project_id: Option<ProjectId>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl Client {
    pub async fn event_history(
        &mut self,
        params: &EventHistoryParams,
    ) -> Result<Value, ClientError> {
        self.request("event.history", params).await
    }
}
