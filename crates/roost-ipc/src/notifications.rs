use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Client, ClientError};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventPolicyParams {
    pub id: Uuid,
    pub lineage: String,
}

impl Client {
    pub async fn notification_policy(
        &mut self,
        params: &EventPolicyParams,
    ) -> Result<Value, ClientError> {
        self.request("notifications.event.policy", params).await
    }

    pub async fn notification_status(&mut self) -> Result<Value, ClientError> {
        self.request("notifications.helper.status", &serde_json::json!({}))
            .await
    }
}
