use std::path::PathBuf;

use roost_core::{
    PhpProvider, ProjectId,
    discovery::{ConnectionSnapshot, ProviderDetection, QueueSuggestions, ResolvedPhp},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{Client, ClientError, workers::ProjectParams};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvePhpParams {
    pub id: ProjectId,
    pub provider: Option<PhpProvider>,
    pub php: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuggestQueueParams {
    pub id: ProjectId,
    #[serde(default)]
    pub explicit: Vec<String>,
}

impl Client {
    pub async fn list_providers(&mut self) -> Result<Vec<PhpProvider>, ClientError> {
        self.request("provider.list", &json!({})).await
    }

    pub async fn detect_providers(
        &mut self,
        params: &ProjectParams,
    ) -> Result<ProviderDetection, ClientError> {
        self.request("provider.detect", params).await
    }

    pub async fn resolve_php(
        &mut self,
        params: &ResolvePhpParams,
    ) -> Result<ResolvedPhp, ClientError> {
        self.request("provider.resolvePhp", params).await
    }

    pub async fn queue_connections(
        &mut self,
        params: &ProjectParams,
    ) -> Result<Option<ConnectionSnapshot>, ClientError> {
        self.request("queue.connections", params).await
    }

    pub async fn discover_queues(
        &mut self,
        params: &ResolvePhpParams,
    ) -> Result<ConnectionSnapshot, ClientError> {
        self.request("queue.discover", params).await
    }

    pub async fn suggest_queues(
        &mut self,
        params: &SuggestQueueParams,
    ) -> Result<QueueSuggestions, ClientError> {
        self.request("queue.suggestions", params).await
    }
}
