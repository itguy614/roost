use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{PhpProvider, WorkerProfileId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhpSource {
    Override,
    Herd,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedPhp {
    pub path: PathBuf,
    pub source: PhpSource,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProviderDetection {
    pub preferred: PhpProvider,
    pub herd_available: bool,
    pub system_available: bool,
    pub override_configured: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConnection {
    pub name: String,
    pub driver: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSnapshot {
    pub default: Option<String>,
    pub connections: Vec<QueueConnection>,
    pub omitted_connections: u32,
    pub observed_at: u64,
    pub php_source: PhpSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionSource {
    StaticSource,
    Explicit,
    WorkerProfile,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueSuggestion {
    pub name: String,
    pub sources: Vec<SuggestionSource>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfiguredQueues {
    pub profile_id: WorkerProfileId,
    pub connection: String,
    pub queues: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueSuggestions {
    pub suggestions: Vec<QueueSuggestion>,
    pub configured_profiles: Vec<ConfiguredQueues>,
    pub complete: bool,
    pub dynamic_references: bool,
    pub scan_limited: bool,
    pub files_scanned: u32,
    pub omitted_values: bool,
}
