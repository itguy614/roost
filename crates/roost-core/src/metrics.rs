use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ProjectId, worker_runtime::ProcessIdentity};

pub const PROCESS_SAMPLE_SECONDS: u64 = 2;
pub const RAW_SECONDS: u64 = 86_400;
pub const ROLLUP_SECONDS: u64 = 604_800;
pub const BUCKET_SECONDS: u64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessKind {
    Worker,
    Scheduler,
}

impl ProcessKind {
    pub fn for_profile(scheduler: bool) -> Self {
        if scheduler {
            Self::Scheduler
        } else {
            Self::Worker
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Scheduler => "scheduler",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessGeneration {
    pub project_id: ProjectId,
    pub kind: ProcessKind,
    pub profile_id: Uuid,
    pub slot: u32,
    pub instance_id: Uuid,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSubject {
    pub key: ProcessGeneration,
    pub process: ProcessIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessSample {
    pub identity: ProcessGeneration,
    pub observed_at: u64,
    pub cpu_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub uptime_seconds: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}
