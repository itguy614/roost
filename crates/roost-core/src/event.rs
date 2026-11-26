use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ProjectId, metrics::ProcessKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ProcessCrash,
    PermanentRecoveryFailure,
    ResourceThreshold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub project_id: ProjectId,
    pub profile_id: Uuid,
    pub slot: u32,
    pub process_kind: ProcessKind,
    pub kind: EventKind,
    pub at: u64,
    pub crash_id: Option<Uuid>,
    pub actions_suppressed: bool,
}
