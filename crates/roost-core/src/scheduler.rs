use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    CrashRestartPolicy, DesiredState, ProjectId, Revision, ValidationError,
    worker_runtime::{
        MAX_GRACEFUL_STOP_SECONDS, ProcessCrash, ProcessLease, ProcessProfileId, ProcessStatus,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchedulerProfileId(pub Uuid);

impl ProcessProfileId for SchedulerProfileId {
    const SCHEDULER: bool = true;

    fn uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerMode {
    ScheduleWork,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerProfile {
    pub id: SchedulerProfileId,
    pub project_id: ProjectId,
    pub enabled: bool,
    pub mode: SchedulerMode,
    pub crash_restart: CrashRestartPolicy,
    pub graceful_stop_seconds: u32,
    pub revision: Revision,
}

impl SchedulerProfile {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if !(1..=MAX_GRACEFUL_STOP_SECONDS).contains(&self.graceful_stop_seconds) {
            return Err(ValidationError(
                "scheduler graceful stop timeout must be between 1 and 60 seconds",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerIntent {
    #[default]
    Stopped,
    Running {
        php: PathBuf,
    },
}

impl SchedulerIntent {
    pub fn desired(&self) -> DesiredState {
        match self {
            Self::Stopped => DesiredState::Stopped,
            Self::Running { .. } => DesiredState::Running,
        }
    }
}

pub type SchedulerLease = ProcessLease<SchedulerProfileId>;
pub type SchedulerProcessStatus = ProcessStatus<SchedulerProfileId>;
pub type SchedulerCrash = ProcessCrash<SchedulerProfileId>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SchedulerStatus {
    pub profile: Option<SchedulerProfile>,
    pub desired_state: DesiredState,
    pub process: Option<SchedulerProcessStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_scheduler_timeout_bounds() {
        let mut profile = SchedulerProfile {
            id: SchedulerProfileId(Uuid::new_v4()),
            project_id: ProjectId(Uuid::new_v4()),
            enabled: true,
            mode: SchedulerMode::ScheduleWork,
            crash_restart: CrashRestartPolicy::OnFailure,
            graceful_stop_seconds: 1,
            revision: Revision(1),
        };
        assert!(profile.validate().is_ok());
        profile.graceful_stop_seconds = 60;
        assert!(profile.validate().is_ok());
        profile.graceful_stop_seconds = 61;
        assert!(profile.validate().is_err());
        profile.graceful_stop_seconds = 0;
        assert!(profile.validate().is_err());
    }
}
