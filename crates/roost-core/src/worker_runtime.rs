use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CrashRestartPolicy, DesiredState, ProjectId, WorkerProfileId};

pub const MAX_PROFILE_INSTANCES: u32 = 16;
pub const MAX_GRACEFUL_STOP_SECONDS: u32 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedState {
    Stopped,
    Starting,
    Running,
    Stopping,
    Backoff,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub group: u32,
    pub uid: u32,
    pub started_seconds: u64,
    pub started_microseconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopCause {
    ProjectStop,
    ProjectRestart,
    DaemonShutdown,
    DaemonDisconnected,
    StartupTimeout,
    StatePublicationFailed,
    GuardianWaitFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessExit {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub at: u64,
    pub requested_stop: bool,
    #[serde(default)]
    pub stop_cause: Option<StopCause>,
    pub forced: bool,
}

impl ProcessExit {
    pub fn restartable(self, policy: CrashRestartPolicy) -> bool {
        !self.requested_stop
            && match policy {
                CrashRestartPolicy::Never => false,
                CrashRestartPolicy::OnFailure => self.code != Some(0),
                CrashRestartPolicy::Always => true,
            }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchFailure {
    Validation,
    SpawnRejected,
    ResourceUnavailable,
    ReadinessTimeout,
    StatePublication,
    Cancelled,
}

impl LaunchFailure {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Validation => "worker configuration or PHP path failed validation",
            Self::SpawnRejected => "the OS rejected the worker launch",
            Self::ResourceUnavailable => "OS resources were temporarily unavailable during launch",
            Self::ReadinessTimeout => "startup timed out and worker cleanup was confirmed",
            Self::StatePublication => {
                "could not persist worker ownership; worker cleanup was confirmed"
            }
            Self::Cancelled => "worker launch was cancelled before execution",
        }
    }

    pub fn retryable(self) -> bool {
        matches!(self, Self::ResourceUnavailable | Self::ReadinessTimeout)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkerLeaseState {
    Reserved,
    Claimed,
    Running { identity: ProcessIdentity },
    Exited { exit: ProcessExit },
    LaunchFailed,
    LaunchFailure { failure: LaunchFailure },
    Uncertain { identity: Option<ProcessIdentity> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
    pub id: Uuid,
    pub project_id: ProjectId,
    pub profile_id: WorkerProfileId,
    pub slot: u32,
    pub state: WorkerLeaseState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatus {
    pub id: Uuid,
    pub project_id: ProjectId,
    pub profile_id: WorkerProfileId,
    pub slot: u32,
    pub generation: u64,
    pub desired_state: DesiredState,
    pub state: ObservedState,
    pub identity: Option<ProcessIdentity>,
    pub started_at: Option<u64>,
    pub last_exit: Option<ProcessExit>,
    pub restarts: u32,
    pub rapid_failures: u8,
    pub retry_in_ms: Option<u64>,
    pub recovery_blocked: bool,
    pub last_error: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub launch_failure: Option<LaunchFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSummary {
    pub instance_id: Uuid,
    pub generation: u64,
    pub at: u64,
    pub stream: OutputStream,
    pub bytes: u32,
    pub newlines: u32,
    #[serde(default)]
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_policy_distinguishes_success_failure_and_requested_stops() {
        let success = ProcessExit {
            code: Some(0),
            signal: None,
            at: 0,
            requested_stop: false,
            stop_cause: None,
            forced: false,
        };
        let failed = ProcessExit {
            code: Some(1),
            ..success
        };
        let killed = ProcessExit {
            code: None,
            signal: Some(9),
            ..success
        };
        let manual = ProcessExit {
            requested_stop: true,
            ..killed
        };

        for policy in [
            CrashRestartPolicy::Never,
            CrashRestartPolicy::OnFailure,
            CrashRestartPolicy::Always,
        ] {
            assert!(!manual.restartable(policy));
            assert_eq!(
                success.restartable(policy),
                policy == CrashRestartPolicy::Always
            );
            assert_eq!(
                failed.restartable(policy),
                policy != CrashRestartPolicy::Never
            );
            assert_eq!(
                killed.restartable(policy),
                policy != CrashRestartPolicy::Never
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchIntent {
    pub php: std::path::PathBuf,
    pub allow_horizon: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectIntent {
    #[default]
    Stopped,
    Running(LaunchIntent),
}

impl ProjectIntent {
    pub fn desired(&self) -> DesiredState {
        match self {
            Self::Stopped => DesiredState::Stopped,
            Self::Running(_) => DesiredState::Running,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStartup {
    FirstStart,
    AfterCleanShutdown,
    AfterInterruptedShutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrashReport {
    pub id: Uuid,
    pub project_id: ProjectId,
    pub profile_id: WorkerProfileId,
    pub slot: u32,
    pub exit: ProcessExit,
    pub reason: String,
    pub output: Vec<OutputSummary>,
}
