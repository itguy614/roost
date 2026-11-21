use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use roost_core::{
    CodeRestartPolicy, CrashRestartPolicy, DesiredState, ProjectId, WorkerProfile,
    worker_runtime::{
        LaunchFailure, ObservedState, OutputStream, OutputSummary, ProcessCrash, ProcessExit,
        ProcessLease, ProcessProfileId, ProcessStatus, StopCause, WorkerLeaseState,
    },
};
use roost_process::DrainResult;
use roost_store::Store;
use uuid::Uuid;

use super::{Error, verify_horizon};
use crate::{backoff, guardian::Guardian, supervised::SupervisedProfile};

const LAUNCH_DEADLINE: Duration = Duration::from_secs(10);
const EXIT_DRAIN_WINDOW: Duration = Duration::from_secs(2);
const MAX_LAUNCH_ATTEMPTS: u8 = 3;

pub(super) struct Slot<P: SupervisedProfile = WorkerProfile> {
    profile: P,
    php: PathBuf,
    allow_horizon: bool,
    status: ProcessStatus<P::Id>,
    guardian: Option<Guardian>,
    pub(super) lease_id: Option<Uuid>,
    since: Instant,
    retry_at: Option<Instant>,
    output: VecDeque<OutputSummary>,
    last_output_at: Instant,
    stderr_seen: bool,
    startup_timed_out: bool,
    draining_since: Option<Instant>,
    launch_failures: u8,
    code_restart_pending: bool,
}

impl<P: SupervisedProfile> Slot<P> {
    pub(super) fn new(
        project: ProjectId,
        profile: P,
        index: u32,
        php: PathBuf,
        allow_horizon: bool,
    ) -> Self {
        Self {
            status: status_for(
                Uuid::new_v4(),
                project,
                profile.id(),
                index,
                DesiredState::Running,
            ),
            profile,
            php,
            allow_horizon,
            guardian: None,
            lease_id: None,
            since: Instant::now(),
            retry_at: None,
            output: VecDeque::new(),
            last_output_at: Instant::now(),
            stderr_seen: false,
            startup_timed_out: false,
            draining_since: None,
            launch_failures: 0,
            code_restart_pending: false,
        }
    }

    pub(super) fn desired(&self) -> DesiredState {
        self.status.desired_state
    }

    pub(super) fn has_guardian(&self) -> bool {
        self.guardian.is_some()
    }

    pub(super) fn recovery_blocked(&self) -> bool {
        self.status.recovery_blocked
    }

    pub(super) fn status(&self) -> ProcessStatus<P::Id> {
        let mut status = self.status.clone();
        status.retry_in_ms = self
            .retry_at
            .map(|at| at.saturating_duration_since(Instant::now()).as_millis() as u64);
        status
    }

    pub(super) fn metric_subject(&self) -> Option<roost_core::metrics::MetricSubject> {
        use roost_core::metrics::{MetricSubject, ProcessGeneration, ProcessKind};
        if self.status.state != ObservedState::Running
            || self.status.recovery_blocked
            || !self.has_guardian()
        {
            return None;
        }
        Some(MetricSubject {
            process: self.status.identity?,
            key: ProcessGeneration {
                project_id: self.status.project_id,
                kind: ProcessKind::for_profile(P::Id::SCHEDULER),
                profile_id: self.status.profile_id.uuid(),
                slot: self.status.slot,
                instance_id: self.status.id,
                generation: self.status.generation,
            },
        })
    }

    pub(super) fn logs(&self) -> impl Iterator<Item = OutputSummary> + '_ {
        self.output.iter().cloned()
    }

    pub(super) fn output_priority(&self) -> Option<(bool, Instant)> {
        (!self.output.is_empty()).then_some((self.has_guardian(), self.last_output_at))
    }

    pub(super) fn discard_output(&mut self) {
        self.output = VecDeque::new();
    }

    pub(super) fn stop(&mut self, cause: StopCause) {
        self.code_restart_pending = false;
        self.status.desired_state = DesiredState::Stopped;
        self.retry_at = None;
        self.status.reason = Some("process stop requested".into());
        if let Some(guardian) = &mut self.guardian {
            guardian.stop(cause);
            self.status.state = ObservedState::Stopping;
        } else if !self.status.recovery_blocked {
            self.status.state = ObservedState::Stopped;
        }
    }

    pub(super) fn should_launch(&self) -> bool {
        self.guardian.is_none()
            && self.status.desired_state == DesiredState::Running
            && match self.status.state {
                ObservedState::Starting => true,
                ObservedState::Backoff => self.retry_at.is_some_and(|at| Instant::now() >= at),
                _ => false,
            }
    }

    pub(super) fn launch(&mut self, store: &Store, binary: &Path, db: &Path) -> Result<(), Error> {
        let project = store
            .get_project(self.status.project_id)?
            .ok_or(roost_store::Error::NotFound)?;
        if !P::Id::SCHEDULER
            && let Err(error) = verify_horizon(&project, self.allow_horizon)
        {
            self.status.state = ObservedState::Failed;
            self.status.reason = Some(error.to_string());
            self.status.last_error = self.status.reason.clone();
            self.status.launch_failure = Some(LaunchFailure::Validation);
            self.retry_at = None;
            return Ok(());
        }
        let lease = ProcessLease {
            id: Uuid::new_v4(),
            project_id: self.status.project_id,
            profile_id: self.status.profile_id,
            slot: self.status.slot,
            state: WorkerLeaseState::Reserved,
        };
        if let Err(error) = store.reserve_process(&lease) {
            if matches!(
                error,
                roost_store::Error::Duplicate | roost_store::Error::Reference
            ) {
                self.block("process slot could not be reserved");
                return Ok(());
            }
            return Err(error.into());
        }
        self.lease_id = Some(lease.id);
        self.status.generation += 1;
        if self.status.generation > 1 {
            self.status.restarts = self.status.restarts.saturating_add(1);
        }
        self.status.state = ObservedState::Starting;
        self.status.started_at = None;
        self.status.last_error = None;
        self.status.reason = None;
        self.status.launch_failure = None;
        self.stderr_seen = false;
        self.startup_timed_out = false;
        self.draining_since = None;
        self.retry_at = None;
        self.since = Instant::now();
        match Guardian::spawn(binary, db, &lease, &self.php) {
            Ok(guardian) => self.guardian = Some(guardian),
            Err(error) => {
                let failure = roost_process::classify_spawn_error(&error);
                // No guardian was spawned. If the DB update itself fails, the
                // reservation remains an ownership fence until daemon recovery.
                if let Err(error) = store
                    .record_process_state(
                        lease.clone(),
                        WorkerLeaseState::LaunchFailure { failure },
                    )
                    .and_then(|()| store.finish_process::<P::Id>(lease.id, None))
                {
                    self.block("could not release a failed process launch reservation");
                    return Err(error.into());
                }
                self.lease_id = None;
                self.after_launch_failure(failure);
            }
        }
        Ok(())
    }

    pub(super) fn poll(&mut self, store: &Store) -> Result<(), Error> {
        let Some(guardian) = &mut self.guardian else {
            return Ok(());
        };
        let exit = match guardian.child.try_wait() {
            Ok(status) => status,
            Err(_) => {
                guardian.stop(StopCause::GuardianWaitFailed);
                self.block("could not wait for process guardian");
                return Ok(());
            }
        };
        let guardian_exited = exit.is_some();
        let absent = || DrainResult {
            saw_output: false,
            complete: true,
        };
        let stdout = guardian.stdout.as_mut().map_or_else(absent, |pipe| {
            pipe.drain(
                OutputStream::Stdout,
                &self.status,
                &mut self.output,
                guardian_exited,
            )
        });
        let stderr = guardian.stderr.as_mut().map_or_else(absent, |pipe| {
            pipe.drain(
                OutputStream::Stderr,
                &self.status,
                &mut self.output,
                guardian_exited,
            )
        });
        self.stderr_seen |= stderr.saw_output;
        if stdout.saw_output || stderr.saw_output {
            self.last_output_at = Instant::now();
        }

        let mut output_incomplete = false;
        if guardian_exited && !(stdout.complete && stderr.complete) {
            let since = self.draining_since.get_or_insert_with(Instant::now);
            self.status.identity = None;
            self.status.state = ObservedState::Stopping;
            self.status.reason = Some("guardian exited; collecting buffered worker output".into());
            if since.elapsed() < EXIT_DRAIN_WINDOW {
                return Ok(());
            }
            // Avoid holding the lease open if an escaped writer keeps sending data.
            // Finalizing the lease drops the pipes and any unfinished capture.
            output_incomplete = true;
        }

        if exit.is_none() && self.status.state == ObservedState::Running {
            if self.since.elapsed() >= backoff::STABLE_RUN {
                self.status.rapid_failures = 0;
            }
            return Ok(());
        }
        let id = self.lease_id.expect("a guardian always has a lease");
        let lease = store.process_lease::<P::Id>(id)?;
        if exit.is_none() {
            if let Some(ProcessLease {
                state: WorkerLeaseState::Running { identity },
                ..
            }) = lease
            {
                if self.status.state == ObservedState::Starting {
                    self.since = Instant::now();
                    self.launch_failures = 0;
                    self.status.started_at = Some(identity.started_seconds);
                    self.status.state = ObservedState::Running;
                }
                self.status.identity = Some(identity);
            } else if self.status.state == ObservedState::Starting
                && self.since.elapsed() > LAUNCH_DEADLINE
            {
                guardian.stop(StopCause::StartupTimeout);
                self.status.state = ObservedState::Stopping;
                self.startup_timed_out = true;
                self.status.reason =
                    Some("process launch did not confirm readiness; waiting for cleanup".into());
            }
            return Ok(());
        }
        match lease.map(|lease| lease.state) {
            Some(WorkerLeaseState::Reserved) => {
                // A reaped guardian that never claimed its reservation could not have spawned a worker.
                let lease = ProcessLease {
                    id,
                    project_id: self.status.project_id,
                    profile_id: self.status.profile_id,
                    slot: self.status.slot,
                    state: WorkerLeaseState::Reserved,
                };
                let failure = LaunchFailure::SpawnRejected;
                store.record_process_state(lease, WorkerLeaseState::LaunchFailure { failure })?;
                // Process the new terminal record next tick if the ledger release fails.
                store.finish_process::<P::Id>(id, None)?;
                self.detach();
                self.after_launch_failure(failure);
            }
            Some(WorkerLeaseState::Exited { exit }) => {
                let report = (!exit.requested_stop && exit.code != Some(0)).then(|| ProcessCrash {
                    id,
                    project_id: self.status.project_id,
                    profile_id: self.status.profile_id,
                    slot: self.status.slot,
                    exit,
                    reason: if output_incomplete {
                        format!(
                            "{}; output drain timed out",
                            exit_reason(exit, self.stderr_seen)
                        )
                    } else {
                        exit_reason(exit, self.stderr_seen)
                    },
                    output: self
                        .output
                        .iter()
                        .filter(|line| line.generation == self.status.generation)
                        .cloned()
                        .collect(),
                });
                store.finish_process::<P::Id>(id, report.as_ref())?;
                self.detach();
                self.status.last_exit = Some(exit);
                let code_restart = std::mem::take(&mut self.code_restart_pending);
                if exit.stop_cause == Some(StopCause::StatePublicationFailed) {
                    self.after_launch_failure(LaunchFailure::StatePublication);
                } else if self.startup_timed_out && exit.requested_stop {
                    self.after_launch_failure(LaunchFailure::ReadinessTimeout);
                } else if code_restart && exit.stop_cause == Some(StopCause::ProjectRestart) {
                    self.status.state = ObservedState::Starting;
                    self.status.reason = Some("restarting after a watched file change".into());
                } else {
                    self.after_exit(exit);
                }
            }
            Some(
                state @ (WorkerLeaseState::LaunchFailed | WorkerLeaseState::LaunchFailure { .. }),
            ) => {
                store.finish_process::<P::Id>(id, None)?;
                self.detach();
                let failure = match state {
                    WorkerLeaseState::LaunchFailure { failure } => failure,
                    _ => LaunchFailure::SpawnRejected,
                };
                if self.startup_timed_out && failure == LaunchFailure::Cancelled {
                    self.after_launch_failure(LaunchFailure::ReadinessTimeout);
                } else {
                    self.after_launch_failure(failure);
                }
            }
            _ => {
                self.guardian = None;
                self.block("process guardian exited before confirming cleanup");
            }
        }
        Ok(())
    }

    fn detach(&mut self) {
        self.guardian = None;
        self.lease_id = None;
        self.status.identity = None;
        self.status.recovery_blocked = false;
    }

    fn after_launch_failure(&mut self, failure: LaunchFailure) {
        if self.status.desired_state == DesiredState::Stopped {
            self.status.state = ObservedState::Stopped;
            self.status.reason = Some("process launch stopped".into());
            self.status.last_error = None;
            return;
        }
        self.launch_failures = self.launch_failures.saturating_add(1);
        let reason = failure.reason();
        self.status.launch_failure = Some(failure);
        self.status.last_error = Some(reason.into());
        if failure.retryable() && self.profile.crash_restart() != CrashRestartPolicy::Never {
            if self.launch_failures < MAX_LAUNCH_ATTEMPTS {
                self.retry_at =
                    Some(Instant::now() + Duration::from_secs(u64::from(self.launch_failures)));
                self.status.state = ObservedState::Backoff;
                self.status.reason = Some(format!("{reason}; retry scheduled"));
                return;
            }
            self.status.reason = Some(format!("{reason}; startup retry limit reached"));
        } else {
            self.status.reason = Some(format!("{reason}; startup will not retry"));
        }
        self.status.state = ObservedState::Failed;
    }

    fn after_exit(&mut self, exit: ProcessExit) {
        let reason = exit_reason(exit, self.stderr_seen);
        if self.status.desired_state == DesiredState::Stopped {
            self.status.state = ObservedState::Stopped;
            self.status.reason = Some(if exit.requested_stop {
                reason
            } else {
                format!("{reason}; project stop prevents restart")
            });
            self.status.last_error = None;
            return;
        }
        if exit.code != Some(0) || exit.requested_stop {
            self.status.last_error = Some(reason.clone());
        }
        if exit.restartable(self.profile.crash_restart()) {
            if let Some(delay) =
                backoff::delay(&mut self.status.rapid_failures, self.since.elapsed())
            {
                self.retry_at = Some(Instant::now() + delay);
                self.status.state = ObservedState::Backoff;
                self.status.reason = Some(format!("{reason}; restart scheduled"));
                return;
            }
            self.status.state = ObservedState::Failed;
            self.status.reason = Some(format!("{reason}; recovery stopped after 10 rapid exits"));
            self.status.last_error = self.status.reason.clone();
        } else {
            self.status.state = if exit.code == Some(0) && !exit.requested_stop {
                ObservedState::Stopped
            } else {
                ObservedState::Failed
            };
            self.status.reason = Some(format!(
                "{reason}; restart policy did not restart the process"
            ));
        }
    }

    fn block(&mut self, message: &'static str) {
        self.status.state = ObservedState::Failed;
        self.status.recovery_blocked = true;
        self.status.last_error = Some(message.into());
        self.status.reason = self.status.last_error.clone();
    }
}

impl Slot {
    pub(super) fn request_code_restart(&mut self, changed_at: Instant) {
        if self.profile.code_restart == CodeRestartPolicy::OnChange
            && self.status.desired_state == DesiredState::Running
            && self.status.state == ObservedState::Running
            && self.has_guardian()
            && self.since <= changed_at
        {
            self.stop(StopCause::ProjectRestart);
            self.code_restart_pending = true;
            self.status.desired_state = DesiredState::Running;
            self.status.reason = Some("restarting after a watched file change".into());
        }
    }
}

fn exit_reason(exit: ProcessExit, stderr_seen: bool) -> String {
    if exit.requested_stop {
        let reason = match exit.stop_cause {
            Some(StopCause::ProjectStop) => "process stopped on user request",
            Some(StopCause::ProjectRestart) => "process stopped for project restart",
            Some(StopCause::DaemonShutdown) => "process stopped during daemon shutdown",
            Some(StopCause::DaemonDisconnected) => {
                "process stopped after losing its daemon connection"
            }
            Some(StopCause::StartupTimeout) => "process stopped after startup timed out",
            Some(StopCause::StatePublicationFailed) => {
                "process stopped because ownership could not be recorded"
            }
            Some(StopCause::GuardianWaitFailed) => "process stopped after a guardian wait error",
            None => "process stopped on request",
        };
        return if exit.forced {
            format!("{reason}; SIGKILL was required")
        } else {
            reason.into()
        };
    }
    if exit.code == Some(0) {
        return "process exited cleanly while the project desired running".into();
    }
    let kind = if let Some(signal) = exit.signal {
        format!("process exited on signal {signal}")
    } else if let Some(code) = exit.code {
        format!("process exited with status {code}")
    } else {
        "process exited unexpectedly".into()
    };
    if stderr_seen {
        format!("{kind}; see the captured stderr")
    } else {
        format!("{kind} without stderr; inspect Laravel's own logs")
    }
}

pub(super) fn status_for<P>(
    id: Uuid,
    project_id: ProjectId,
    profile_id: P,
    slot: u32,
    desired_state: DesiredState,
) -> ProcessStatus<P> {
    ProcessStatus {
        id,
        project_id,
        profile_id,
        slot,
        generation: 0,
        desired_state,
        state: ObservedState::Starting,
        identity: None,
        started_at: None,
        last_exit: None,
        restarts: 0,
        rapid_failures: 0,
        retry_in_ms: None,
        recovery_blocked: false,
        last_error: None,
        reason: None,
        launch_failure: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roost_core::{CodeRestartPolicy, Revision, WorkerProfileId};

    fn slot(policy: CrashRestartPolicy) -> Slot {
        let project = ProjectId(Uuid::new_v4());
        Slot::new(
            project,
            WorkerProfile {
                id: WorkerProfileId(Uuid::new_v4()),
                project_id: project,
                name: "test".into(),
                connection: "database".into(),
                queues: Vec::new(),
                desired_instances: 1,
                sleep_seconds: 3,
                tries: 1,
                timeout_seconds: 60,
                graceful_stop_seconds: 1,
                crash_restart: policy,
                code_restart: CodeRestartPolicy::Never,
                notifications: false,
                enabled: true,
                revision: Revision(1),
            },
            0,
            "/php".into(),
            false,
        )
    }

    #[test]
    fn only_transient_launch_failures_retry_and_stop_cancels_the_retry() {
        let mut worker = slot(CrashRestartPolicy::OnFailure);
        worker.after_launch_failure(LaunchFailure::ResourceUnavailable);
        assert_eq!(worker.status.state, ObservedState::Backoff);
        assert!(worker.retry_at.is_some());
        worker.after_launch_failure(LaunchFailure::ReadinessTimeout);
        assert_eq!(worker.status.state, ObservedState::Backoff);
        worker.after_launch_failure(LaunchFailure::ResourceUnavailable);
        assert_eq!(worker.status.state, ObservedState::Failed);
        assert!(
            worker
                .status
                .reason
                .as_ref()
                .unwrap()
                .contains("retry limit")
        );

        let mut worker = slot(CrashRestartPolicy::Always);
        worker.after_launch_failure(LaunchFailure::Validation);
        assert_eq!(worker.status.state, ObservedState::Failed);
        assert!(worker.retry_at.is_none());

        let mut worker = slot(CrashRestartPolicy::Never);
        worker.after_launch_failure(LaunchFailure::ResourceUnavailable);
        assert_eq!(worker.status.state, ObservedState::Failed);
        assert!(worker.retry_at.is_none());

        let mut worker = slot(CrashRestartPolicy::Always);
        worker.after_launch_failure(LaunchFailure::ResourceUnavailable);
        worker.stop(StopCause::ProjectStop);
        assert_eq!(worker.status.state, ObservedState::Stopped);
        assert!(!worker.should_launch());
    }
}
