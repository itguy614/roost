use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use roost_core::{
    DesiredState, ProjectId, WorkerProfile,
    worker_runtime::{
        ObservedState, OutputStream, OutputSummary, WorkerLease, WorkerLeaseState, WorkerStatus,
    },
};
use roost_store::Store;
use uuid::Uuid;

use super::{Error, verify_horizon};
use crate::{backoff, guardian::Guardian};

const LAUNCH_DEADLINE: Duration = Duration::from_secs(10);

pub(super) struct Slot {
    profile: WorkerProfile,
    php: PathBuf,
    allow_horizon: bool,
    status: WorkerStatus,
    guardian: Option<Guardian>,
    pub(super) lease_id: Option<Uuid>,
    since: Instant,
    retry_at: Option<Instant>,
    output: VecDeque<OutputSummary>,
    stderr_seen: bool,
}

impl Slot {
    pub(super) fn new(
        project: ProjectId,
        profile: WorkerProfile,
        index: u32,
        php: PathBuf,
        allow_horizon: bool,
    ) -> Self {
        Self {
            status: status_for(
                Uuid::new_v4(),
                project,
                profile.id,
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
            stderr_seen: false,
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

    pub(super) fn status(&self) -> WorkerStatus {
        let mut status = self.status.clone();
        status.retry_in_ms = self
            .retry_at
            .map(|at| at.saturating_duration_since(Instant::now()).as_millis() as u64);
        status
    }

    pub(super) fn logs(&self) -> impl Iterator<Item = OutputSummary> + '_ {
        self.output.iter().cloned()
    }

    pub(super) fn stop(&mut self) {
        self.status.desired_state = DesiredState::Stopped;
        self.retry_at = None;
        if let Some(guardian) = &mut self.guardian {
            guardian.stop();
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
        if let Err(error) = verify_horizon(&project, self.allow_horizon) {
            self.status.state = ObservedState::Failed;
            self.status.last_error = Some(error.to_string());
            self.retry_at = None;
            return Ok(());
        }
        let lease = WorkerLease {
            id: Uuid::new_v4(),
            project_id: self.status.project_id,
            profile_id: self.status.profile_id,
            slot: self.status.slot,
            state: WorkerLeaseState::Reserved,
        };
        if let Err(error) = store.reserve_worker(&lease) {
            if matches!(
                error,
                roost_store::Error::Duplicate | roost_store::Error::Reference
            ) {
                self.block("worker slot could not be reserved");
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
        self.stderr_seen = false;
        self.retry_at = None;
        self.since = Instant::now();
        match Guardian::spawn(binary, db, &lease, &self.php) {
            Ok(guardian) => self.guardian = Some(guardian),
            Err(_) => {
                self.status.state = ObservedState::Failed;
                self.status.last_error = Some("could not launch worker guardian".into());
                store.record_worker_state(lease.clone(), WorkerLeaseState::LaunchFailed)?;
                store.release_worker(lease.id)?;
                self.lease_id = None;
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
                guardian.stop();
                self.block("could not wait for worker guardian");
                return Ok(());
            }
        };
        if let Some(pipe) = &mut guardian.stdout {
            pipe.drain(OutputStream::Stdout, &self.status, &mut self.output);
        }
        if let Some(pipe) = &mut guardian.stderr {
            self.stderr_seen |= pipe.drain(OutputStream::Stderr, &self.status, &mut self.output);
        }
        if exit.is_none() && self.status.state == ObservedState::Running {
            if self.since.elapsed() >= backoff::STABLE_RUN {
                self.status.rapid_failures = 0;
            }
            return Ok(());
        }
        let id = self.lease_id.expect("a guardian always has a lease");
        let lease = store.worker_lease(id)?;
        if exit.is_none() {
            if let Some(WorkerLease {
                state: WorkerLeaseState::Running { identity },
                ..
            }) = lease
            {
                if self.status.state == ObservedState::Starting {
                    self.since = Instant::now();
                    self.status.started_at = Some(identity.started_seconds);
                    self.status.state = ObservedState::Running;
                }
                self.status.identity = Some(identity);
            } else if self.status.state == ObservedState::Starting
                && self.since.elapsed() > LAUNCH_DEADLINE
            {
                guardian.stop();
                self.status.state = ObservedState::Stopping;
                self.status.last_error = Some("worker launch did not confirm readiness".into());
            }
            return Ok(());
        }
        match lease.map(|lease| lease.state) {
            Some(WorkerLeaseState::Exited { exit }) => {
                store.release_worker(id)?;
                self.guardian = None;
                self.lease_id = None;
                self.status.identity = None;
                self.status.last_exit = Some(exit);
                self.status.recovery_blocked = false;
                self.after_exit(exit);
            }
            Some(WorkerLeaseState::LaunchFailed) => {
                store.release_worker(id)?;
                self.guardian = None;
                self.lease_id = None;
                self.status.identity = None;
                self.status.recovery_blocked = false;
                if self.status.desired_state == DesiredState::Stopped {
                    self.status.state = ObservedState::Stopped;
                } else {
                    self.status.state = ObservedState::Failed;
                    self.status.last_error = Some("worker executable could not start".into());
                }
            }
            _ => {
                self.guardian = None;
                self.block("worker guardian exited before confirming cleanup");
            }
        }
        Ok(())
    }

    fn after_exit(&mut self, exit: roost_core::worker_runtime::ProcessExit) {
        if self.status.desired_state == DesiredState::Stopped {
            self.status.state = ObservedState::Stopped;
            self.status.last_error = None;
        } else if !exit.requested_stop && exit.code != Some(0) {
            self.status.last_error = Some(
                if self.stderr_seen {
                    "worker exited unsuccessfully and emitted stderr (content suppressed)"
                } else {
                    "worker exited unsuccessfully without stderr; inspect Laravel's own logs"
                }
                .into(),
            );
            self.after_policy(exit);
        } else {
            self.after_policy(exit);
        }
    }

    fn after_policy(&mut self, exit: roost_core::worker_runtime::ProcessExit) {
        if exit.restartable(self.profile.crash_restart) {
            if let Some(delay) =
                backoff::delay(&mut self.status.rapid_failures, self.since.elapsed())
            {
                self.retry_at = Some(Instant::now() + delay);
                self.status.state = ObservedState::Backoff;
            } else {
                self.status.state = ObservedState::Failed;
                self.status.last_error =
                    Some("worker recovery stopped after 10 rapid exits".into());
            }
        } else if exit.code == Some(0) && !exit.requested_stop {
            self.status.state = ObservedState::Stopped;
        } else {
            self.status.state = ObservedState::Failed;
            self.status
                .last_error
                .get_or_insert_with(|| "worker stopped before reaching its desired state".into());
        }
    }

    fn block(&mut self, message: &'static str) {
        self.status.state = ObservedState::Failed;
        self.status.recovery_blocked = true;
        self.status.last_error = Some(message.into());
    }
}

pub(super) fn status_for(
    id: Uuid,
    project_id: ProjectId,
    profile_id: roost_core::WorkerProfileId,
    slot: u32,
    desired_state: DesiredState,
) -> WorkerStatus {
    WorkerStatus {
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
    }
}
