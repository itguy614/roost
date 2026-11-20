use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use roost_core::{
    DesiredState, Project, ProjectId, WorkerProfile, unix_timestamp,
    worker_runtime::{
        ObservedState, OutputStream, OutputSummary, WorkerLease, WorkerLeaseState, WorkerStatus,
    },
};
use roost_store::Store;
use uuid::Uuid;

use crate::{backoff, guardian::Guardian};

const MAX_ACTIVE: usize = 32;
const LAUNCH_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] roost_store::Error),
    #[error("{0}")]
    Invalid(&'static str),
    #[error("{0}")]
    Conflict(&'static str),
}

pub struct Supervisor {
    binary: PathBuf,
    db: PathBuf,
    projects: HashMap<ProjectId, ProjectRuntime>,
    closing: bool,
}

struct ProjectRuntime {
    desired: DesiredState,
    replacement: Option<Plan>,
    slots: Vec<Slot>,
}

pub struct Plan {
    php: PathBuf,
    slots: Vec<(WorkerProfile, u32)>,
}

struct Slot {
    profile: WorkerProfile,
    php: PathBuf,
    status: WorkerStatus,
    guardian: Option<Guardian>,
    lease_id: Option<Uuid>,
    since: Instant,
    retry_at: Option<Instant>,
    output: VecDeque<OutputSummary>,
    stderr_seen: bool,
}

impl Supervisor {
    pub fn new(binary: PathBuf, db: PathBuf) -> Self {
        Self {
            binary,
            db,
            projects: HashMap::new(),
            closing: false,
        }
    }

    pub fn desired(&self, id: ProjectId) -> DesiredState {
        self.projects
            .get(&id)
            .map_or(DesiredState::Stopped, |project| project.desired)
    }

    pub fn validate_mutation(&self) -> Result<(), Error> {
        if self.closing {
            return Err(Error::Conflict("daemon is shutting down"));
        }
        Ok(())
    }

    pub fn validate_config_change(&self, store: &Store, id: ProjectId) -> Result<(), Error> {
        self.validate_mutation()?;
        if self.desired(id) == DesiredState::Running || !store.project_worker_leases(id)?.is_empty()
        {
            return Err(Error::Conflict(
                "stop the project and wait for worker cleanup first",
            ));
        }
        Ok(())
    }

    pub fn prepare(
        &self,
        store: &Store,
        id: ProjectId,
        php: &Path,
        allow_horizon: bool,
        restart: bool,
    ) -> Result<Option<Plan>, Error> {
        self.validate_mutation()?;
        let project = store.get_project(id)?.ok_or(roost_store::Error::NotFound)?;
        let runtime = self.projects.get(&id);
        for lease in store.project_worker_leases(id)? {
            if lease_is_complete(&lease.state) {
                continue;
            }
            if runtime.is_none_or(|runtime| {
                !runtime.slots.iter().any(|slot| {
                    slot.guardian.is_some()
                        && slot.lease_id == Some(lease.id)
                        && lease.state != WorkerLeaseState::Uncertain
                })
            }) {
                return Err(Error::Conflict(
                    "worker recovery is blocked until the previous guardian confirms cleanup",
                ));
            }
        }
        if !restart && self.desired(id) == DesiredState::Running {
            return Ok(None);
        }
        #[cfg(not(target_os = "macos"))]
        return Err(Error::Invalid(
            "worker supervision is currently supported on macOS",
        ));

        #[cfg(target_os = "macos")]
        {
            verify_horizon(&project, allow_horizon)?;
            let mut slots = Vec::new();
            let mut executable = None;
            for profile in store
                .list_worker_profiles(id)?
                .into_iter()
                .filter(|p| p.enabled && p.desired_instances > 0)
            {
                let php =
                    roost_process::validate_worker(&project, &profile, php).map_err(|_| {
                        Error::Invalid(
                            "project, worker profile, or PHP executable failed validation",
                        )
                    })?;
                if slots.len() + profile.desired_instances as usize > MAX_ACTIVE {
                    return Err(Error::Invalid("project exceeds 32 worker instances"));
                }
                executable = Some(php);
                slots.extend((0..profile.desired_instances).map(|slot| (profile.clone(), slot)));
            }
            let php = executable.ok_or(Error::Invalid(
                "project has no enabled worker profiles with instances",
            ))?;
            let other_count: usize = self
                .projects
                .iter()
                .filter(|(project, _)| **project != id)
                .map(|(_, runtime)| runtime.reserved_capacity())
                .sum();
            if other_count + slots.len() > MAX_ACTIVE {
                return Err(Error::Conflict("daemon capacity is 32 worker instances"));
            }
            Ok(Some(Plan { php, slots }))
        }
    }

    pub fn apply_start(&mut self, id: ProjectId, plan: Plan) {
        let runtime = self.projects.entry(id).or_insert_with(|| ProjectRuntime {
            desired: DesiredState::Stopped,
            replacement: None,
            slots: Vec::new(),
        });
        runtime.stop_slots();
        runtime.desired = DesiredState::Running;
        runtime.replacement = Some(plan);
    }

    pub fn apply_stop(&mut self, id: ProjectId) {
        if let Some(project) = self.projects.get_mut(&id) {
            project.desired = DesiredState::Stopped;
            project.replacement = None;
            project.stop_slots();
        }
    }

    pub fn remove(&mut self, id: ProjectId) {
        self.projects.remove(&id);
    }

    pub fn shutdown(&mut self) {
        self.closing = true;
        for runtime in self.projects.values_mut() {
            runtime.replacement = None;
            runtime.desired = DesiredState::Stopped;
            runtime.stop_slots();
        }
    }

    pub fn has_children(&self) -> bool {
        self.projects
            .values()
            .any(|project| project.slots.iter().any(|slot| slot.guardian.is_some()))
    }

    pub fn status(&self, store: &Store, id: ProjectId) -> Result<Vec<WorkerStatus>, Error> {
        let mut statuses: Vec<_> = self
            .projects
            .get(&id)
            .into_iter()
            .flat_map(|runtime| {
                runtime.slots.iter().map(|slot| {
                    let mut status = slot.status.clone();
                    status.retry_in_ms = slot
                        .retry_at
                        .map(|at| at.saturating_duration_since(Instant::now()).as_millis() as u64);
                    status
                })
            })
            .collect();
        for lease in store.project_worker_leases(id)? {
            if lease_is_complete(&lease.state)
                || self.projects.get(&id).is_some_and(|runtime| {
                    runtime
                        .slots
                        .iter()
                        .any(|slot| slot.lease_id == Some(lease.id))
                })
            {
                continue;
            }
            let identity = match lease.state {
                WorkerLeaseState::Running { identity } => Some(identity),
                _ => None,
            };
            statuses.push(WorkerStatus {
                identity,
                ..WorkerStatus {
                    state: ObservedState::Failed,
                    recovery_blocked: true,
                    last_error: Some("previous guardian has not confirmed worker cleanup".into()),
                    ..status_for(
                        lease.id,
                        lease.project_id,
                        lease.profile_id,
                        lease.slot,
                        DesiredState::Stopped,
                    )
                }
            });
        }
        Ok(statuses)
    }

    pub fn logs(&self, id: ProjectId) -> Vec<OutputSummary> {
        self.projects
            .get(&id)
            .into_iter()
            .flat_map(|runtime| runtime.slots.iter())
            .flat_map(|slot| slot.output.iter().cloned())
            .collect()
    }

    pub fn tick(&mut self) -> Result<(), Error> {
        let store = Store::open(&self.db)?;
        for (&id, runtime) in &mut self.projects {
            for slot in &mut runtime.slots {
                slot.poll(&store)?;
            }
            if runtime.replacement.is_some()
                && runtime
                    .slots
                    .iter()
                    .all(|slot| slot.guardian.is_none() && !slot.status.recovery_blocked)
            {
                let plan = runtime.replacement.take().expect("replacement was checked");
                runtime.slots = plan
                    .slots
                    .into_iter()
                    .map(|(profile, index)| Slot::new(id, profile, index, plan.php.clone()))
                    .collect();
            }
            for slot in &mut runtime.slots {
                if slot.should_launch() {
                    slot.launch(&store, &self.binary, &self.db)?;
                }
            }
        }
        Ok(())
    }

    pub fn clean_previous(&self, store: &Store, id: ProjectId) -> Result<(), Error> {
        let owned = self.projects.get(&id);
        for lease in store.project_worker_leases(id)? {
            if lease_is_complete(&lease.state)
                && owned.is_none_or(|runtime| {
                    !runtime
                        .slots
                        .iter()
                        .any(|slot| slot.lease_id == Some(lease.id))
                })
            {
                store.release_worker(lease.id)?;
            }
        }
        Ok(())
    }
}

fn lease_is_complete(state: &WorkerLeaseState) -> bool {
    matches!(
        state,
        WorkerLeaseState::Exited { .. } | WorkerLeaseState::LaunchFailed
    )
}

impl ProjectRuntime {
    fn reserved_capacity(&self) -> usize {
        let current = self
            .slots
            .iter()
            .filter(|slot| {
                slot.guardian.is_some() || slot.status.desired_state == DesiredState::Running
            })
            .count();
        current.max(self.replacement.as_ref().map_or(0, |plan| plan.slots.len()))
    }

    fn stop_slots(&mut self) {
        for slot in &mut self.slots {
            slot.status.desired_state = DesiredState::Stopped;
            slot.retry_at = None;
            if let Some(guardian) = &mut slot.guardian {
                guardian.stop();
                slot.status.state = ObservedState::Stopping;
            } else if !slot.status.recovery_blocked {
                slot.status.state = ObservedState::Stopped;
            }
        }
    }
}

impl Slot {
    fn new(project: ProjectId, profile: WorkerProfile, index: u32, php: PathBuf) -> Self {
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
            guardian: None,
            lease_id: None,
            since: Instant::now(),
            retry_at: None,
            output: VecDeque::new(),
            stderr_seen: false,
        }
    }

    fn should_launch(&self) -> bool {
        self.guardian.is_none()
            && self.status.desired_state == DesiredState::Running
            && match self.status.state {
                ObservedState::Starting => true,
                ObservedState::Backoff => self.retry_at.is_some_and(|at| Instant::now() >= at),
                _ => false,
            }
    }

    fn launch(&mut self, store: &Store, binary: &Path, db: &Path) -> Result<(), Error> {
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

    fn poll(&mut self, store: &Store) -> Result<(), Error> {
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

fn status_for(
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

#[cfg(target_os = "macos")]
fn verify_horizon(project: &Project, allow: bool) -> Result<(), Error> {
    if allow {
        return Ok(());
    }
    for name in ["composer.json", "composer.lock"] {
        let file = match File::open(project.path.join(name)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => {
                return Err(Error::Invalid(
                    "could not check Composer metadata for Horizon",
                ));
            }
        };
        let input = file.take(8 * 1024 * 1024 + 1);
        let data: serde_json::Value = serde_json::from_reader(input)
            .map_err(|_| Error::Invalid("could not check Composer metadata for Horizon"))?;
        let declared = ["require", "require-dev"]
            .iter()
            .any(|key| data[key].get("laravel/horizon").is_some());
        let installed = ["packages", "packages-dev"].iter().any(|key| {
            data[key]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["name"] == "laravel/horizon"))
        });
        if declared || installed {
            return Err(Error::Conflict(
                "Horizon is listed in Composer metadata; explicit confirmation is required",
            ));
        }
    }
    Ok(())
}
