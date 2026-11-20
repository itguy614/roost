use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use roost_core::{
    DesiredState, ProjectId,
    scheduler::{SchedulerProfile, SchedulerProfileId, SchedulerStatus},
    worker_runtime::{ObservedState, OutputSummary, StopCause, WorkerLeaseState},
};
use roost_store::Store;

use super::{
    Error, MAX_ACTIVE, Supervisor,
    slot::{Slot, status_for},
    trim_slots,
};

pub struct SchedulerPlan {
    pub php: PathBuf,
    profile: SchedulerProfile,
}

struct SchedulerRuntime {
    desired: DesiredState,
    slot: Slot<SchedulerProfile>,
    replacement: Option<SchedulerPlan>,
}

pub(super) struct Schedulers {
    projects: HashMap<ProjectId, SchedulerRuntime>,
    recovering: Vec<(ProjectId, PathBuf)>,
    recovery_errors: HashMap<ProjectId, String>,
    next_recovery: Instant,
}

impl Schedulers {
    pub(super) fn new(recovering: Vec<(ProjectId, PathBuf)>) -> Self {
        Self {
            projects: HashMap::new(),
            recovering,
            recovery_errors: HashMap::new(),
            next_recovery: Instant::now(),
        }
    }

    pub(super) fn reserved_capacity(&self, except: Option<ProjectId>) -> usize {
        self.projects
            .iter()
            .filter(|(id, runtime)| {
                except != Some(**id)
                    && (runtime.desired == DesiredState::Running || runtime.slot.has_guardian())
            })
            .count()
    }

    pub(super) fn has_children(&self) -> bool {
        self.projects
            .values()
            .any(|runtime| runtime.slot.has_guardian())
    }

    pub(super) fn shutdown(&mut self) {
        self.recovering.clear();
        for runtime in self.projects.values_mut() {
            runtime.desired = DesiredState::Stopped;
            runtime.replacement = None;
            runtime.slot.stop(StopCause::DaemonShutdown);
        }
    }

    pub(super) fn pending(&self) -> bool {
        self.projects.values().any(|runtime| {
            runtime.replacement.is_some()
                || runtime.slot.has_guardian()
                || runtime.slot.should_launch()
        }) || self.recovery_due()
    }

    fn recovery_due(&self) -> bool {
        !self.recovering.is_empty() && Instant::now() >= self.next_recovery
    }

    pub(super) fn tick(&mut self, store: &Store, binary: &Path, db: &Path) -> Result<(), Error> {
        let mut outcome = Ok(());
        for (&id, runtime) in &mut self.projects {
            if let Err(error) = runtime.slot.poll(store) {
                outcome = Err(error);
            }
            if !runtime.slot.has_guardian()
                && !runtime.slot.recovery_blocked()
                && let Some(plan) = runtime.replacement.take()
            {
                runtime.slot = new_slot(id, plan);
            }
            if runtime.slot.should_launch()
                && let Err(error) = runtime.slot.launch(store, binary, db)
            {
                outcome = Err(error);
            }
        }
        trim_slots(self.projects.values_mut().map(|runtime| &mut runtime.slot));
        outcome
    }

    fn cancel_recovery(&mut self, id: ProjectId) {
        self.recovering.retain(|(project, _)| *project != id);
        self.recovery_errors.remove(&id);
    }
}

impl Supervisor {
    pub(super) fn recover_schedulers(&mut self, store: &Store) {
        if !self.schedulers.recovery_due() {
            return;
        }
        self.schedulers.next_recovery = Instant::now() + Duration::from_secs(1);
        for (id, php) in std::mem::take(&mut self.schedulers.recovering) {
            let result = self
                .clean_previous_scheduler(store, id)
                .and_then(|()| self.prepare_scheduler(store, id, &php, false));
            match result {
                Ok(Some(plan)) => self.start_scheduler(id, plan),
                Ok(None) => {}
                Err(error @ (Error::RecoveryBlocked | Error::Capacity)) => {
                    self.schedulers
                        .recovery_errors
                        .insert(id, error.to_string());
                    self.schedulers.recovering.push((id, php));
                }
                Err(Error::Store(_)) => {
                    self.schedulers
                        .recovery_errors
                        .insert(id, "could not read scheduler recovery state".into());
                    self.schedulers.recovering.push((id, php));
                }
                Err(error) => {
                    self.schedulers
                        .recovery_errors
                        .insert(id, error.to_string());
                }
            }
        }
    }

    pub(crate) fn prepare_scheduler(
        &self,
        store: &Store,
        id: ProjectId,
        php: &Path,
        restart: bool,
    ) -> Result<Option<SchedulerPlan>, Error> {
        self.validate_mutation()?;
        let project = store.get_project(id)?.ok_or(roost_store::Error::NotFound)?;
        let profile = store
            .project_scheduler(id)?
            .ok_or(Error::Conflict("configure a scheduler before starting it"))?;
        let current = self.schedulers.projects.get(&id);
        for lease in store.project_leases::<SchedulerProfileId>(id)? {
            if !lease.state.is_complete()
                && current.is_none_or(|runtime| {
                    runtime.slot.lease_id != Some(lease.id)
                        || !runtime.slot.has_guardian()
                        || matches!(lease.state, WorkerLeaseState::Uncertain { .. })
                })
            {
                return Err(Error::RecoveryBlocked);
            }
        }
        if !restart && current.is_some_and(|runtime| runtime.desired == DesiredState::Running) {
            return Ok(None);
        }
        if !cfg!(target_os = "macos") {
            return Err(Error::Invalid(
                "scheduler supervision is currently supported on macOS",
            ));
        }
        if !profile.enabled {
            return Err(Error::Conflict("the scheduler profile is disabled"));
        }
        let php = roost_process::validate_scheduler(&project, &profile, php).map_err(|_| {
            Error::Invalid("project, scheduler profile, or PHP executable failed validation")
        })?;
        let used = self
            .projects
            .values()
            .map(super::ProjectRuntime::reserved_capacity)
            .sum::<usize>()
            + self.schedulers.reserved_capacity(Some(id));
        if used >= MAX_ACTIVE {
            return Err(Error::Capacity);
        }
        Ok(Some(SchedulerPlan { php, profile }))
    }

    pub(crate) fn start_scheduler(&mut self, id: ProjectId, plan: SchedulerPlan) {
        self.schedulers.cancel_recovery(id);
        if let Some(runtime) = self.schedulers.projects.get_mut(&id)
            && runtime.slot.has_guardian()
        {
            runtime.slot.stop(StopCause::ProjectRestart);
            runtime.desired = DesiredState::Running;
            runtime.replacement = Some(plan);
        } else {
            self.schedulers.projects.insert(
                id,
                SchedulerRuntime {
                    desired: DesiredState::Running,
                    slot: new_slot(id, plan),
                    replacement: None,
                },
            );
        }
    }

    pub(crate) fn stop_scheduler(&mut self, id: ProjectId) {
        self.schedulers.cancel_recovery(id);
        if let Some(runtime) = self.schedulers.projects.get_mut(&id) {
            runtime.desired = DesiredState::Stopped;
            runtime.replacement = None;
            runtime.slot.stop(StopCause::ProjectStop);
        }
    }

    pub(crate) fn cancel_scheduler_recovery(&mut self, id: ProjectId) {
        self.schedulers.cancel_recovery(id);
    }

    pub(crate) fn remove_scheduler_runtime(&mut self, id: ProjectId) {
        self.schedulers.cancel_recovery(id);
        self.schedulers.projects.remove(&id);
    }

    pub(crate) fn scheduler_clear_for_change(
        &self,
        store: &Store,
        id: ProjectId,
    ) -> Result<(), Error> {
        self.validate_mutation()?;
        if self
            .schedulers
            .projects
            .get(&id)
            .is_some_and(|runtime| runtime.desired == DesiredState::Running)
            || self
                .schedulers
                .recovering
                .iter()
                .any(|(project, _)| *project == id)
            || !store.project_leases::<SchedulerProfileId>(id)?.is_empty()
        {
            return Err(Error::Conflict(
                "stop the scheduler and wait for cleanup first",
            ));
        }
        Ok(())
    }

    pub(crate) fn clean_previous_scheduler(
        &self,
        store: &Store,
        id: ProjectId,
    ) -> Result<(), Error> {
        let current = self.schedulers.projects.get(&id);
        for lease in store.project_leases::<SchedulerProfileId>(id)? {
            if lease.state.is_complete()
                && current.is_none_or(|runtime| runtime.slot.lease_id != Some(lease.id))
            {
                store.finish_previous_process(&lease)?;
            }
        }
        Ok(())
    }

    pub(crate) fn scheduler_status(
        &self,
        store: &Store,
        id: ProjectId,
    ) -> Result<SchedulerStatus, Error> {
        let profile = store.project_scheduler(id)?;
        let desired_state = store.scheduler_intent(id)?.desired();
        let mut process = self
            .schedulers
            .projects
            .get(&id)
            .map(|runtime| runtime.slot.status());
        if process.is_none()
            && let Some(lease) = store
                .project_leases::<SchedulerProfileId>(id)?
                .into_iter()
                .find(|lease| !lease.state.is_complete())
        {
            let mut status = status_for(lease.id, id, lease.profile_id, 0, desired_state);
            status.identity = match lease.state {
                WorkerLeaseState::Running { identity } => Some(identity),
                WorkerLeaseState::Uncertain { identity } => identity,
                _ => None,
            };
            status.state = ObservedState::Failed;
            status.recovery_blocked = true;
            status.reason = Some("previous guardian has not confirmed scheduler cleanup".into());
            status.last_error = status.reason.clone();
            process = Some(status);
        }
        if process.is_none()
            && let Some(error) = self.schedulers.recovery_errors.get(&id)
            && let Some(profile) = &profile
        {
            let mut status = status_for(profile.id.0, id, profile.id, 0, desired_state);
            status.state = ObservedState::Failed;
            status.reason = Some(error.clone());
            status.last_error = status.reason.clone();
            process = Some(status);
        }
        Ok(SchedulerStatus {
            profile,
            desired_state,
            process,
        })
    }

    pub(crate) fn scheduler_logs(&self, id: ProjectId) -> Vec<OutputSummary> {
        self.schedulers
            .projects
            .get(&id)
            .into_iter()
            .flat_map(|runtime| runtime.slot.logs())
            .collect()
    }
}

fn new_slot(id: ProjectId, plan: SchedulerPlan) -> Slot<SchedulerProfile> {
    Slot::new(id, plan.profile, 0, plan.php, false)
}
