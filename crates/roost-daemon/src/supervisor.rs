use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use std::{
    fs::OpenOptions,
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
};

use roost_core::{
    DesiredState, ProjectId, WorkerProfile,
    worker_runtime::{
        LaunchIntent, ObservedState, OutputSummary, StopCause, WorkerLeaseState, WorkerStatus,
    },
};
use roost_store::Store;

mod scheduler;
mod slot;
use crate::supervised::SupervisedProfile;
use scheduler::Schedulers;
use slot::{Slot, status_for};

const MAX_ACTIVE: usize = 32;
const MAX_OUTPUT_SLOTS: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] roost_store::Error),
    #[error("{0}")]
    Invalid(&'static str),
    #[error("{0}")]
    Conflict(&'static str),
    #[error("process recovery is blocked until the previous guardian confirms cleanup")]
    RecoveryBlocked,
    #[error("daemon capacity is 32 managed process instances")]
    Capacity,
}

pub struct Supervisor {
    binary: PathBuf,
    db: PathBuf,
    projects: HashMap<ProjectId, ProjectRuntime>,
    closing: bool,
    schedulers: Schedulers,
    recovering: Vec<(ProjectId, LaunchIntent)>,
    recovery_errors: HashMap<ProjectId, String>,
    next_recovery: Instant,
}

struct ProjectRuntime {
    desired: DesiredState,
    replacement: Option<Plan>,
    slots: Vec<Slot>,
}

pub struct Plan {
    php: PathBuf,
    allow_horizon: bool,
    slots: Vec<(WorkerProfile, u32)>,
}

impl Plan {
    pub fn php(&self) -> &Path {
        &self.php
    }
}

impl Supervisor {
    pub fn new(
        binary: PathBuf,
        db: PathBuf,
        recovering: Vec<(ProjectId, LaunchIntent)>,
        recovering_schedulers: Vec<(ProjectId, PathBuf)>,
    ) -> Self {
        Self {
            binary,
            db,
            projects: HashMap::new(),
            closing: false,
            schedulers: Schedulers::new(recovering_schedulers),
            recovering,
            recovery_errors: HashMap::new(),
            next_recovery: Instant::now(),
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
        if self.desired(id) == DesiredState::Running
            || self.recovering.iter().any(|(project, _)| *project == id)
            || !store
                .project_leases::<roost_core::WorkerProfileId>(id)?
                .is_empty()
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
        for lease in store.project_leases::<roost_core::WorkerProfileId>(id)? {
            if lease.state.is_complete() {
                continue;
            }
            if runtime.is_none_or(|runtime| {
                !runtime.slots.iter().any(|slot| {
                    slot.has_guardian()
                        && slot.lease_id == Some(lease.id)
                        && !matches!(lease.state, WorkerLeaseState::Uncertain { .. })
                })
            }) {
                return Err(Error::RecoveryBlocked);
            }
        }
        if !restart && self.desired(id) == DesiredState::Running {
            return Ok(None);
        }
        if !cfg!(target_os = "macos") {
            return Err(Error::Invalid(
                "worker supervision is currently supported on macOS",
            ));
        }
        verify_horizon(&project, allow_horizon)?;
        let mut slots = Vec::new();
        let mut executable = None;
        for profile in store
            .list_worker_profiles(id)?
            .into_iter()
            .filter(|p| p.enabled && p.desired_instances > 0)
        {
            let php = roost_process::validate_worker(&project, &profile, php).map_err(|_| {
                Error::Invalid("project, worker profile, or PHP executable failed validation")
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
        if other_count + slots.len() + self.schedulers.reserved_capacity(None) > MAX_ACTIVE {
            return Err(Error::Capacity);
        }
        Ok(Some(Plan {
            php,
            allow_horizon,
            slots,
        }))
    }

    pub fn cancel_recovery(&mut self, id: ProjectId) {
        self.recovering.retain(|(project, _)| *project != id);
        self.recovery_errors.remove(&id);
    }

    fn recover_projects(&mut self, store: &Store) {
        self.next_recovery = Instant::now() + Duration::from_secs(1);
        for (id, launch) in std::mem::take(&mut self.recovering) {
            let result = self
                .clean_previous(store, id)
                .and_then(|()| self.prepare(store, id, &launch.php, launch.allow_horizon, false));
            match result {
                Ok(Some(plan)) => self.apply_start(id, plan),
                Ok(None) => {}
                Err(error @ (Error::RecoveryBlocked | Error::Capacity)) => {
                    self.recovery_errors.insert(id, error.to_string());
                    self.recovering.push((id, launch));
                }
                Err(Error::Store(_)) => {
                    self.recovery_errors
                        .insert(id, "could not read worker recovery state".into());
                    self.recovering.push((id, launch));
                }
                Err(error) => {
                    self.recovery_errors.insert(id, error.to_string());
                }
            }
        }
    }

    pub fn apply_start(&mut self, id: ProjectId, plan: Plan) {
        self.cancel_recovery(id);
        let runtime = self.projects.entry(id).or_insert_with(|| ProjectRuntime {
            desired: DesiredState::Stopped,
            replacement: None,
            slots: Vec::new(),
        });
        runtime.stop_slots(StopCause::ProjectRestart);
        runtime.desired = DesiredState::Running;
        runtime.replacement = Some(plan);
        runtime.install_replacement(id);
    }

    pub fn apply_stop(&mut self, id: ProjectId) {
        self.cancel_recovery(id);
        if let Some(project) = self.projects.get_mut(&id) {
            project.desired = DesiredState::Stopped;
            project.replacement = None;
            project.stop_slots(StopCause::ProjectStop);
        }
    }

    pub(crate) fn restart_changed_workers(&mut self, id: ProjectId, changed_at: Instant) {
        if !self.closing
            && let Some(runtime) = self.projects.get_mut(&id)
            && runtime.desired == DesiredState::Running
            && runtime.replacement.is_none()
        {
            for slot in &mut runtime.slots {
                slot.request_code_restart(changed_at);
            }
        }
    }

    pub fn remove(&mut self, id: ProjectId) {
        self.cancel_recovery(id);
        self.projects.remove(&id);
    }

    pub fn shutdown(&mut self) {
        self.closing = true;
        self.schedulers.shutdown();
        self.recovering.clear();
        for runtime in self.projects.values_mut() {
            runtime.replacement = None;
            runtime.desired = DesiredState::Stopped;
            runtime.stop_slots(StopCause::DaemonShutdown);
        }
    }

    pub fn has_children(&self) -> bool {
        self.schedulers.has_children()
            || self
                .projects
                .values()
                .any(|project| project.slots.iter().any(|slot| slot.has_guardian()))
    }

    pub fn status(&self, store: &Store, id: ProjectId) -> Result<Vec<WorkerStatus>, Error> {
        let mut statuses: Vec<_> = self
            .projects
            .get(&id)
            .into_iter()
            .flat_map(|runtime| runtime.slots.iter().map(Slot::status))
            .collect();
        let desired = store.project_intent(id)?.desired();
        for lease in store.project_leases::<roost_core::WorkerProfileId>(id)? {
            if lease.state.is_complete()
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
                WorkerLeaseState::Uncertain { identity } => identity,
                _ => None,
            };
            statuses.push(WorkerStatus {
                identity,
                ..WorkerStatus {
                    state: ObservedState::Failed,
                    recovery_blocked: true,
                    last_error: Some("previous guardian has not confirmed worker cleanup".into()),
                    reason: Some("previous guardian has not confirmed worker cleanup".into()),
                    ..status_for(
                        lease.id,
                        lease.project_id,
                        lease.profile_id,
                        lease.slot,
                        desired,
                    )
                }
            });
        }
        if statuses.is_empty()
            && let Some(error) = self.recovery_errors.get(&id)
        {
            for profile in store
                .list_worker_profiles(id)?
                .into_iter()
                .filter(|p| p.enabled && p.desired_instances > 0)
            {
                statuses.push(WorkerStatus {
                    state: ObservedState::Failed,
                    reason: Some(error.clone()),
                    last_error: Some(error.clone()),
                    ..status_for(profile.id.0, id, profile.id, 0, desired)
                });
            }
        }
        Ok(statuses)
    }

    pub fn logs(&self, id: ProjectId) -> Vec<OutputSummary> {
        self.projects
            .get(&id)
            .into_iter()
            .flat_map(|runtime| runtime.slots.iter())
            .flat_map(Slot::logs)
            .collect()
    }

    pub fn tick(&mut self) -> Result<(), Error> {
        let pending = self.projects.values().any(|runtime| {
            runtime.replacement.is_some()
                || runtime
                    .slots
                    .iter()
                    .any(|slot| slot.has_guardian() || slot.should_launch())
        });
        let recover_now = !self.recovering.is_empty() && Instant::now() >= self.next_recovery;
        if !pending && !recover_now && !self.schedulers.pending() {
            return Ok(());
        }
        let store = Store::open(&self.db)?;
        if recover_now {
            self.recover_projects(&store);
        }
        self.recover_schedulers(&store);
        let mut outcome = Ok(());
        for (&id, runtime) in &mut self.projects {
            for slot in &mut runtime.slots {
                if let Err(error) = slot.poll(&store) {
                    outcome = Err(error);
                }
            }
            runtime.install_replacement(id);
            for slot in &mut runtime.slots {
                if slot.should_launch()
                    && let Err(error) = slot.launch(&store, &self.binary, &self.db)
                {
                    outcome = Err(error);
                }
            }
        }
        trim_slots(
            self.projects
                .values_mut()
                .flat_map(|runtime| &mut runtime.slots),
        );
        if let Err(error) = self.schedulers.tick(&store, &self.binary, &self.db) {
            outcome = Err(error);
        }
        outcome
    }

    pub fn clean_previous(&self, store: &Store, id: ProjectId) -> Result<(), Error> {
        let owned = self.projects.get(&id);
        for lease in store.project_leases::<roost_core::WorkerProfileId>(id)? {
            if lease.state.is_complete()
                && owned.is_none_or(|runtime| {
                    !runtime
                        .slots
                        .iter()
                        .any(|slot| slot.lease_id == Some(lease.id))
                })
            {
                store.finish_previous_process(&lease)?;
            }
        }
        Ok(())
    }
}

fn trim_slots<'a, P: SupervisedProfile + 'a>(slots: impl Iterator<Item = &'a mut Slot<P>>) {
    let mut retained: Vec<_> = slots
        .filter_map(|slot| slot.output_priority().map(|priority| (priority, slot)))
        .collect();
    if retained.len() > MAX_OUTPUT_SLOTS {
        retained.sort_unstable_by_key(|(priority, _)| *priority);
        let excess = retained.len() - MAX_OUTPUT_SLOTS;
        for (_, slot) in retained.into_iter().take(excess) {
            slot.discard_output();
        }
    }
}

impl ProjectRuntime {
    fn install_replacement(&mut self, id: ProjectId) {
        if self
            .slots
            .iter()
            .any(|slot| slot.has_guardian() || slot.recovery_blocked())
        {
            return;
        }
        if let Some(plan) = self.replacement.take() {
            self.slots = plan
                .slots
                .into_iter()
                .map(|(profile, index)| {
                    Slot::new(id, profile, index, plan.php.clone(), plan.allow_horizon)
                })
                .collect();
        }
    }

    fn reserved_capacity(&self) -> usize {
        let current = self
            .slots
            .iter()
            .filter(|slot| slot.has_guardian() || slot.desired() == DesiredState::Running)
            .count();
        current.max(self.replacement.as_ref().map_or(0, |plan| plan.slots.len()))
    }

    fn stop_slots(&mut self, cause: StopCause) {
        for slot in &mut self.slots {
            slot.stop(cause);
        }
    }
}

fn verify_horizon(project: &roost_core::Project, allow: bool) -> Result<(), Error> {
    if allow {
        return Ok(());
    }
    for name in ["composer.json", "composer.lock"] {
        const MAX_METADATA_BYTES: u64 = 8 * 1024 * 1024;
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(project.path.join(name))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => {
                return Err(Error::Invalid(
                    "could not check Composer metadata for Horizon",
                ));
            }
        };
        if !file
            .metadata()
            .is_ok_and(|meta| meta.is_file() && meta.len() <= MAX_METADATA_BYTES)
        {
            return Err(Error::Invalid(
                "Composer metadata must be a regular file of at most 8 MiB",
            ));
        }
        let input = file.take(MAX_METADATA_BYTES + 1);
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
