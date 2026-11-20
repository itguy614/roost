use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use std::{
    fs::OpenOptions,
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
};

use roost_core::{
    DesiredState, ProjectId, WorkerProfile,
    worker_runtime::{ObservedState, OutputSummary, WorkerLeaseState, WorkerStatus},
};
use roost_store::Store;

mod slot;
use slot::{Slot, status_for};

const MAX_ACTIVE: usize = 32;

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
    allow_horizon: bool,
    slots: Vec<(WorkerProfile, u32)>,
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
                    slot.has_guardian()
                        && slot.lease_id == Some(lease.id)
                        && !matches!(lease.state, WorkerLeaseState::Uncertain { .. })
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
        if other_count + slots.len() > MAX_ACTIVE {
            return Err(Error::Conflict("daemon capacity is 32 worker instances"));
        }
        Ok(Some(Plan {
            php,
            allow_horizon,
            slots,
        }))
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
        runtime.install_replacement(id);
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
            .any(|project| project.slots.iter().any(|slot| slot.has_guardian()))
    }

    pub fn status(&self, store: &Store, id: ProjectId) -> Result<Vec<WorkerStatus>, Error> {
        let mut statuses: Vec<_> = self
            .projects
            .get(&id)
            .into_iter()
            .flat_map(|runtime| runtime.slots.iter().map(Slot::status))
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
                WorkerLeaseState::Uncertain { identity } => identity,
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
        if !pending {
            return Ok(());
        }
        let store = Store::open(&self.db)?;
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
        outcome
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

    fn stop_slots(&mut self) {
        for slot in &mut self.slots {
            slot.stop();
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
