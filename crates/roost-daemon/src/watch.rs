use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use roost_core::{Group, Project, ProjectId, Revision, WatchRules};
use roost_ipc::watch::{SharedWatchStatus, WatchState};
use roost_store::Store;
use roost_watch::{MAX_PROJECTS, Poll, ProjectWatch, Rules};

use crate::DaemonService;

#[derive(Clone)]
pub(super) struct RuntimeStatus {
    pub revision: Revision,
    pub state: WatchState,
    pub pending: bool,
    pub problem: Option<&'static str>,
    pub shared: Vec<SharedWatchStatus>,
}

struct Active {
    revision: Revision,
    configured_at: Option<Revision>,
    path: PathBuf,
    rules: WatchRules,
    nested_roots: HashSet<PathBuf>,
    watch: ProjectWatch,
    changes: Changes,
}

#[derive(Clone, Copy, Default)]
struct Changes {
    invalidate_snapshot: bool,
    delivery: Option<Instant>,
}

impl Changes {
    fn observe(&mut self, observed: &Poll) {
        self.invalidate_snapshot |= observed.changed;
        self.delivery = self.delivery.max(observed.changed_at);
    }

    fn pending(self) -> bool {
        self.invalidate_snapshot || self.delivery.is_some()
    }

    fn merge(&mut self, changes: Self) {
        self.invalidate_snapshot |= changes.invalidate_snapshot;
        self.delivery = self.delivery.max(changes.delivery);
    }
}

fn watch_state(observed: &Poll) -> WatchState {
    if observed.problem.is_some() {
        WatchState::Degraded
    } else if observed.ready {
        WatchState::Watching
    } else {
        WatchState::Initializing
    }
}

impl Active {
    fn matches_scope(
        &self,
        project: &Project,
        configured_at: Option<Revision>,
        nested_roots: &HashSet<PathBuf>,
    ) -> bool {
        self.configured_at == configured_at
            && self.path == project.path
            && self.rules == project.watch_rules
            && &self.nested_roots == nested_roots
    }
}

#[derive(Default)]
struct Watches {
    active: HashMap<ProjectId, Active>,
    shared: shared::SharedWatches,
    status: HashMap<ProjectId, RuntimeStatus>,
    next_config: Option<Instant>,
}

impl DaemonService {
    pub(super) async fn poll_watches(self: &Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut watches = Watches::default();
        let mut failed = false;
        loop {
            interval.tick().await;
            let service = self.clone();
            let result = tokio::task::spawn_blocking(move || {
                let success = watches.tick(&service).is_ok();
                (watches, success)
            })
            .await;
            let success;
            (watches, success) = result.unwrap_or_default();
            if failed == success {
                failed = !success;
                if failed {
                    tracing::warn!("watch reconciliation could not complete");
                }
            }
        }
    }
}

impl Watches {
    fn tick(&mut self, service: &DaemonService) -> Result<(), crate::supervisor::Error> {
        let now = Instant::now();
        if self.next_config.is_none_or(|next| now >= next) {
            let store = Store::open(&service.db)?;
            let revisions = service
                .watch_config_revisions
                .lock()
                .map_err(|_| crate::supervisor::Error::Conflict("watch configuration unavailable"))?
                .clone();
            self.refresh(
                &store.list_projects()?,
                &store.list_groups()?,
                &revisions,
                now,
            );
            self.next_config = Some(now + Duration::from_millis(500));
        }
        for (&id, active) in &mut self.active {
            let observed = active.watch.poll(Instant::now(), &active.nested_roots);
            active.changes.observe(&observed);
            self.status.insert(
                id,
                RuntimeStatus {
                    revision: active.revision,
                    state: watch_state(&observed),
                    pending: observed.pending || active.changes.delivery.is_some(),
                    problem: observed.problem.map(|problem| problem.message()),
                    shared: Vec::new(),
                },
            );
        }
        for status in self.status.values_mut() {
            status.shared.clear();
        }
        self.shared.poll(&mut self.status);
        self.deliver(service)?;
        if let Ok(mut status) = service.watch_status.lock() {
            *status = self.status.clone();
        }
        Ok(())
    }

    fn refresh(
        &mut self,
        projects: &[Project],
        groups: &[Group],
        revisions: &HashMap<ProjectId, Revision>,
        now: Instant,
    ) {
        self.status.clear();
        let shared_scopes = shared::scopes(groups, projects, revisions);
        let mut configured = HashSet::new();
        for (index, project) in projects
            .iter()
            .filter(|project| project.watch_enabled)
            .enumerate()
        {
            if index >= MAX_PROJECTS {
                continue;
            }
            configured.insert(project.id);
            let nested_roots = boundaries(project, projects, &shared_scopes);
            let configured_at = revisions.get(&project.id).copied();
            if let Some(active) = self.active.get_mut(&project.id)
                && active.matches_scope(project, configured_at, &nested_roots)
            {
                active.revision = project.revision;
                continue;
            }
            self.active.remove(&project.id);
            match Rules::compile(&project.watch_rules) {
                Ok(rules) => {
                    self.active.insert(
                        project.id,
                        Active {
                            revision: project.revision,
                            configured_at,
                            path: project.path.clone(),
                            rules: project.watch_rules.clone(),
                            nested_roots,
                            watch: ProjectWatch::new(project.path.clone(), rules, now),
                            changes: Changes::default(),
                        },
                    );
                }
                Err(_) => {
                    self.status.insert(
                        project.id,
                        RuntimeStatus {
                            revision: project.revision,
                            state: WatchState::Degraded,
                            pending: false,
                            problem: Some("stored watch rules are invalid"),
                            shared: Vec::new(),
                        },
                    );
                }
            }
        }
        self.active.retain(|id, _| configured.contains(id));
        self.shared.refresh(shared_scopes, now);
    }

    fn deliver(&mut self, service: &DaemonService) -> Result<(), crate::supervisor::Error> {
        if !self.active.values().any(|active| active.changes.pending())
            && !self.shared.has_changes()
        {
            return Ok(());
        }
        let mut supervisor = service
            .workers
            .lock()
            .map_err(|_| crate::supervisor::Error::Conflict("supervisor unavailable"))?;
        let store = Store::open(&service.db)?;
        let projects = store.list_projects()?;
        let groups = store.list_groups()?;
        let revisions = service
            .watch_config_revisions
            .lock()
            .map_err(|_| crate::supervisor::Error::Conflict("watch configuration unavailable"))?;
        let shared_scopes = shared::scopes(&groups, &projects, &revisions);
        let mut cache = service
            .snapshots
            .lock()
            .map_err(|_| crate::supervisor::Error::Conflict("discovery cache unavailable"))?;
        let mut changes = HashMap::new();
        for project in projects.iter().filter(|project| project.watch_enabled) {
            if let Some(active) = self.active.get_mut(&project.id)
                && active.changes.pending()
                && active.matches_scope(
                    project,
                    revisions.get(&project.id).copied(),
                    &boundaries(project, &projects, &shared_scopes),
                )
            {
                changes.insert(project.id, std::mem::take(&mut active.changes));
            }
        }
        self.shared.collect(&shared_scopes, &mut changes);
        for (&id, change) in &changes {
            if change.invalidate_snapshot {
                cache.invalidate(id);
            }
        }
        drop(cache);
        for (id, change) in changes {
            if let Some(changed_at) = change.delivery {
                supervisor.restart_changed_workers(id, changed_at);
            }
        }
        Ok(())
    }
}

fn boundaries(
    project: &Project,
    projects: &[Project],
    shared: &HashMap<PathBuf, shared::Scope>,
) -> HashSet<PathBuf> {
    projects
        .iter()
        .map(|other| &other.path)
        .chain(shared.keys())
        .filter(|path| shared::excludes_from(&project.path, path))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn metadata_revision_preserves_a_batch_during_debounce() {
        struct Temp(PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let root = Temp(
            std::env::temp_dir().join(format!("roost-watch-refresh-{}", uuid::Uuid::new_v4())),
        );
        fs::create_dir(&root.0).unwrap();
        let artisan = root.0.join("artisan");
        fs::write(&artisan, "before").unwrap();
        let mut project = Project::register(&root.0).unwrap();
        project.watch_enabled = true;
        let id = project.id;
        let start = Instant::now();
        let mut watches = Watches::default();
        let revisions = HashMap::new();
        watches.refresh(std::slice::from_ref(&project), &[], &revisions, start);
        let active = watches.active.get_mut(&id).unwrap();
        let initial = active.watch.poll(start, &active.nested_roots);
        assert!(initial.ready && !initial.changed);

        fs::write(&artisan, "after").unwrap();
        let observed_at = start + Duration::from_secs(2);
        let observed = active.watch.poll(observed_at, &active.nested_roots);
        assert!(observed.changed && observed.pending && observed.changed_at.is_none());

        project.autostart = true;
        project.revision = Revision(2);
        let during_debounce = observed_at + Duration::from_millis(250);
        watches.refresh(
            std::slice::from_ref(&project),
            &[],
            &revisions,
            during_debounce,
        );
        let active = watches.active.get_mut(&id).unwrap();
        assert_eq!(active.revision, Revision(2));
        let observed = active.watch.poll(during_debounce, &active.nested_roots);
        assert!(observed.pending && observed.changed_at.is_none());
        let due = active.watch.poll(
            observed_at + Duration::from_millis(500),
            &active.nested_roots,
        );
        assert_eq!(due.changed_at, Some(observed_at));

        fs::write(artisan, "next").unwrap();
        let next_change = observed_at + Duration::from_secs(2);
        let observed = active.watch.poll(next_change, &active.nested_roots);
        assert!(observed.pending);
        project.watch_rules.include = vec!["config/**/*.php".into()];
        project.revision = Revision(3);
        let changed_rules = next_change + Duration::from_millis(50);
        watches.refresh(&[project], &[], &revisions, changed_rules);
        let active = watches.active.get_mut(&id).unwrap();
        let observed = active.watch.poll(changed_rules, &active.nested_roots);
        assert!(observed.ready && !observed.changed && !observed.pending);
    }
}
mod shared;
