use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use roost_core::{Project, ProjectId, Revision};
use roost_ipc::watch::WatchState;
use roost_store::Store;
use roost_watch::{MAX_PROJECTS, ProjectWatch, Rules};

use crate::DaemonService;

#[derive(Clone, Copy)]
pub(super) struct RuntimeStatus {
    pub revision: Revision,
    pub state: WatchState,
    pub pending: bool,
    pub problem: Option<&'static str>,
}

struct Active {
    revision: Revision,
    nested_roots: HashSet<PathBuf>,
    watch: ProjectWatch,
    delivery: Option<Instant>,
}

#[derive(Default)]
struct Watches {
    active: HashMap<ProjectId, Active>,
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
            self.refresh(&store.list_projects()?, now);
            self.next_config = Some(now + Duration::from_millis(500));
        }
        for (&id, active) in &mut self.active {
            let observed = active.watch.poll(Instant::now(), &active.nested_roots);
            if let Some(changed_at) = observed.changed_at {
                active.delivery = Some(changed_at);
            }
            let state = if observed.problem.is_some() {
                WatchState::Degraded
            } else if observed.ready {
                WatchState::Watching
            } else {
                WatchState::Initializing
            };
            self.status.insert(
                id,
                RuntimeStatus {
                    revision: active.revision,
                    state,
                    pending: observed.pending || active.delivery.is_some(),
                    problem: observed.problem.map(|problem| problem.message()),
                },
            );
        }
        self.deliver(service)?;
        if let Ok(mut status) = service.watch_status.lock() {
            *status = self.status.clone();
        }
        Ok(())
    }

    fn refresh(&mut self, projects: &[Project], now: Instant) {
        self.status.clear();
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
            let nested_roots = nested_roots(project, projects);
            if self.active.get(&project.id).is_some_and(|active| {
                active.revision == project.revision && active.nested_roots == nested_roots
            }) {
                continue;
            }
            self.active.remove(&project.id);
            match Rules::compile(&project.watch_rules) {
                Ok(rules) => {
                    self.active.insert(
                        project.id,
                        Active {
                            revision: project.revision,
                            nested_roots,
                            watch: ProjectWatch::new(project.path.clone(), rules, now),
                            delivery: None,
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
                        },
                    );
                }
            }
        }
        self.active.retain(|id, _| configured.contains(id));
    }

    fn deliver(&mut self, service: &DaemonService) -> Result<(), crate::supervisor::Error> {
        if !self.active.values().any(|active| active.delivery.is_some()) {
            return Ok(());
        }
        let mut supervisor = service
            .workers
            .lock()
            .map_err(|_| crate::supervisor::Error::Conflict("supervisor unavailable"))?;
        let projects = Store::open(&service.db)?.list_projects()?;
        for project in projects.iter().filter(|project| project.watch_enabled) {
            if let Some(active) = self.active.get_mut(&project.id)
                && let Some(changed_at) = active.delivery.take()
                && active.revision == project.revision
                && active.nested_roots == nested_roots(project, &projects)
            {
                supervisor.restart_changed_workers(project.id, changed_at);
            }
        }
        Ok(())
    }
}

fn nested_roots(project: &Project, projects: &[Project]) -> HashSet<PathBuf> {
    projects
        .iter()
        .filter(|other| other.id != project.id && other.path.starts_with(&project.path))
        .map(|other| other.path.clone())
        .collect()
}
