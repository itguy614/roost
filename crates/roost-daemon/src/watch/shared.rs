use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::Instant,
};

use roost_core::{Group, GroupId, Project, ProjectId, Revision, WatchRules};
use roost_ipc::watch::SharedWatchStatus;
use roost_watch::{ProjectWatch, Rules};

use super::{Changes, RuntimeStatus, watch_state};
use crate::group_rpc::MAX_SHARED_WATCHES;

#[derive(PartialEq, Eq)]
pub(super) struct Scope {
    group: GroupId,
    relative: PathBuf,
    rules: WatchRules,
    targets: HashMap<ProjectId, Option<Revision>>,
    exclusions: HashSet<PathBuf>,
}

pub(super) fn scopes(
    groups: &[Group],
    projects: &[Project],
    revisions: &HashMap<ProjectId, Revision>,
) -> HashMap<PathBuf, Scope> {
    let by_id: HashMap<_, _> = projects
        .iter()
        .map(|project| (project.id, project))
        .collect();
    let mut scopes = HashMap::new();
    for group in groups {
        let Some(root) = &group.root_path else {
            continue;
        };
        for mapping in &group.shared_paths {
            if scopes.len() >= MAX_SHARED_WATCHES {
                return scopes;
            }
            let path = root.join(&mapping.path);
            if projects.iter().any(|project| project.path == path) {
                continue;
            }
            let targets = mapping
                .projects
                .iter()
                .filter_map(|id| {
                    let project = by_id.get(id)?;
                    (project.watch_enabled
                        && project.group_id == Some(group.id)
                        && project.path.starts_with(root))
                    .then(|| (*id, revisions.get(id).copied()))
                })
                .collect();
            let exclusions = projects
                .iter()
                .filter(|project| project.path.starts_with(&path))
                .map(|project| project.path.clone())
                .collect();
            scopes.insert(
                path,
                Scope {
                    group: group.id,
                    relative: mapping.path.clone(),
                    rules: group.watch_rules.clone(),
                    targets,
                    exclusions,
                },
            );
        }
    }
    scopes
}

#[derive(Default)]
pub(super) struct SharedWatches {
    active: HashMap<PathBuf, Active>,
}

struct Active {
    scope: Scope,
    watch: ProjectWatch,
    changes: Changes,
}

impl SharedWatches {
    pub(super) fn refresh(&mut self, scopes: HashMap<PathBuf, Scope>, now: Instant) {
        self.active
            .retain(|root, active| scopes.get(root) == Some(&active.scope));
        for (root, scope) in scopes {
            if !scope.targets.is_empty()
                && !self.active.contains_key(&root)
                && let Ok(rules) = Rules::compile(&scope.rules)
            {
                self.active.insert(
                    root.clone(),
                    Active {
                        scope,
                        watch: ProjectWatch::new(root, rules, now),
                        changes: Changes::default(),
                    },
                );
            }
        }
    }

    pub(super) fn poll(&mut self, statuses: &mut HashMap<ProjectId, RuntimeStatus>) {
        for active in self.active.values_mut() {
            let observed = active.watch.poll(Instant::now(), &active.scope.exclusions);
            active.changes.observe(&observed);
            let status = SharedWatchStatus {
                group_id: active.scope.group,
                path: active.scope.relative.clone(),
                state: watch_state(&observed),
                pending: observed.pending || active.changes.delivery.is_some(),
                problem: observed.problem.map(|problem| problem.message().to_owned()),
            };
            for target in active.scope.targets.keys() {
                if let Some(project) = statuses.get_mut(target) {
                    project.shared.push(status.clone());
                }
            }
        }
    }

    pub(super) fn has_changes(&self) -> bool {
        self.active.values().any(|active| active.changes.pending())
    }

    pub(super) fn collect(
        &mut self,
        scopes: &HashMap<PathBuf, Scope>,
        targets: &mut HashMap<ProjectId, Changes>,
    ) {
        for (path, active) in &mut self.active {
            if active.changes.pending() && scopes.get(path) == Some(&active.scope) {
                let changes = std::mem::take(&mut active.changes);
                for &id in active.scope.targets.keys() {
                    targets.entry(id).or_default().merge(changes);
                }
            }
        }
    }
}

pub(super) fn excludes_from(project: &Path, path: &Path) -> bool {
    path != project && path.starts_with(project)
}
