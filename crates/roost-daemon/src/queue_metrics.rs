mod php;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use roost_core::{
    Project, ProjectId, Revision,
    queue_metrics::{Problem, Reading, SELECTORS_PER_PROBE, Selector},
    unix_timestamp,
};
use roost_process::ProbeCleanup;
use roost_providers::{Resolver, probe_queue_depths, resolve_queue_php};
use roost_store::{Store, queue_metrics::QueueProbeStamp};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::DaemonService;

const ROUND: Duration = Duration::from_secs(10);
const SLOT: Duration = Duration::from_secs(2);
const SLOTS: usize = 2;
const ADMISSIONS: usize = 8;
const STALE: u64 = 20;

#[derive(Default)]
pub(super) struct Current {
    projects: HashMap<ProjectId, Entry>,
    php: php::Cache,
}

struct Entry {
    revision: Revision,
    token: String,
    ticket: Arc<()>,
    readings: HashMap<Selector, Metric>,
    files: Option<php::ProjectFiles>,
}

#[derive(Default)]
struct Metric {
    failure: Option<Problem>,
    effective_queue: Option<String>,
    success: Option<(u64, u64)>,
}

impl Current {
    pub(super) fn invalidate(&mut self, id: ProjectId) {
        self.projects.remove(&id);
        self.php.take(id);
    }

    pub(super) fn invalidate_token(&mut self, id: ProjectId, token: &str) {
        if self
            .projects
            .get(&id)
            .is_some_and(|entry| entry.token == token)
        {
            self.invalidate(id);
        }
    }

    fn prepare(&mut self, project: &Project, token: &str, selectors: &[Selector]) -> Arc<()> {
        let entry = self
            .projects
            .entry(project.id)
            .or_insert_with(|| Entry::new(project, token));
        if !entry.matches(project, token) {
            self.php.take(project.id);
            *entry = Entry::new(project, token);
        }
        entry
            .readings
            .retain(|selector, _| selectors.binary_search(selector).is_ok());
        entry.ticket.clone()
    }

    pub(super) fn render(
        &self,
        project: &Project,
        token: &str,
        selectors: &[Selector],
        now: u64,
    ) -> Vec<Value> {
        let readings = self
            .projects
            .get(&project.id)
            .filter(|entry| entry.matches(project, token))
            .map(|entry| &entry.readings);
        selectors.iter().map(|selector| {
            let reading = readings.and_then(|readings| readings.get(selector));
            let pending = reading.map_or(json!({"state":"unknown","value":null,"reason":"awaiting_probe"}),
                |r| r.render(now));
            let queue = reading.and_then(|reading| reading.effective_queue.as_ref()).or(selector.queue.as_ref());
            json!({"connection":selector.connection,"queue":queue,"requested_queue":selector.queue,"pending":pending})
        }).collect()
    }
}

impl Entry {
    fn new(project: &Project, token: &str) -> Self {
        Self {
            revision: project.revision,
            token: token.into(),
            ticket: Arc::new(()),
            readings: HashMap::new(),
            files: None,
        }
    }

    fn matches(&self, project: &Project, token: &str) -> bool {
        self.revision == project.revision && self.token == token
    }
}

impl Metric {
    fn attribute(&self, reading: &mut Reading) {
        if reading.queue.is_none() && reading.reason.is_some_and(temporary) {
            reading.queue = self.effective_queue.clone();
        }
    }

    fn update(&mut self, reading: Reading, now: u64) {
        let resolved = reading.queue.is_some();
        if let Some(queue) = reading.queue {
            if self
                .effective_queue
                .as_ref()
                .is_some_and(|previous| *previous != queue)
            {
                self.success = None;
            }
            self.effective_queue = Some(queue);
        }
        if let Some(count) = reading.pending {
            self.success = Some((count, now));
        }
        if reading.reason.is_some_and(|reason| {
            reason.unsupported()
                || matches!(
                    reason,
                    Problem::InvalidDefault
                        | Problem::ConnectionMissing
                        | Problem::ProjectUnavailable
                )
        }) {
            self.success = None;
            if !resolved {
                self.effective_queue = None;
            }
        }
        self.failure = reading.reason;
    }

    fn render(&self, now: u64) -> Value {
        if let Some(reason) = self.failure.filter(|r| r.unsupported()) {
            return json!({"state":"unsupported","value":null,"reason":reason});
        }
        match (self.success, self.failure) {
            (Some((count, at)), None) if now.saturating_sub(at) < STALE => {
                json!({"state":"fresh","value":count,"observed_at":super::metrics_rpc::timestamp(at)})
            }
            (Some((count, at)), failure) => {
                json!({"state":"stale","value":null,"reason": failure.map_or_else(|| json!("capacity"), |reason| json!(reason)),
                       "last_known":{"value":count,"observed_at":super::metrics_rpc::timestamp(at)}})
            }
            (None, reason) => {
                json!({"state":"unknown","value":null,"reason":reason.map_or_else(|| json!("awaiting_probe"), |r| json!(r))})
            }
        }
    }
}

fn temporary(reason: Problem) -> bool {
    matches!(
        reason,
        Problem::BackendUnavailable
            | Problem::PhpUnavailable
            | Problem::Timeout
            | Problem::OutputLimit
            | Problem::InvalidOutput
            | Problem::CleanupUnconfirmed
    )
}

#[derive(Default)]
struct Fair {
    order: VecDeque<ProjectId>,
    visited: HashSet<ProjectId>,
}

impl Fair {
    fn round(&mut self, projects: Vec<ProjectId>) {
        let current: HashSet<_> = projects.iter().copied().collect();
        self.order.retain(|id| current.contains(id));
        let previous: HashSet<_> = self.order.iter().copied().collect();
        self.order
            .extend(projects.into_iter().filter(|id| !previous.contains(id)));
        self.visited.clear();
    }

    fn next(&mut self, occupied: &HashSet<ProjectId>) -> Option<ProjectId> {
        let index = self
            .order
            .iter()
            .position(|id| !self.visited.contains(id) && !occupied.contains(id))?;
        let id = self.order.remove(index)?;
        self.order.push_back(id);
        self.visited.insert(id);
        Some(id)
    }
}

impl DaemonService {
    pub(super) async fn poll_queue_metrics(self: &Arc<Self>) {
        let mut fair = Fair::default();
        let mut offsets = HashMap::new();
        let mut running = JoinSet::new();
        let mut active = HashMap::new();
        let mut end = tokio::time::Instant::now();
        let mut admitted = ADMISSIONS;
        loop {
            let occupied = active.values().copied().collect();
            while running.len() < SLOTS
                && admitted < ADMISSIONS
                && end.saturating_duration_since(tokio::time::Instant::now()) >= SLOT
                && roost_process::probe_cleanup_status() == ProbeCleanup::Ready
            {
                let Some(id) = fair.next(&occupied) else {
                    break;
                };
                let service = self.clone();
                let deadline = Instant::now() + SLOT;
                let offset = offsets.get(&id).copied().unwrap_or(0);
                let task =
                    running.spawn_blocking(move || (id, run_batch(&service, id, offset, deadline)));
                active.insert(task.id(), id);
                admitted += 1;
            }
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(end) => {
                    end = tokio::time::Instant::now() + ROUND;
                    admitted = 0;
                    let service = self.clone();
                    let projects = tokio::task::spawn_blocking(move || queue_round(&service)).await;
                    let Ok(Ok(projects)) = projects else {
                        tracing::warn!("queue probe round could not load projects");
                        fair.round(Vec::new());
                        continue;
                    };
                    offsets.retain(|id, _| projects.contains(id));
                    fair.round(projects);
                }
                () = tokio::time::sleep(Duration::from_millis(250)),
                    if roost_process::probe_cleanup_status() == ProbeCleanup::Retrying => {
                    let _ = tokio::task::spawn_blocking(roost_process::retry_probe_cleanup).await;
                }
                Some(joined) = running.join_next_with_id(), if !running.is_empty() => {
                    match joined {
                        Ok((task, (id, offset))) => {
                            active.remove(&task);
                            if let Ok(offset) = offset {
                                offsets.insert(id, offset);
                            } else {
                                tracing::warn!("queue polling batch could not finish");
                            }
                        }
                        Err(error) => {
                            active.remove(&error.id());
                            tracing::warn!("queue probe task failed");
                        }
                    }
                }
            }
        }
    }
}

fn queue_round(service: &DaemonService) -> Result<Vec<ProjectId>, roost_store::Error> {
    let store = Store::open_for_metric_writes(&service.db)?;
    let projects = store.queue_probe_projects()?;
    if !projects.is_empty() || store.has_queue_metrics()? {
        // A busy retention write should not prevent the scheduler from admitting probes.
        if store.maintain_queue_metrics(unix_timestamp()).is_err() {
            tracing::warn!("queue metric retention could not commit");
        }
    }
    let mut cache = service
        .queue_metrics
        .lock()
        .map_err(|_| roost_store::Error::Schema)?;
    cache.projects.retain(|id, _| projects.contains(id));
    cache.php.retain(|id| projects.contains(&id));
    Ok(projects)
}

fn rotated(selectors: &[Selector], offset: usize) -> (Vec<Selector>, usize) {
    let count = selectors.len().min(SELECTORS_PER_PROBE);
    let start = offset % selectors.len();
    (
        selectors
            .iter()
            .cycle()
            .skip(start)
            .take(count)
            .cloned()
            .collect(),
        (start + count) % selectors.len(),
    )
}

fn run_batch(
    service: &DaemonService,
    id: ProjectId,
    offset: usize,
    deadline: Instant,
) -> Result<usize, roost_store::Error> {
    let store = Store::open_for_metric_writes(&service.db)?;
    let Some(project) = store.get_project(id)? else {
        return Ok(0);
    };
    let Some(token) = store.queue_probe_token(id)? else {
        return Ok(0);
    };
    let selectors = store.queue_probe_selectors(id)?;
    if selectors.is_empty() {
        return Ok(0);
    }
    let (batch, next) = rotated(&selectors, offset);
    let files = php::ProjectFiles::read(&project);
    let (ticket, cached) = {
        let mut cache = service
            .queue_metrics
            .lock()
            .map_err(|_| roost_store::Error::Schema)?;
        let ticket = cache.prepare(&project, &token, &selectors);
        let entry = cache.projects.get_mut(&id).expect("prepared queue project");
        if entry
            .files
            .replace(files.clone())
            .is_some_and(|previous| previous != files)
        {
            // The readings live longer than an evicted executable cache
            // entry. Keep the same configuration fence for both.
            entry.readings.clear();
        }
        (ticket, cache.php.take(id))
    };
    let changed = cached.as_ref().is_some_and(|cached| cached.changed(&files));
    let resolver = Resolver::default();
    let mut old = cached;
    let mut resolved_cache = None;
    let resolved = if !changed && old.as_ref().is_some_and(|c| c.fresh(Instant::now())) {
        resolved_cache = old.take();
        Ok(resolved_cache
            .as_ref()
            .expect("fresh resolution")
            .php
            .clone())
    } else {
        resolve_queue_php(&resolver, &project, deadline).inspect(|resolved| {
            resolved_cache = php::Resolved::new(resolved.clone(), files.clone(), Instant::now());
        })
    };
    let changed = changed
        || old
            .as_ref()
            .zip(resolved_cache.as_ref())
            .is_some_and(|(old, new)| !old.same_runtime(new));
    if changed {
        let mut cache = service
            .queue_metrics
            .lock()
            .map_err(|_| roost_store::Error::Schema)?;
        if let Some(entry) = cache
            .projects
            .get_mut(&id)
            .filter(|entry| Arc::ptr_eq(&ticket, &entry.ticket))
        {
            entry.readings.clear();
        }
    }
    if store.queue_probe_token(id)?.as_deref() != Some(&token) {
        return Ok(next);
    }
    let result =
        resolved.and_then(|php| probe_queue_depths(&resolver, &project, &php, &batch, deadline));
    if matches!(
        result,
        Err(Problem::PhpUnavailable | Problem::ProjectUnavailable)
    ) {
        resolved_cache = None;
    }
    let readings = result.unwrap_or_else(|error| vec![Reading::failed(error); batch.len()]);
    let mut batch: Vec<_> = batch.into_iter().zip(readings).collect();
    let at = unix_timestamp();
    let files_changed = php::ProjectFiles::read(&project) != files;
    {
        let mut cache = service
            .queue_metrics
            .lock()
            .map_err(|_| roost_store::Error::Schema)?;
        let Some(entry) = cache
            .projects
            .get_mut(&id)
            .filter(|entry| Arc::ptr_eq(&ticket, &entry.ticket))
        else {
            return Ok(next);
        };
        if files_changed {
            cache.invalidate(id);
            return Ok(next);
        }
        for (selector, reading) in &mut batch {
            if let Some(metric) = entry.readings.get(selector) {
                metric.attribute(reading);
            }
        }
        if let Some(resolved) = resolved_cache {
            cache.php.put(id, resolved);
        }
    }
    let stamp = QueueProbeStamp {
        project: id,
        revision: project.revision,
        token: &token,
        selectors: &selectors,
    };
    publish(service, &store, stamp, &ticket, at, batch);
    Ok(next)
}

fn publish(
    service: &DaemonService,
    store: &Store,
    stamp: QueueProbeStamp<'_>,
    ticket: &Arc<()>,
    at: u64,
    batch: Vec<(Selector, Reading)>,
) {
    let id = stamp.project;
    let write = match store.stage_queue_metrics(stamp, at, &batch) {
        Ok(Some(write)) => write,
        Ok(None) => return,
        Err(_) => {
            tracing::warn!("queue metric batch could not stage");
            return;
        }
    };
    let Ok(mut cache) = service.queue_metrics.lock() else {
        return;
    };
    let Some(entry) = cache
        .projects
        .get_mut(&id)
        .filter(|entry| Arc::ptr_eq(ticket, &entry.ticket))
    else {
        return;
    };
    // Staging may wait for SQLite without holding this lock. COMMIT uses a
    // zero busy timeout to keep the watcher and current-read lock bounded.
    if write.commit().is_ok() {
        for (selector, reading) in batch {
            entry
                .readings
                .entry(selector)
                .or_default()
                .update(reading, at);
        }
    } else {
        tracing::warn!("queue metric batch could not commit");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_invalidation_revokes_only_its_project_ticket_and_readings() {
        let root = std::env::temp_dir().join(format!("roost-queue-cache-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("artisan"), "").unwrap();
        let project = Project::register(&root).unwrap();
        std::fs::remove_dir_all(root).unwrap();
        let unrelated = Project {
            id: ProjectId(uuid::Uuid::new_v4()),
            ..project.clone()
        };
        let selectors = [Selector {
            connection: "redis".into(),
            queue: Some("default".into()),
        }];
        let mut cache = Current::default();
        let ticket = cache.prepare(&project, "token", &selectors);
        let other = cache.prepare(&unrelated, "token", &selectors);
        cache
            .projects
            .get_mut(&project.id)
            .unwrap()
            .readings
            .insert(
                selectors[0].clone(),
                Metric {
                    success: Some((1, 100)),
                    ..Metric::default()
                },
            );
        assert_eq!(
            cache.render(&project, "token", &selectors, 101)[0]["pending"]["value"],
            1
        );
        cache.invalidate(project.id);
        assert!(!Arc::ptr_eq(
            &ticket,
            &cache.prepare(&project, "token", &selectors)
        ));
        assert!(Arc::ptr_eq(
            &other,
            &cache.prepare(&unrelated, "token", &selectors)
        ));
        assert_eq!(
            cache.render(&project, "token", &selectors, 101)[0]["pending"]["reason"],
            "awaiting_probe"
        );
    }

    #[test]
    fn rounds_rotate_nine_projects_and_keep_busy_projects_eligible() {
        let projects: Vec<_> = (1..=9)
            .map(|i| ProjectId(uuid::Uuid::from_u128(i)))
            .collect();
        let mut fair = Fair::default();
        fair.round(projects.clone());
        let occupied = HashSet::from([projects[0]]);
        assert_eq!(
            (0..8)
                .map(|_| fair.next(&occupied).unwrap())
                .collect::<Vec<_>>(),
            projects[1..]
        );
        fair.round(projects.clone());
        assert_eq!(fair.next(&HashSet::new()), Some(projects[0]));
        assert!((0..8).all(|_| fair.next(&HashSet::new()).is_some()));
        assert_eq!(fair.next(&HashSet::new()), None);
    }

    #[test]
    fn rotates_selectors_and_keeps_last_success_when_a_probe_times_out() {
        let selectors: Vec<_> = (0..65)
            .map(|i| Selector {
                connection: "database".into(),
                queue: Some(i.to_string()),
            })
            .collect();
        let (first, offset) = rotated(&selectors, 0);
        let (second, offset) = rotated(&selectors, offset);
        let (third, _) = rotated(&selectors, offset);
        assert_eq!(first, selectors[..32]);
        assert_eq!(second, selectors[32..64]);
        assert_eq!(third[0], selectors[64]);
        let mut reading = Metric::default();
        reading.update(
            Reading {
                queue: Some("jobs".into()),
                pending: Some(0),
                reason: None,
            },
            100,
        );
        assert_eq!(reading.render(119)["value"], 0);
        assert_eq!(reading.render(120)["reason"], "capacity");
        reading.update(Reading::failed(Problem::Timeout), 109);
        assert_eq!(reading.render(109)["state"], "stale");
        assert_eq!(reading.render(109)["last_known"]["value"], 0);
    }
}
