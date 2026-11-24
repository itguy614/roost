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
use roost_providers::{Resolver, probe_queue_depths};
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
}

struct Entry {
    revision: Revision,
    token: String,
    ticket: Arc<()>,
    readings: HashMap<Selector, Metric>,
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
    }

    fn prepare(&mut self, project: &Project, token: &str, selectors: &[Selector]) -> Arc<()> {
        let entry = self
            .projects
            .entry(project.id)
            .or_insert_with(|| Entry::new(project, token));
        if !entry.matches(project, token) {
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
        }
    }

    fn matches(&self, project: &Project, token: &str) -> bool {
        self.revision == project.revision && self.token == token
    }
}

impl Metric {
    fn update(&mut self, reading: Reading, now: u64) {
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
                || matches!(reason, Problem::InvalidDefault | Problem::ConnectionMissing)
        }) {
            self.success = None;
            if matches!(
                reading.reason,
                Some(Problem::InvalidDefault | Problem::ConnectionMissing)
            ) {
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
                && !roost_process::probe_cleanup_pending()
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
    service
        .queue_metrics
        .lock()
        .map_err(|_| roost_store::Error::Schema)?
        .projects
        .retain(|id, _| projects.contains(id));
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
    let ticket = {
        let mut cache = service
            .queue_metrics
            .lock()
            .map_err(|_| roost_store::Error::Schema)?;
        if store.queue_probe_token(id)?.as_deref() != Some(&token) {
            return Ok(0);
        }
        cache.prepare(&project, &token, &selectors)
    };
    let result = probe_queue_depths(&Resolver::default(), &project, &batch, deadline);
    let readings = result.unwrap_or_else(|error| vec![Reading::failed(error); batch.len()]);
    let batch: Vec<_> = batch.into_iter().zip(readings).collect();
    let at = unix_timestamp();
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
    let stamp = QueueProbeStamp {
        project: id,
        revision: project.revision,
        token: &token,
        selectors: &selectors,
    };
    match store.save_queue_metrics(stamp, at, &batch) {
        Ok(true) => {
            for (selector, reading) in batch {
                entry
                    .readings
                    .entry(selector)
                    .or_default()
                    .update(reading, at);
            }
        }
        Ok(false) => {}
        Err(_) => tracing::warn!("queue metric batch could not commit"),
    }
    Ok(next)
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
