use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use roost_core::{
    Revision, WorkerProfileId,
    alerts::{ConfiguredRule, Episode, Metric, Reading, RestartDeferred},
    metrics::{MetricSubject, ProcessGeneration, ProcessKind},
    unix_timestamp,
};
use roost_store::Store;
use uuid::Uuid;

use crate::{
    DaemonService,
    alerts_clock::Clock,
    metrics::{Current, Observation},
    supervisor::Supervisor,
};

const FRESHNESS: Duration = Duration::from_secs(6);
const STREAK_INTERVAL: Duration = Duration::from_millis(3500);

#[derive(Default)]
pub(super) struct Evaluator {
    states: HashMap<(ProcessGeneration, Metric), State>,
    clock: Option<Clock>,
}

struct State {
    revision: Revision,
    streak: Streak,
    current: Option<Uuid>,
    previous: Option<Instant>,
}

#[derive(Default)]
struct Streak(u8);

impl Streak {
    fn reached(&mut self, active: bool, reading: Reading) -> bool {
        let wanted = if active {
            Reading::Clear
        } else {
            Reading::Breach
        };
        self.0 = if reading == wanted {
            self.0.saturating_add(1)
        } else {
            0
        };
        self.0 >= 3
    }
}

impl Evaluator {
    pub(super) fn has_open(&self) -> bool {
        self.states.values().any(|state| state.current.is_some())
    }

    pub(super) fn tick(
        &mut self,
        service: &DaemonService,
        subjects: &[MetricSubject],
        samples: &Current,
    ) -> Result<(), ()> {
        let result = self.evaluate(service, subjects, samples);
        if result.is_err() {
            self.reset_streaks();
        }
        result
    }

    fn reset_streaks(&mut self) {
        for state in self.states.values_mut() {
            state.streak = Streak::default();
            state.previous = None;
        }
    }

    fn evaluate(
        &mut self,
        service: &DaemonService,
        subjects: &[MetricSubject],
        samples: &Current,
    ) -> Result<(), ()> {
        let store = Store::open_without_busy_wait(&service.db).map_err(|_| ())?;
        if self.clock.is_none() {
            self.clock = Some(Clock::new(store.alert_elapsed().map_err(|_| ())?));
        }
        let clock = self.clock.as_mut().ok_or(())?;
        let eligibility = clock.sample(unix_timestamp(), Instant::now());
        let profiles: Vec<_> = subjects
            .iter()
            .filter(|subject| subject.key.kind == ProcessKind::Worker)
            .map(|subject| WorkerProfileId(subject.key.profile_id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let rules = if profiles.is_empty() {
            Vec::new()
        } else {
            store.alert_rules(&profiles).map_err(|_| ())?
        };
        let mut selected = HashMap::new();
        for subject in subjects
            .iter()
            .filter(|subject| subject.key.kind == ProcessKind::Worker)
        {
            for rule in rules
                .iter()
                .filter(|rule| rule.profile_id.0 == subject.key.profile_id)
            {
                selected.insert((subject.key, rule.rule.metric()), (*subject, rule));
            }
        }

        let now = unix_timestamp();
        let obsolete: Vec<_> = self
            .states
            .keys()
            .copied()
            .filter(|key| !selected.contains_key(key))
            .collect();
        for key in obsolete {
            let state = &self.states[&key];
            if let Some(id) = state.current {
                store
                    .close_alert(id, now, "generation_ended")
                    .map_err(|_| ())?;
            }
            self.states.remove(&key);
        }
        let mut selected: Vec<_> = selected.into_iter().collect();
        selected.sort_unstable_by_key(|((generation, metric), _)| {
            (
                generation.project_id.0,
                generation.profile_id,
                generation.slot,
                metric.name(),
            )
        });
        let mut restarts: Vec<Restart<'_>> = Vec::new();
        for (key, (subject, rule)) in selected {
            let state = self
                .states
                .entry(key)
                .or_insert_with(|| State::new(rule.revision));
            if state.revision != rule.revision {
                *state = State::new(rule.revision);
            }
            if !eligibility.verified {
                state.streak = Streak::default();
                state.previous = None;
                continue;
            }
            let sample = samples
                .get(&key.0)
                .copied()
                .filter(|sample| fresh(*sample) && sample.sample.observed_at > rule.configured_at);
            if state
                .previous
                .is_some_and(|previous| previous.elapsed() >= STREAK_INTERVAL)
            {
                state.streak = Streak::default();
            }
            state.previous = sample.map(|sample| sample.at);
            let reading =
                sample.map_or(Reading::Missing, |sample| rule.rule.reading(sample.sample));
            if state.streak.reached(state.current.is_some(), reading) {
                if let Some(id) = state.current {
                    store.close_alert(id, now, "recovered").map_err(|_| ())?;
                    state.current = None;
                } else if let Some(sample) = sample {
                    let sibling_charged = restarts.iter().any(|action| action.subject == subject);
                    if let Some(episode) = open(
                        service,
                        &store,
                        subject,
                        rule,
                        sample,
                        eligibility.elapsed,
                        sibling_charged,
                    )? {
                        state.current = Some(episode.id);
                        if episode.restart_charged {
                            restarts.push(Restart {
                                subject,
                                rule,
                                sample,
                            });
                        }
                    }
                }
                state.streak = Streak::default();
            }
        }
        dispatch(service, &store, clock, restarts)
    }
}

impl State {
    fn new(revision: Revision) -> Self {
        Self {
            revision,
            streak: Streak::default(),
            current: None,
            previous: None,
        }
    }
}

pub(super) fn fresh(sample: Observation) -> bool {
    sample.at.elapsed() < FRESHNESS
        && unix_timestamp().saturating_sub(sample.sample.observed_at) < FRESHNESS.as_secs()
        && sample.cpu_reason != Some("sampling_gap")
}

fn open(
    service: &DaemonService,
    store: &Store,
    subject: MetricSubject,
    rule: &ConfiguredRule,
    sample: Observation,
    elapsed: u64,
    sibling_charged: bool,
) -> Result<Option<Episode>, ()> {
    let Some(breach_evidence) = rule.rule.evidence(sample.sample) else {
        return Ok(None);
    };
    let episode = Episode {
        id: Uuid::new_v4(),
        identity: subject.key,
        rule: rule.rule.clone(),
        rule_revision: rule.revision,
        opened_at: unix_timestamp(),
        breach_observed_at: sample.sample.observed_at,
        breach_evidence,
        actions_suppressed: false,
        restart_charged: false,
        restart_deferred: (sibling_charged && rule.rule.restart())
            .then_some(RestartDeferred::OtherMetric),
    };
    let Some(write) = store.stage_alert(episode, elapsed).map_err(|_| ())? else {
        return Ok(None);
    };
    // SQLite owns a write reservation. A supervisor operation could already
    // be attempting a write, so never wait for its lock from this transaction.
    let mut supervisor = service.workers.try_lock().map_err(|_| ())?;
    if !fresh(sample)
        || supervisor
            .threshold_target(store, subject)
            .map_err(|_| ())?
            .is_none()
    {
        return Ok(None);
    }
    write.commit().map(Some).map_err(|_| ())
}

struct Restart<'a> {
    subject: MetricSubject,
    rule: &'a ConfiguredRule,
    sample: Observation,
}

fn dispatch(
    service: &DaemonService,
    store: &Store,
    clock: &Clock,
    restarts: Vec<Restart<'_>>,
) -> Result<(), ()> {
    if restarts.is_empty() {
        return Ok(());
    }
    let mut supervisor = service.workers.lock().map_err(|_| ())?;
    let mut confirmed = Vec::new();
    // Finish all fallible reads before signalling the first guardian. A failed
    // cycle keeps committed charges and dispatches none of its planned actions.
    for action in restarts {
        if fresh(action.sample)
            && supervisor
                .threshold_target(store, action.subject)
                .map_err(|_| ())?
                .is_some()
            && store
                .alert_rules(&[action.rule.profile_id])
                .map_err(|_| ())?
                .iter()
                .any(|rule| rule.revision == action.rule.revision && rule.rule == action.rule.rule)
        {
            confirmed.push(action);
        }
    }
    if !clock.can_dispatch(unix_timestamp(), Instant::now()) {
        return Err(());
    }
    for action in confirmed {
        if fresh(action.sample)
            && let Some(slot) = supervisor.threshold_slot(action.subject)
        {
            Supervisor::threshold_restart(slot);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_three_consecutive_readings_in_each_direction() {
        let mut streak = Streak::default();
        for input in [
            Reading::Breach,
            Reading::Breach,
            Reading::Missing,
            Reading::Breach,
            Reading::Breach,
        ] {
            assert!(!streak.reached(false, input));
        }
        assert!(streak.reached(false, Reading::Breach));
        streak = Streak::default();
        for input in [
            Reading::Clear,
            Reading::Clear,
            Reading::Between,
            Reading::Clear,
            Reading::Clear,
        ] {
            assert!(!streak.reached(true, input));
        }
        assert!(streak.reached(true, Reading::Clear));
    }
    #[test]
    fn failed_cycle_discards_breach_and_recovery_without_erasing_committed_state() {
        use roost_core::ProjectId;
        let identity = ProcessGeneration {
            project_id: ProjectId(Uuid::new_v4()),
            kind: ProcessKind::Worker,
            profile_id: Uuid::new_v4(),
            slot: 0,
            instance_id: Uuid::new_v4(),
            generation: 1,
        };
        let active_id = Uuid::new_v4();
        let mut evaluator = Evaluator::default();
        for (metric, current) in [
            (Metric::CpuPercent, None),
            (Metric::RssBytes, Some(active_id)),
        ] {
            evaluator.states.insert(
                (identity, metric),
                State {
                    revision: Revision(1),
                    streak: Streak(2),
                    current,
                    previous: Some(Instant::now()),
                },
            );
        }
        evaluator.reset_streaks();
        for ((_, metric), state) in &mut evaluator.states {
            assert!(state.previous.is_none());
            assert_eq!(
                state.current,
                (*metric == Metric::RssBytes).then_some(active_id)
            );
            let reading = if state.current.is_some() {
                Reading::Clear
            } else {
                Reading::Breach
            };
            assert!(!state.streak.reached(state.current.is_some(), reading));
            assert!(!state.streak.reached(state.current.is_some(), reading));
            assert!(state.streak.reached(state.current.is_some(), reading));
        }
    }
}
