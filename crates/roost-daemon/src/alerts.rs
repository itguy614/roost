use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use roost_core::{
    Revision, WorkerProfileId,
    alerts::{ConfiguredRule, Episode, Metric, Reading},
    metrics::{MetricSubject, ProcessGeneration, ProcessKind},
    unix_timestamp,
};
use roost_store::Store;
use uuid::Uuid;

use crate::{
    DaemonService,
    metrics::{Current, Observation},
    supervisor::Supervisor,
};

const FRESHNESS: Duration = Duration::from_secs(6);
const STREAK_INTERVAL: Duration = Duration::from_millis(3500);

#[derive(Default)]
pub(super) struct Evaluator {
    states: HashMap<(ProcessGeneration, Metric), State>,
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
        store: &Store,
        subjects: &[MetricSubject],
        samples: &Current,
    ) -> Result<(), ()> {
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
        for (key, (subject, rule)) in selected {
            let state = self
                .states
                .entry(key)
                .or_insert_with(|| State::new(rule.revision));
            if state.revision != rule.revision {
                *state = State::new(rule.revision);
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
                    state.current = open(service, store, subject, rule, sample)?;
                }
                state.streak = Streak::default();
            }
        }
        Ok(())
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
) -> Result<Option<Uuid>, ()> {
    let mut supervisor = service.workers.lock().map_err(|_| ())?;
    let Some(slot) = supervisor
        .threshold_target(store, subject)
        .map_err(|_| ())?
    else {
        return Ok(None);
    };
    if !fresh(sample)
        || !store
            .alert_rules(&[rule.profile_id])
            .map_err(|_| ())?
            .iter()
            .any(|current| current.revision == rule.revision && current.rule == rule.rule)
    {
        return Ok(None);
    }
    let Some(breach_evidence) = rule.rule.evidence(sample.sample) else {
        return Ok(None);
    };
    let episode = store
        .open_alert(
            Episode {
                id: Uuid::new_v4(),
                identity: subject.key,
                rule: rule.rule.clone(),
                rule_revision: rule.revision,
                opened_at: unix_timestamp(),
                breach_observed_at: sample.sample.observed_at,
                breach_evidence,
                actions_suppressed: false,
                restart_charged: false,
            },
            true,
        )
        .map_err(|_| ())?;
    if episode.restart_charged && fresh(sample) {
        Supervisor::threshold_restart(slot);
    }
    Ok(Some(episode.id))
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
}
