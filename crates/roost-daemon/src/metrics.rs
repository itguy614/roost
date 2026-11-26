use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use roost_core::{
    metrics::{MetricSubject, PROCESS_SAMPLE_SECONDS, ProcessGeneration, ProcessSample},
    unix_timestamp,
};
use roost_process::{ProcessCounters, read_process_counters};
use roost_store::Store;

use crate::DaemonService;

const MAX_INTERVAL_NS: u64 = 6_000_000_000;

#[derive(Clone, Copy)]
pub(super) struct Observation {
    pub sample: ProcessSample,
    pub cpu_reason: Option<&'static str>,
    pub at: Instant,
}

pub(super) type Current = HashMap<ProcessGeneration, Observation>;

#[derive(Default, Clone, Copy)]
struct Baseline {
    previous: Option<ProcessCounters>,
    peak: Option<u64>,
}

impl Baseline {
    fn sample(
        &mut self,
        key: ProcessGeneration,
        counters: Option<ProcessCounters>,
        now: u64,
    ) -> Observation {
        let previous = std::mem::replace(&mut self.previous, counters);
        let (cpu_percent, cpu_reason) = calculate_cpu(previous, counters);
        if let Some(counters) = counters {
            self.peak = self.peak.max(Some(counters.rss_bytes));
        }
        Observation {
            at: Instant::now(),
            sample: ProcessSample {
                identity: key,
                observed_at: now,
                cpu_percent,
                rss_bytes: counters.map(|c| c.rss_bytes),
                uptime_seconds: counters.and_then(|c| c.uptime_seconds),
                peak_rss_bytes: self.peak,
            },
            cpu_reason,
        }
    }
}

fn calculate_cpu(
    previous: Option<ProcessCounters>,
    current: Option<ProcessCounters>,
) -> (Option<f64>, Option<&'static str>) {
    let Some(current) = current else {
        return (None, Some("process_unavailable"));
    };
    let Some(previous) = previous else {
        return (None, Some("first_sample"));
    };
    let elapsed = current
        .sampled_at_nanoseconds
        .saturating_sub(previous.sampled_at_nanoseconds);
    let awake_elapsed = current
        .awake_nanoseconds
        .saturating_sub(previous.awake_nanoseconds);
    if elapsed == 0
        || elapsed > MAX_INTERVAL_NS
        || elapsed.saturating_sub(awake_elapsed) > 50_000_000
    {
        return (None, Some("sampling_gap"));
    }
    match current
        .cpu_nanoseconds
        .checked_sub(previous.cpu_nanoseconds)
    {
        Some(cpu) => (Some(cpu as f64 * 100.0 / elapsed as f64), None),
        None => (None, Some("counter_reset")),
    }
}

#[derive(Default)]
struct Sampler {
    baselines: HashMap<ProcessGeneration, (MetricSubject, Baseline)>,
    next_maintenance: Option<Instant>,
    alerts: crate::alerts::Evaluator,
    alerts_failed: bool,
}

impl Sampler {
    fn tick(&mut self, service: &DaemonService) -> Result<(), ()> {
        let subjects = service.workers.lock().map_err(|_| ())?.metric_subjects();
        let now = unix_timestamp();
        let measurements: Vec<_> = subjects
            .into_iter()
            .map(|subject| (subject, read_process_counters(subject.process).ok()))
            .collect();
        let mut baselines = HashMap::new();
        let mut current = Current::new();
        {
            let supervisor = service.workers.lock().map_err(|_| ())?;
            let confirmed = supervisor.metric_subjects();
            for (subject, counters) in measurements {
                if !confirmed.contains(&subject) {
                    continue;
                }
                let mut baseline = self
                    .baselines
                    .get(&subject.key)
                    .filter(|(prior, _)| *prior == subject)
                    .map_or_else(Baseline::default, |(_, baseline)| *baseline);
                let sample = baseline.sample(subject.key, counters, now);
                baselines.insert(subject.key, (subject, baseline));
                current.insert(subject.key, sample);
            }
            *service.process_metrics.lock().map_err(|_| ())? = current.clone();
        }
        self.baselines = baselines;
        let maintain = self
            .next_maintenance
            .is_none_or(|next| Instant::now() >= next);
        if !current.is_empty() || maintain || self.alerts.has_open() {
            let subjects = self
                .baselines
                .values()
                .map(|(subject, _)| *subject)
                .collect::<Vec<_>>();
            let failed = self.alerts.tick(service, &subjects, &current).is_err();
            if failed && !self.alerts_failed {
                tracing::warn!("resource alert evaluation did not finish");
            }
            self.alerts_failed = failed;
            let store = Store::open_for_metric_writes(&service.db).map_err(|_| ())?;
            let rows: Vec<_> = current
                .values()
                .map(|observation| observation.sample)
                .collect();
            store.save_process_samples(&rows).map_err(|_| ())?;
            if maintain {
                store.maintain_process_metrics(now).map_err(|_| ())?;
                self.next_maintenance = Some(Instant::now() + Duration::from_secs(10));
            }
        }
        Ok(())
    }
}

impl DaemonService {
    pub(super) async fn poll_metrics(self: &Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(PROCESS_SAMPLE_SECONDS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sampler = Sampler::default();
        let mut failed = false;
        loop {
            interval.tick().await;
            let service = self.clone();
            let (next, success) = tokio::task::spawn_blocking(move || {
                let success = sampler.tick(&service).is_ok();
                (sampler, success)
            })
            .await
            .unwrap_or_default();
            sampler = next;
            if failed == success {
                failed = !success;
                if failed {
                    tracing::warn!("process metrics sampling or retention failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(cpu: u64, clock: u64) -> ProcessCounters {
        ProcessCounters {
            cpu_nanoseconds: cpu,
            sampled_at_nanoseconds: clock,
            awake_nanoseconds: clock,
            rss_bytes: 100,
            uptime_seconds: Some(1),
        }
    }

    #[test]
    fn cpu_requires_same_generation_stable_samples() {
        let initial = Some(counters(1_000_000_000, 1));
        assert_eq!(calculate_cpu(None, initial), (None, Some("first_sample")));
        assert_eq!(
            calculate_cpu(initial, Some(counters(4_000_000_000, 2_000_000_001))),
            (Some(150.0), None)
        );
        assert_eq!(
            calculate_cpu(initial, Some(counters(0, 2_000_000_001))),
            (None, Some("counter_reset"))
        );
        assert_eq!(
            calculate_cpu(initial, Some(counters(2_000_000_000, 30_000_000_001))),
            (None, Some("sampling_gap"))
        );
        let wake = ProcessCounters {
            awake_nanoseconds: 1_000_000_001,
            ..counters(2_000_000_000, 2_000_000_001)
        };
        assert_eq!(
            calculate_cpu(initial, Some(wake)),
            (None, Some("sampling_gap"))
        );
    }
}
