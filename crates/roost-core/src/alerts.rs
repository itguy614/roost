use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Revision, WorkerProfileId,
    metrics::{ProcessGeneration, ProcessSample},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    CpuPercent,
    RssBytes,
}

impl Metric {
    pub fn name(self) -> &'static str {
        match self {
            Self::CpuPercent => "cpu_percent",
            Self::RssBytes => "rss_bytes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "metric", rename_all = "snake_case", deny_unknown_fields)]
pub enum Rule {
    CpuPercent {
        clear_at: f64,
        breach_at: f64,
        #[serde(default)]
        restart: bool,
    },
    RssBytes {
        clear_at: u64,
        breach_at: u64,
        #[serde(default)]
        restart: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "metric", content = "value", rename_all = "snake_case")]
pub enum Evidence {
    CpuPercent(f64),
    RssBytes(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reading {
    Missing,
    Clear,
    Between,
    Breach,
}

impl Rule {
    pub fn metric(&self) -> Metric {
        match self {
            Self::CpuPercent { .. } => Metric::CpuPercent,
            Self::RssBytes { .. } => Metric::RssBytes,
        }
    }

    pub fn validate(&self) -> bool {
        match self {
            Self::CpuPercent {
                clear_at,
                breach_at,
                ..
            } => *clear_at >= 0.0 && clear_at < breach_at && breach_at.is_finite(),
            Self::RssBytes {
                clear_at,
                breach_at,
                ..
            } => clear_at < breach_at,
        }
    }

    pub fn restart(&self) -> bool {
        match self {
            Self::CpuPercent { restart, .. } | Self::RssBytes { restart, .. } => *restart,
        }
    }

    pub fn evidence(&self, sample: ProcessSample) -> Option<Evidence> {
        match self.metric() {
            Metric::CpuPercent => sample.cpu_percent.map(Evidence::CpuPercent),
            Metric::RssBytes => sample.rss_bytes.map(Evidence::RssBytes),
        }
    }

    pub fn reading(&self, sample: ProcessSample) -> Reading {
        fn classify<T: PartialOrd>(value: Option<T>, clear: T, breach: T) -> Reading {
            match value {
                None => Reading::Missing,
                Some(value) if value >= breach => Reading::Breach,
                Some(value) if value <= clear => Reading::Clear,
                Some(_) => Reading::Between,
            }
        }
        match *self {
            Self::CpuPercent {
                clear_at,
                breach_at,
                ..
            } => classify(sample.cpu_percent, clear_at, breach_at),
            Self::RssBytes {
                clear_at,
                breach_at,
                ..
            } => classify(sample.rss_bytes, clear_at, breach_at),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfiguredRule {
    pub profile_id: WorkerProfileId,
    pub revision: Revision,
    pub configured_at: u64,
    pub rule: Rule,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Episode {
    pub id: Uuid,
    pub identity: ProcessGeneration,
    pub rule: Rule,
    pub rule_revision: Revision,
    pub opened_at: u64,
    pub breach_observed_at: u64,
    pub breach_evidence: Evidence,
    pub actions_suppressed: bool,
    pub restart_charged: bool,
}
