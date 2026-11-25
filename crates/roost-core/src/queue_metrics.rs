use serde::{Deserialize, Serialize};

pub const SELECTORS_PER_PROBE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub connection: String,
    pub queue: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Problem {
    DriverUnsupported,
    FrameworkUnsupported,
    CustomConnector,
    InvalidDefault,
    ConnectionMissing,
    BackendUnavailable,
    PhpUnavailable,
    ProjectUnavailable,
    Timeout,
    OutputLimit,
    InvalidOutput,
    CleanupUnconfirmed,
}

impl Problem {
    pub fn unsupported(self) -> bool {
        matches!(
            self,
            Self::DriverUnsupported | Self::FrameworkUnsupported | Self::CustomConnector
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reading {
    pub queue: Option<String>,
    pub pending: Option<u64>,
    pub reason: Option<Problem>,
}

impl Reading {
    pub fn failed(reason: Problem) -> Self {
        Self {
            queue: None,
            pending: None,
            reason: Some(reason),
        }
    }
}

/// Queue metrics use the same conservative identifier policy as discovery.
pub fn safe_identifier(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    !name.is_empty()
        && name.len() <= 96
        && (name.as_bytes()[0].is_ascii_alphanumeric() || name.starts_with('{'))
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:{}".contains(&byte))
        && !["ghp_", "github_pat_", "sk-", "xox", "akia", "asia", "eyj"]
            .iter()
            .any(|prefix| lower.starts_with(prefix))
}
