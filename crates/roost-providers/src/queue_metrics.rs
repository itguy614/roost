use std::time::{Duration, Instant};

use roost_core::{
    Project,
    queue_metrics::{Problem, Reading, SELECTORS_PER_PROBE, Selector, safe_identifier},
};
use roost_process::ProbeError;

use crate::{Error, Resolver};

pub fn probe_queue_depths(
    resolver: &Resolver,
    project: &Project,
    selectors: &[Selector],
    slot_deadline: Instant,
) -> Result<Vec<Reading>, Problem> {
    if selectors.is_empty()
        || selectors.len() > SELECTORS_PER_PROBE
        || selectors.iter().any(|s| {
            !safe_identifier(&s.connection)
                || s.queue.as_deref().is_some_and(|q| !safe_identifier(q))
        })
    {
        return Err(Problem::InvalidOutput);
    }
    // Reserve the tail of the slot for termination and group cleanup. Herd's
    // lookup and PHP execution share the same absolute slot deadline.
    let execution_deadline = slot_deadline - Duration::from_millis(300);
    let php = resolver
        .resolve_before(project, None, None, execution_deadline)
        .map_err(|error| match error {
            Error::HerdProbe(ProbeError::Timeout) => Problem::Timeout,
            Error::HerdProbe(ProbeError::Cleanup) => Problem::CleanupUnconfirmed,
            _ => Problem::PhpUnavailable,
        })?;
    if !project.path.join("vendor/autoload.php").is_file()
        || !project.path.join("bootstrap/app.php").is_file()
    {
        return Err(Problem::PhpUnavailable);
    }
    let input = serde_json::to_string(selectors).map_err(|_| Problem::InvalidOutput)?;
    let mut command = resolver.command(&php.path, &project.path);
    command.args([
        "-d",
        "memory_limit=128M",
        "-r",
        include_str!("queue_metrics.php")
            .strip_prefix("<?php\n")
            .expect("embedded probe starts with a PHP opening tag"),
        "--",
        &input,
    ]);
    let output = roost_process::run_probe_before(
        command,
        &resolver.environment(),
        execution_deadline,
        slot_deadline,
        16 * 1024,
    )
    .map_err(probe_problem)?;
    parse(&output, selectors)
}

fn probe_problem(error: ProbeError) -> Problem {
    match error {
        ProbeError::Timeout => Problem::Timeout,
        ProbeError::Cleanup => Problem::CleanupUnconfirmed,
        ProbeError::OutputLimit => Problem::OutputLimit,
        _ => Problem::BackendUnavailable,
    }
}

fn parse(bytes: &[u8], selectors: &[Selector]) -> Result<Vec<Reading>, Problem> {
    let results: Vec<Reading> =
        serde_json::from_slice(bytes).map_err(|_| Problem::InvalidOutput)?;
    if results.len() != selectors.len()
        || results.iter().zip(selectors).any(|(reading, selector)| {
            reading.pending.is_some() == reading.reason.is_some()
                || reading.pending.is_some_and(|p| p > i64::MAX as u64)
                || reading.pending.is_some() && reading.queue.is_none()
                || reading.queue.as_deref().is_some_and(|queue| {
                    !safe_identifier(queue)
                        || selector
                            .queue
                            .as_deref()
                            .is_some_and(|asked| queue != asked)
                })
                || reading.reason.is_some_and(|reason| {
                    !matches!(
                        reason,
                        Problem::DriverUnsupported
                            | Problem::FrameworkUnsupported
                            | Problem::InvalidDefault
                            | Problem::ConnectionMissing
                            | Problem::BackendUnavailable
                            | Problem::CustomConnector
                    )
                })
        })
    {
        return Err(Problem::InvalidOutput);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unvalidated_output_and_preserves_a_measured_zero() {
        let selectors = [Selector {
            connection: "database".into(),
            queue: Some("app".into()),
        }];
        assert_eq!(
            parse(
                br#"[{"queue":"app","pending":0,"reason":null}]"#,
                &selectors
            )
            .unwrap()[0]
                .pending,
            Some(0)
        );
        for input in [
            r#"[{"queue":"other","pending":0,"reason":null}]"#,
            r#"[{"queue":"app","pending":0,"reason":null,"extra":"secret"}]"#,
            r#"[{"queue":null,"pending":1,"reason":null}]"#,
            r#"[{"queue":"app","pending":0,"reason":"timeout"}]"#,
            r#"[{"queue":"app","pending":null,"reason":"cleanup_unconfirmed"}]"#,
            r#"[{"queue":"app","pending":0,"reason":null}] diagnostic"#,
        ] {
            assert_eq!(
                parse(input.as_bytes(), &selectors),
                Err(Problem::InvalidOutput)
            );
        }
    }
}
