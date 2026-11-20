use std::{ffi::OsString, process::Command, time::Duration};

#[cfg(target_os = "macos")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_os = "macos")]
static CLEANUP_PENDING: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("could not launch or read the probe process")]
    Io(#[from] std::io::Error),
    #[error("probe exceeded its time limit")]
    Timeout,
    #[error("probe exceeded its output limit")]
    OutputLimit,
    #[error("probe exited unsuccessfully")]
    Exit,
    #[error("probe cleanup could not be confirmed")]
    Cleanup,
    #[error("runtime probes are currently supported on macOS")]
    Unsupported,
}

/// Run a short read-only-by-contract probe in an owned process group.
/// Only stdout is retained. Stderr counts toward the same output limit.
#[cfg(target_os = "macos")]
pub fn run_probe(
    mut command: Command,
    environment: &[(&str, OsString)],
    duration: Duration,
    limit: usize,
) -> Result<Vec<u8>, ProbeError> {
    use std::{process::Stdio, thread, time::Instant};

    // Refuse new probes while a previously killed child is still unreaped.
    // The daemon separately limits in-flight probes, bounding cleanup threads.
    if CLEANUP_PENDING.load(Ordering::Acquire) != 0 {
        return Err(ProbeError::Cleanup);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = crate::spawn_session(command, Some(environment))?;
    let pid = child.id() as libc::pid_t;
    // spawn_in_session only returns after setsid and exec succeed. The child
    // PID stays pinned until we reap it, including if its leader exits first.
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let mut waited = false;
    let result = (|| {
        nonblocking(&stdout)?;
        nonblocking(&stderr)?;
        let until = Instant::now() + duration;
        let mut output = Vec::new();
        let mut bytes = 0;
        let mut exited = false;
        loop {
            if !exited && crate::macos::exited(pid)? {
                // Stop background helpers still in the group before final pipe reads.
                terminate_group(pid, true)?;
                exited = true;
            }
            let stdout_empty = read_pass(&mut stdout, &mut bytes, limit, Some(&mut output))?;
            let stderr_empty = read_pass(&mut stderr, &mut bytes, limit, None)?;
            if exited && stdout_empty && stderr_empty {
                waited = true;
                let status = child.wait()?;
                return if status.success() {
                    Ok(output)
                } else {
                    Err(ProbeError::Exit)
                };
            }
            if Instant::now() >= until {
                return Err(ProbeError::Timeout);
            }
            if stdout_empty && stderr_empty {
                thread::sleep(Duration::from_millis(10));
            }
        }
    })();

    if result.is_err() && !waited {
        // No normal wait occurred on this path. Check custody before signaling.
        let cleanup = crate::macos::exited(pid)
            .map_err(|_| ProbeError::Cleanup)
            .and_then(|exited| terminate_group(pid, exited));
        if cleanup.is_err() {
            return defer_reap(child);
        }
        let until = Instant::now() + Duration::from_millis(250);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < until => thread::sleep(Duration::from_millis(10)),
                _ => return defer_reap(child),
            }
        }
    }
    result
}

#[cfg(target_os = "macos")]
fn defer_reap(mut child: std::process::Child) -> Result<Vec<u8>, ProbeError> {
    // No further signals. If cleanup cannot be proved, keep discovery disabled.
    CLEANUP_PENDING.fetch_add(1, Ordering::AcqRel);
    let _ = std::thread::Builder::new()
        .name("roost-probe-reaper".into())
        .spawn(move || {
            if child.wait().is_ok() {
                CLEANUP_PENDING.fetch_sub(1, Ordering::AcqRel);
            }
        });
    Err(ProbeError::Cleanup)
}

#[cfg(target_os = "macos")]
fn terminate_group(pid: libc::pid_t, exited: bool) -> Result<(), ProbeError> {
    let group_empty = || {
        crate::macos::group_has_live_members(pid)
            .map(|live| !live)
            .map_err(|_| ProbeError::Cleanup)
    };
    if exited && group_empty()? {
        return Ok(());
    }
    match crate::macos::send_group(pid, libc::SIGKILL) {
        Ok(()) => Ok(()),
        // The child can exit between waitid and kill, with no live group left.
        Err(_) if crate::macos::exited(pid).is_ok_and(|exited| exited) && group_empty()? => Ok(()),
        Err(_) => Err(ProbeError::Cleanup),
    }
}

#[cfg(not(target_os = "macos"))]
pub fn run_probe(
    _: Command,
    _: &[(&str, OsString)],
    _: Duration,
    _: usize,
) -> Result<Vec<u8>, ProbeError> {
    Err(ProbeError::Unsupported)
}

#[cfg(target_os = "macos")]
fn nonblocking(reader: &impl std::os::fd::AsRawFd) -> std::io::Result<()> {
    let fd = reader.as_raw_fd();
    // SAFETY: the reader owns the descriptor for both calls.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn read_pass(
    reader: &mut impl std::io::Read,
    bytes: &mut usize,
    limit: usize,
    mut output: Option<&mut Vec<u8>>,
) -> Result<bool, ProbeError> {
    let mut buffer = [0; 8192];
    for _ in 0..4 {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(true),
            Ok(count) => {
                if count > limit.saturating_sub(*bytes) {
                    return Err(ProbeError::OutputLimit);
                }
                *bytes += count;
                if let Some(output) = &mut output {
                    output.extend_from_slice(&buffer[..count]);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn uses_only_the_explicit_environment() {
        let output = run_probe(
            Command::new("/usr/bin/env"),
            &[("ROOST_FIXTURE", "visible".into())],
            Duration::from_secs(1),
            1024,
        )
        .unwrap();
        assert_eq!(output, b"ROOST_FIXTURE=visible\n");
    }

    #[test]
    fn bounds_timeout_and_output_and_reports_exit_failure() {
        let mut sleep = Command::new("/bin/sleep");
        sleep.arg("10");
        assert!(matches!(
            run_probe(sleep, &[], Duration::from_millis(50), 1024),
            Err(ProbeError::Timeout)
        ));
        assert!(matches!(
            run_probe(
                Command::new("/usr/bin/yes"),
                &[],
                Duration::from_secs(1),
                1024
            ),
            Err(ProbeError::OutputLimit)
        ));
        assert!(matches!(
            run_probe(
                Command::new("/usr/bin/false"),
                &[],
                Duration::from_secs(1),
                1024
            ),
            Err(ProbeError::Exit)
        ));
    }
}
