use std::{
    ffi::OsString,
    process::Command,
    time::{Duration, Instant},
};

#[cfg(target_os = "macos")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_os = "macos")]
static CLEANUP_PENDING: AtomicUsize = AtomicUsize::new(0);

pub fn probe_cleanup_pending() -> bool {
    #[cfg(target_os = "macos")]
    {
        CLEANUP_PENDING.load(Ordering::Acquire) != 0
    }
    #[cfg(not(target_os = "macos"))]
    {
        false
    }
}

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
pub fn run_probe(
    command: Command,
    environment: &[(&str, OsString)],
    duration: Duration,
    limit: usize,
) -> Result<Vec<u8>, ProbeError> {
    let execution_deadline = Instant::now() + duration;
    run_probe_before(
        command,
        environment,
        execution_deadline,
        execution_deadline + Duration::from_millis(250),
        limit,
    )
}

/// Only stdout is retained. Stderr counts toward the same output limit.
#[cfg(target_os = "macos")]
pub fn run_probe_before(
    mut command: Command,
    environment: &[(&str, OsString)],
    execution_deadline: Instant,
    cleanup_deadline: Instant,
    limit: usize,
) -> Result<Vec<u8>, ProbeError> {
    use std::{process::Stdio, thread};

    if CLEANUP_PENDING.load(Ordering::Acquire) != 0 {
        return Err(ProbeError::Cleanup);
    }
    if Instant::now() >= execution_deadline {
        return Err(ProbeError::Timeout);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = crate::spawn_session(command, Some(environment))?;
    let pid = child.id() as libc::pid_t;
    // Keep the leader PID pinned until the group is empty. An exited leader
    // can still have live descendants holding inherited pipes.
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let mut signaled = false;
    let result = (|| {
        nonblocking(&stdout)?;
        nonblocking(&stderr)?;
        let mut output = Vec::new();
        let mut bytes = 0;
        loop {
            let exited = crate::macos::exited(pid)?;
            if exited && !signaled {
                terminate_group(pid, true)?;
                signaled = true;
            }
            let out_idle = read_pass(&mut stdout, &mut bytes, limit, Some(&mut output))?;
            let err_idle = read_pass(&mut stderr, &mut bytes, limit, None)?;
            if Instant::now() >= execution_deadline {
                return Err(ProbeError::Timeout);
            }
            if exited && out_idle && err_idle && group_empty(pid)? {
                return Ok(output);
            }
            if out_idle && err_idle {
                thread::sleep(Duration::from_millis(10));
            }
        }
    })();
    if result.is_err() {
        let cleanup = crate::macos::exited(pid)
            .map_err(|_| ProbeError::Cleanup)
            .and_then(|exited| terminate_group(pid, exited));
        if cleanup.is_err() {
            return defer_reap(child);
        }
        loop {
            if crate::macos::exited(pid).is_ok_and(|exited| exited)
                && group_empty(pid).is_ok_and(|empty| empty)
            {
                break;
            }
            if Instant::now() >= cleanup_deadline {
                return defer_reap(child);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    let status = child.wait()?;
    result.and_then(|output| {
        if status.success() {
            Ok(output)
        } else {
            Err(ProbeError::Exit)
        }
    })
}

#[cfg(target_os = "macos")]
fn defer_reap(mut child: std::process::Child) -> Result<Vec<u8>, ProbeError> {
    CLEANUP_PENDING.fetch_add(1, Ordering::AcqRel);
    let _ = std::thread::Builder::new()
        .name("roost-probe-reaper".into())
        .spawn(move || {
            let pid = child.id() as libc::pid_t;
            loop {
                if crate::macos::exited(pid).is_ok_and(|exited| exited)
                    && group_empty(pid).is_ok_and(|empty| empty)
                {
                    if child.wait().is_ok() {
                        CLEANUP_PENDING.fetch_sub(1, Ordering::AcqRel);
                    }
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        });
    Err(ProbeError::Cleanup)
}

#[cfg(target_os = "macos")]
fn group_empty(pid: libc::pid_t) -> Result<bool, ProbeError> {
    crate::macos::group_has_live_members(pid)
        .map(|live| !live)
        .map_err(|_| ProbeError::Cleanup)
}

#[cfg(target_os = "macos")]
fn terminate_group(pid: libc::pid_t, exited: bool) -> Result<(), ProbeError> {
    if exited && group_empty(pid)? {
        return Ok(());
    }
    match crate::macos::send_group(pid, libc::SIGKILL) {
        Ok(()) => Ok(()),
        // The child can exit between waitid and kill, with no live group left.
        Err(_) if crate::macos::exited(pid).is_ok_and(|exited| exited) && group_empty(pid)? => {
            Ok(())
        }
        Err(_) => Err(ProbeError::Cleanup),
    }
}

#[cfg(not(target_os = "macos"))]
pub fn run_probe_before(
    _: Command,
    _: &[(&str, OsString)],
    _: Instant,
    _: Instant,
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

    #[test]
    fn timeout_confirms_cleanup_of_a_live_descendant() {
        const CHILD: &str = "ROOST_TEST_PROBE_DESCENDANT";
        if let Some(path) = std::env::var_os(CHILD) {
            let mut descendant = Command::new("/bin/sleep").arg("60").spawn().unwrap();
            std::fs::write(path, descendant.id().to_string()).unwrap();
            let _ = descendant.wait();
            return;
        }

        let path = std::env::temp_dir().join(format!("roost-probe-group-{}", std::process::id()));
        let mut fixture = Command::new(std::env::current_exe().unwrap());
        fixture.args([
            "--exact",
            "probe::tests::timeout_confirms_cleanup_of_a_live_descendant",
        ]);
        let started = Instant::now();
        let deadline = started + Duration::from_millis(350);
        let outcome = run_probe_before(
            fixture,
            &[(CHILD, path.clone().into_os_string())],
            deadline,
            deadline + Duration::from_millis(300),
            16 * 1024,
        );
        let pid = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(matches!(outcome, Err(ProbeError::Timeout)));
        let status = Command::new("/bin/ps")
            .args(["-p", pid.trim(), "-o", "state="])
            .output()
            .unwrap();
        let state = String::from_utf8(status.stdout).unwrap();
        assert!(
            state.trim().is_empty() || state.trim().starts_with('Z'),
            "{state}"
        );
    }
}
