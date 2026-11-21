use std::{
    io,
    mem::{MaybeUninit, size_of},
    os::unix::process::ExitStatusExt,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use roost_core::{
    unix_timestamp,
    worker_runtime::{LaunchFailure, ProcessExit, ProcessIdentity, StopCause, WorkerLeaseState},
};

const TICK: Duration = Duration::from_millis(20);
const KILL_DEADLINE: Duration = Duration::from_secs(2);
const PROC_PGRP_ONLY: u32 = 2;

/// Runs inside a dedicated guardian. The guardian exclusively owns and reaps its child.
pub fn run_guarded(
    mut command: Command,
    grace: Duration,
    stop: impl Fn() -> Option<StopCause>,
    mut record: impl FnMut(WorkerLeaseState) -> io::Result<()>,
) -> io::Result<()> {
    if stop().is_some() {
        return record(WorkerLeaseState::LaunchFailure {
            failure: LaunchFailure::Cancelled,
        });
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut child = match crate::spawn_in_session(command) {
        Ok(child) => child,
        Err(error) => {
            return record(WorkerLeaseState::LaunchFailure {
                failure: crate::classify_spawn_error(&error),
            });
        }
    };

    let mut last_identity = None;
    let result = supervise(&mut child, grace, stop, &mut |state| {
        if let WorkerLeaseState::Running { identity } = state {
            last_identity = Some(identity);
        }
        record(state)
    });
    if result.is_err() {
        // An unconfirmed group is never made eligible for replacement.
        record(WorkerLeaseState::Uncertain {
            identity: last_identity,
        })?;
    }
    result
}

fn supervise(
    child: &mut Child,
    grace: Duration,
    stop: impl Fn() -> Option<StopCause>,
    record: &mut impl FnMut(WorkerLeaseState) -> io::Result<()>,
) -> io::Result<()> {
    let pid = child.id() as libc::pid_t;
    let identity = if exited(pid)? {
        None
    } else {
        match inspect(pid) {
            Ok(info)
                if info.pbi_pid == child.id()
                    && info.pbi_ppid == std::process::id()
                    && info.pbi_pgid == child.id()
                    && info.pbi_uid == unsafe { libc::geteuid() } =>
            {
                Some(identity(&info))
            }
            _ if exited(pid)? => None,
            _ => return Err(io::Error::other("cannot verify worker process identity")),
        }
    };
    let mut stop_cause = None;
    let mut forced = false;
    let mut deadline = None;
    if let Some(identity) = identity {
        // If durable publication fails, stop the verified child instead of losing ownership.
        if record(WorkerLeaseState::Running { identity }).is_err() {
            stop_cause = Some(StopCause::StatePublicationFailed);
        }
        loop {
            if exited(pid)? {
                break;
            }
            if deadline.is_none()
                && let Some(cause) = stop_cause.or_else(&stop)
            {
                stop_cause = Some(cause);
                signal(pid, identity, libc::SIGTERM)?;
                deadline = Some(Instant::now() + grace);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                forced = true;
                signal(pid, identity, libc::SIGKILL)?;
                let until = Instant::now() + KILL_DEADLINE;
                while !exited(pid)? {
                    if Instant::now() >= until {
                        return Err(io::Error::other("worker did not exit after SIGKILL"));
                    }
                    thread::sleep(TICK);
                }
                break;
            }
            thread::sleep(TICK);
        }
    }

    // waitid(WNOWAIT) still pins the leader PID here. It cannot be reused as a
    // different session/process-group ID until reaping. No foreign session can join it.
    if group_has_live_members(pid)? {
        let graceful_deadline = if let Some(deadline) = deadline {
            deadline
        } else {
            send_group(pid, libc::SIGTERM)?;
            Instant::now() + grace
        };
        while Instant::now() < graceful_deadline && group_has_live_members(pid)? {
            thread::sleep(TICK);
        }
        let until = Instant::now() + KILL_DEADLINE;
        while group_has_live_members(pid)? {
            send_group(pid, libc::SIGKILL)?;
            forced = true;
            if Instant::now() >= until {
                return Err(io::Error::other("worker process group did not empty"));
            }
            thread::sleep(TICK);
        }
    }
    let status = child.wait()?;
    record(WorkerLeaseState::Exited {
        exit: ProcessExit {
            code: status.code(),
            signal: status.signal(),
            at: unix_timestamp(),
            requested_stop: stop_cause.is_some(),
            stop_cause,
            forced,
        },
    })
}

pub(crate) fn exited(pid: libc::pid_t) -> io::Result<bool> {
    // SAFETY: waitid fills the initialized siginfo and WNOWAIT preserves the child PID.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { info.si_pid() } == pid)
}

fn inspect(pid: libc::pid_t) -> io::Result<libc::proc_bsdinfo> {
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = size_of::<libc::proc_bsdinfo>() as i32;
    // SAFETY: proc_pidinfo is given an appropriately sized output buffer.
    if unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    } == size
    {
        Ok(unsafe { info.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

fn identity(info: &libc::proc_bsdinfo) -> ProcessIdentity {
    ProcessIdentity {
        pid: info.pbi_pid,
        group: info.pbi_pgid,
        uid: info.pbi_uid,
        started_seconds: info.pbi_start_tvsec,
        started_microseconds: info.pbi_start_tvusec,
    }
}

pub fn read_process_counters(expected: ProcessIdentity) -> io::Result<crate::ProcessCounters> {
    let pid = expected.pid as libc::pid_t;
    let verify = || match inspect(pid) {
        Ok(info) if identity(&info) == expected && info.pbi_status != libc::SZOMB => Ok(()),
        _ => Err(io::Error::other("process identity changed or disappeared")),
    };
    verify()?;
    let mut usage = MaybeUninit::<libc::rusage_info_v0>::uninit();
    // SAFETY: RUSAGE_INFO_V0 selects exactly the supplied buffer layout.
    if unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V0, usage.as_mut_ptr().cast()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let usage = unsafe { usage.assume_init() };
    let awake_nanoseconds = clock_nanoseconds(libc::CLOCK_UPTIME_RAW)?;
    let sampled_at_nanoseconds = clock_nanoseconds(libc::CLOCK_MONOTONIC_RAW)?;
    verify()?;
    Ok(crate::ProcessCounters {
        cpu_nanoseconds: usage.ri_user_time.saturating_add(usage.ri_system_time),
        rss_bytes: usage.ri_resident_size,
        uptime_seconds: process_uptime(usage.ri_proc_start_abstime),
        sampled_at_nanoseconds,
        awake_nanoseconds,
    })
}

fn clock_nanoseconds(kind: libc::clockid_t) -> io::Result<u64> {
    let mut clock = MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the supplied timespec on success.
    if unsafe { libc::clock_gettime(kind, clock.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let clock = unsafe { clock.assume_init() };
    Ok((clock.tv_sec as u64) * 1_000_000_000 + clock.tv_nsec as u64)
}

// Kernel rusage reports process start in Mach timebase ticks. Keep these SDK
// calls together despite the libc crate's deprecation of its Mach bindings.
#[allow(deprecated)]
fn process_uptime(start: u64) -> Option<u64> {
    let mut scale = libc::mach_timebase_info { numer: 0, denom: 0 };
    // SAFETY: mach_timebase_info writes the provided struct.
    if start == 0 || unsafe { libc::mach_timebase_info(&mut scale) } != 0 || scale.denom == 0 {
        return None;
    }
    let elapsed = unsafe { libc::mach_absolute_time() }.checked_sub(start)?;
    let seconds =
        u128::from(elapsed) * u128::from(scale.numer) / u128::from(scale.denom) / 1_000_000_000;
    u64::try_from(seconds).ok()
}

fn signal(pid: libc::pid_t, expected: ProcessIdentity, sig: i32) -> io::Result<()> {
    match inspect(pid) {
        Ok(info) if info.pbi_ppid == std::process::id() && identity(&info) == expected => {
            send_group(pid, sig)
        }
        _ if exited(pid)? => Ok(()),
        _ => Err(io::Error::other("worker identity changed before signal")),
    }
}

pub(crate) fn send_group(pid: libc::pid_t, signal: i32) -> io::Result<()> {
    // SAFETY: pid is the positive PID of our unreaped session leader.
    if unsafe { libc::kill(-pid, signal) } < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) fn group_has_live_members(leader: libc::pid_t) -> io::Result<bool> {
    let mut pids = [0_i32; 512];
    let size = size_of_val(&pids) as i32;
    // SAFETY: proc_listpids writes at most size bytes to the PID array.
    let count = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            leader as u32,
            pids.as_mut_ptr().cast(),
            size,
        )
    };
    if count < 0 || count == size {
        return Err(io::Error::other(
            "worker group membership could not be verified",
        ));
    }
    for &pid in &pids[..count as usize / size_of::<libc::pid_t>()] {
        if pid == 0 || pid == leader {
            continue;
        }
        // The listed PID can disappear or be reused in another group.
        let group = unsafe { libc::getpgid(pid) };
        if group < 0 && io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(io::Error::other(
                "worker group member could not be verified",
            ));
        }
        if group == leader && !inspect(pid).is_ok_and(|info| info.pbi_status == libc::SZOMB) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_read_checks_identity_and_returns_live_counters() {
        let pid = std::process::id() as i32;
        let ident = identity(&inspect(pid).unwrap());
        let initial = read_process_counters(ident).unwrap();
        assert!(initial.rss_bytes > 0);
        let until = Instant::now() + Duration::from_millis(30);
        while Instant::now() < until {
            std::hint::spin_loop();
        }
        let next = read_process_counters(ident).unwrap();
        assert!(next.cpu_nanoseconds.saturating_sub(initial.cpu_nanoseconds) > 1_000_000);
        assert!(next.sampled_at_nanoseconds > initial.sampled_at_nanoseconds);
        assert!(
            read_process_counters(ProcessIdentity {
                started_microseconds: ident.started_microseconds + 1,
                ..ident
            })
            .is_err()
        );
    }

    #[test]
    fn observes_an_exited_child_without_reaping_its_pid() {
        let mut child = Command::new("/usr/bin/true").spawn().unwrap();
        let limit = Instant::now() + Duration::from_secs(2);
        while !exited(child.id() as i32).unwrap() {
            assert!(Instant::now() < limit);
            thread::sleep(TICK);
        }
        assert!(child.wait().unwrap().success());
    }
}
