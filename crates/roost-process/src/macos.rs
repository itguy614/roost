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

fn exited(pid: libc::pid_t) -> io::Result<bool> {
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

fn signal(pid: libc::pid_t, expected: ProcessIdentity, sig: i32) -> io::Result<()> {
    match inspect(pid) {
        Ok(info) if info.pbi_ppid == std::process::id() && identity(&info) == expected => {
            send_group(pid, sig)
        }
        _ if exited(pid)? => Ok(()),
        _ => Err(io::Error::other("worker identity changed before signal")),
    }
}

fn send_group(pid: libc::pid_t, signal: i32) -> io::Result<()> {
    // SAFETY: pid is the positive PID of our unreaped session leader.
    if unsafe { libc::kill(-pid, signal) } < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

fn group_has_live_members(leader: libc::pid_t) -> io::Result<bool> {
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
