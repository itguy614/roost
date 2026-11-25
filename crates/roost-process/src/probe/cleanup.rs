use std::{
    process::Child,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use super::{ProbeCleanup, ProbeError, group_empty, terminate_group};

const ATTEMPTS: u8 = 8;
const RETRY: Duration = Duration::from_millis(250);

static PENDING: Mutex<Vec<Pending>> = Mutex::new(Vec::new());

struct Pending {
    child: Child,
    attempts: u8,
    next: Instant,
}

fn pending() -> MutexGuard<'static, Vec<Pending>> {
    // Preserve the handles and the admission fence even if a caller panics.
    PENDING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) fn status() -> ProbeCleanup {
    phase(&pending())
}

fn phase(pending: &[Pending]) -> ProbeCleanup {
    if pending.is_empty() {
        ProbeCleanup::Ready
    } else if pending.iter().any(|entry| entry.attempts < ATTEMPTS) {
        ProbeCleanup::Retrying
    } else {
        ProbeCleanup::Blocked
    }
}

pub(super) fn defer(child: Child) -> ProbeError {
    // At most the already admitted discovery and queue probes can reach this
    // point. A pending handle closes admission for both callers.
    pending().push(Pending {
        child,
        attempts: 0,
        next: Instant::now() + RETRY,
    });
    ProbeError::Cleanup
}

pub(super) fn retry() {
    let now = Instant::now();
    pending().retain_mut(|entry| entry.retry_at(now, settle_owned));
}

impl Pending {
    fn retry_at(&mut self, now: Instant, settle: impl FnOnce(&mut Child) -> bool) -> bool {
        // Keep an exhausted child handle pinned. The daemon stays fail-closed
        // and reports blocked rather than launching into unverified custody.
        if self.attempts == ATTEMPTS || self.next > now {
            return true;
        }
        self.attempts += 1;
        self.next = now + RETRY;
        !settle(&mut self.child)
    }
}

fn settle_owned(child: &mut Child) -> bool {
    let pid = child.id() as libc::pid_t;
    // WNOWAIT checks that the original, unreaped session leader is still
    // our child. Never signal if that check or group enumeration fails.
    let Ok(exited) = crate::macos::exited(pid) else {
        return false;
    };
    let Ok(empty) = group_empty(pid) else {
        return false;
    };
    if exited && empty {
        return child.try_wait().is_ok_and(|outcome| outcome.is_some());
    }
    let _ = terminate_group(pid, exited);
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn own_child_cleanup_rechecks_the_group_and_reaps_without_waiting() {
        let mut command = Command::new("/bin/sleep");
        command.arg("60").stdin(Stdio::null());
        let mut child = crate::spawn_session(command, Some(&[])).unwrap();
        // First pass signals the verified group. Subsequent passes can reap
        // the pinned child only after its exit and an empty group.
        assert!(!settle_owned(&mut child));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !settle_owned(&mut child) {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn exhausted_cleanup_stays_blocked_and_does_not_signal_unrelated_pids() {
        // Wait first to simulate a stale/unverifiable handle. waitid can no
        // longer prove custody, so settle_owned cannot signal it.
        let mut child = Command::new("/usr/bin/true").spawn().unwrap();
        child.wait().unwrap();
        assert!(!settle_owned(&mut child));
        let now = Instant::now();
        let mut entries = [Pending {
            child,
            attempts: 0,
            next: now + RETRY,
        }];
        assert_eq!(phase(&entries), ProbeCleanup::Retrying);
        assert!(entries[0].retry_at(now, |_| panic!("retry was not due")));
        for index in 1..=ATTEMPTS {
            assert!(entries[0].retry_at(now + RETRY * u32::from(index), settle_owned));
        }
        assert_eq!(phase(&entries), ProbeCleanup::Blocked);
        assert!(entries[0].retry_at(now + RETRY * 100, |_| panic!("exhausted retry budget")));
        assert_eq!(phase(&[]), ProbeCleanup::Ready);
    }
}
