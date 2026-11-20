use std::{
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use roost_core::worker_runtime::{WorkerLease, WorkerLeaseState};
use roost_process::OutputPipe;
use roost_store::Store;
use serde::{Deserialize, Serialize};

pub const ARG: &str = "__worker-guardian";

#[derive(Serialize, Deserialize)]
struct Launch {
    lease_id: uuid::Uuid,
    php: PathBuf,
}

pub struct Guardian {
    pub child: Child,
    pub control: Option<ChildStdin>,
    pub stdout: Option<OutputPipe<ChildStdout>>,
    pub stderr: Option<OutputPipe<ChildStderr>>,
}

impl Guardian {
    pub fn spawn(binary: &Path, db: &Path, lease: &WorkerLease, php: &Path) -> io::Result<Self> {
        let mut launch = serde_json::to_vec(&Launch {
            lease_id: lease.id,
            php: php.to_owned(),
        })?;
        launch.push(b'\n');
        let mut child = Command::new(binary)
            .arg(ARG)
            .arg(db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .and_then(|pipe| OutputPipe::new(pipe).ok());
        let stderr = child
            .stderr
            .take()
            .and_then(|pipe| OutputPipe::new(pipe).ok());
        let mut control = child.stdin.take().expect("guardian stdin is piped");
        // A partial or failed handshake closes the pipe and leaves reconciliation to the lease.
        let control = control.write_all(&launch).ok().map(|()| control);
        Ok(Self {
            child,
            control,
            stdout,
            stderr,
        })
    }

    pub fn stop(&mut self) {
        self.control.take();
    }
}

pub fn run(db: &Path) -> Result<(), Box<dyn std::error::Error>> {
    const LAUNCH_LIMIT: u64 = 8192;
    let mut input = BufReader::new(io::stdin());
    let mut line = Vec::new();
    input
        .by_ref()
        .take(LAUNCH_LIMIT)
        .read_until(b'\n', &mut line)?;
    if line.last() != Some(&b'\n') {
        return Err(io::Error::other("incomplete worker guardian launch").into());
    }
    let launch: Launch = serde_json::from_slice(&line)?;
    let store = Store::open(db)?;
    let lease = store
        .worker_lease(launch.lease_id)?
        .ok_or(io::Error::other("worker lease missing"))?;
    if lease.state != WorkerLeaseState::Reserved {
        return Err(io::Error::other("worker lease is not reserved").into());
    }
    let project = store
        .get_project(lease.project_id)?
        .ok_or(roost_store::Error::NotFound)?;
    let profile = store
        .get_worker_profile(lease.profile_id)?
        .ok_or(roost_store::Error::NotFound)?;
    let Ok(php) = roost_process::validate_worker(&project, &profile, &launch.php) else {
        store.record_worker_state(lease, WorkerLeaseState::LaunchFailed)?;
        return Ok(());
    };

    let stopped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stopped);
    std::thread::Builder::new()
        .name("roost-guardian-control".into())
        .spawn(move || {
            let mut byte = [0; 1];
            let _ = input.read(&mut byte);
            flag.store(true, Ordering::Release);
        })?;

    #[cfg(target_os = "macos")]
    roost_process::run_guarded(
        roost_process::worker_command(&project, &profile, &php),
        Duration::from_secs(u64::from(profile.graceful_stop_seconds)),
        || stopped.load(Ordering::Acquire),
        |state| {
            store
                .record_worker_state(lease.clone(), state)
                .map_err(io::Error::other)
        },
    )?;
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (php, stopped, Duration::ZERO);
        store.record_worker_state(lease, WorkerLeaseState::LaunchFailed)?;
        return Err(io::Error::other("worker supervision is currently supported on macOS").into());
    }
    Ok(())
}
