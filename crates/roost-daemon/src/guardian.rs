use std::{
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use roost_core::{
    WorkerProfile,
    scheduler::SchedulerProfile,
    worker_runtime::{LaunchFailure, ProcessLease, ProcessProfileId, StopCause, WorkerLeaseState},
};
use roost_process::OutputPipe;
use roost_store::Store;
use serde::{Deserialize, Serialize};

use crate::supervised::SupervisedProfile;

pub const ARG: &str = "__worker-guardian";

#[derive(Serialize, Deserialize)]
struct Launch {
    lease_id: uuid::Uuid,
    php: PathBuf,
    #[serde(default)]
    scheduler: bool,
}

pub struct Guardian {
    pub child: Child,
    pub control: Option<ChildStdin>,
    pub stdout: Option<OutputPipe<ChildStdout>>,
    pub stderr: Option<OutputPipe<ChildStderr>>,
}

impl Guardian {
    pub fn spawn<P: ProcessProfileId>(
        binary: &Path,
        db: &Path,
        lease: &ProcessLease<P>,
        php: &Path,
    ) -> io::Result<Self> {
        let mut launch = serde_json::to_vec(&Launch {
            lease_id: lease.id,
            php: php.to_owned(),
            scheduler: P::SCHEDULER,
        })?;
        launch.push(b'\n');
        let mut command = Command::new(binary);
        command
            .arg(ARG)
            .arg(db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Separate the guardian from terminal interrupts to the foreground daemon.
        let mut child = roost_process::spawn_in_session(command)?;
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

    pub fn stop(&mut self, cause: StopCause) {
        if let Some(mut control) = self.control.take() {
            let _ = control.write_all(&[encode_stop(cause)]);
        }
    }
}

fn encode_stop(cause: StopCause) -> u8 {
    match cause {
        StopCause::ProjectStop => 1,
        StopCause::ProjectRestart => 2,
        StopCause::DaemonShutdown => 3,
        StopCause::StartupTimeout => 4,
        StopCause::GuardianWaitFailed => 5,
        _ => 6,
    }
}

fn decode_stop(byte: u8) -> Option<StopCause> {
    match byte {
        0 => None,
        1 => Some(StopCause::ProjectStop),
        2 => Some(StopCause::ProjectRestart),
        3 => Some(StopCause::DaemonShutdown),
        4 => Some(StopCause::StartupTimeout),
        5 => Some(StopCause::GuardianWaitFailed),
        _ => Some(StopCause::DaemonDisconnected),
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
    if launch.scheduler {
        run_process::<SchedulerProfile>(&store, launch, input)
    } else {
        run_process::<WorkerProfile>(&store, launch, input)
    }
}

fn run_process<P: SupervisedProfile>(
    store: &Store,
    launch: Launch,
    mut input: BufReader<io::Stdin>,
) -> Result<(), Box<dyn std::error::Error>> {
    let lease = store
        .process_lease::<P::Id>(launch.lease_id)?
        .ok_or(io::Error::other("process lease missing"))?;
    let lease = store.claim_process(lease)?;
    let project = store
        .get_project(lease.project_id)?
        .ok_or(roost_store::Error::NotFound)?;
    let profile = P::load(store, lease.profile_id)?.ok_or(roost_store::Error::NotFound)?;
    let Ok(command) = profile.command(&project, &launch.php) else {
        store.record_process_state(
            lease,
            WorkerLeaseState::LaunchFailure {
                failure: LaunchFailure::Validation,
            },
        )?;
        return Ok(());
    };

    let stopped = Arc::new(AtomicU8::new(0));
    let flag = Arc::clone(&stopped);
    if let Err(error) = std::thread::Builder::new()
        .name("roost-guardian-control".into())
        .spawn(move || {
            let mut byte = [0; 1];
            if input.read_exact(&mut byte).is_err() || byte[0] == 0 {
                byte[0] = encode_stop(StopCause::DaemonDisconnected);
            }
            flag.store(byte[0], Ordering::Release);
        })
    {
        store.record_process_state(
            lease,
            WorkerLeaseState::LaunchFailure {
                failure: roost_process::classify_spawn_error(&error),
            },
        )?;
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    roost_process::run_guarded(
        command,
        Duration::from_secs(u64::from(profile.grace_seconds())),
        || decode_stop(stopped.load(Ordering::Acquire)),
        |state| {
            store
                .record_process_state(lease.clone(), state)
                .map_err(io::Error::other)
        },
    )?;
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (command, stopped, Duration::ZERO, profile.grace_seconds());
        store.record_process_state(
            lease,
            WorkerLeaseState::LaunchFailure {
                failure: LaunchFailure::Validation,
            },
        )?;
        return Err(io::Error::other("worker supervision is currently supported on macOS").into());
    }
    Ok(())
}
