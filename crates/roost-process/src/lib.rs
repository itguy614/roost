use std::{
    collections::VecDeque,
    io::{self, Read},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::Command,
};

use roost_core::worker_runtime::{MAX_GRACEFUL_STOP_SECONDS, MAX_PROFILE_INSTANCES};
use roost_core::{
    Project, WorkerProfile, unix_timestamp,
    worker_runtime::{OutputStream, OutputSummary},
};

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::run_guarded;

pub fn validate_worker(
    project: &Project,
    profile: &WorkerProfile,
    php_path: &Path,
) -> io::Result<PathBuf> {
    project.validate_location().map_err(io::Error::other)?;
    profile.validate().map_err(io::Error::other)?;
    if profile.project_id != project.id || !profile.enabled || profile.desired_instances == 0 {
        return Err(io::Error::other(
            "worker profile is not enabled for this project",
        ));
    }
    if profile.desired_instances > MAX_PROFILE_INSTANCES
        || profile.graceful_stop_seconds > MAX_GRACEFUL_STOP_SECONDS
    {
        return Err(io::Error::other(
            "worker profile exceeds supervision limits",
        ));
    }
    if !php_path.is_absolute() {
        return Err(io::Error::other("PHP executable path must be absolute"));
    }
    let path = php_path
        .canonicalize()
        .map_err(|_| io::Error::other("PHP executable cannot be resolved"))?;
    use std::os::unix::fs::PermissionsExt;
    let metadata = path
        .metadata()
        .map_err(|_| io::Error::other("PHP executable cannot be read"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(io::Error::other(
            "PHP path must point to an executable file",
        ));
    }
    Ok(path)
}

pub fn worker_command(project: &Project, profile: &WorkerProfile, php: &Path) -> Command {
    let mut command = Command::new(php);
    command
        .current_dir(&project.path)
        .arg("artisan")
        .arg("queue:work")
        .arg(&profile.connection)
        .args([
            format!("--sleep={}", profile.sleep_seconds),
            format!("--tries={}", profile.tries),
            format!("--timeout={}", profile.timeout_seconds),
        ]);
    if !profile.queues.is_empty() {
        command.arg(format!("--queue={}", profile.queues.join(",")));
    }
    command
}

pub struct OutputPipe<R>(R);

impl<R: Read + AsRawFd> OutputPipe<R> {
    pub fn new(reader: R) -> io::Result<Self> {
        let fd = reader.as_raw_fd();
        // SAFETY: reader owns a valid pipe descriptor.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(reader))
    }

    /// Drain at most 64 KiB per tick so a noisy child cannot monopolize the supervisor.
    pub fn drain(
        &mut self,
        stream: OutputStream,
        status: &roost_core::worker_runtime::WorkerStatus,
        entries: &mut VecDeque<OutputSummary>,
    ) -> bool {
        let mut buffer = [0; 8192];
        let mut bytes = 0;
        let mut newlines = 0;
        for _ in 0..8 {
            match self.0.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    bytes += count as u32;
                    newlines += buffer[..count]
                        .iter()
                        .filter(|&&byte| byte == b'\n')
                        .count() as u32;
                }
            }
        }
        if bytes > 0 {
            if entries.len() >= 64 {
                entries.pop_front();
            }
            entries.push_back(OutputSummary {
                instance_id: status.id,
                generation: status.generation,
                at: unix_timestamp(),
                stream,
                bytes,
                newlines,
            });
        }
        bytes > 0
    }
}
