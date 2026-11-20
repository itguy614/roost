use std::{
    collections::VecDeque,
    io::{self, Read},
    os::fd::AsRawFd,
    sync::LazyLock,
};

use regex::Regex;
use roost_core::{
    unix_timestamp,
    worker_runtime::{OutputStream, OutputSummary, WorkerStatus},
};

const LINE_LIMIT: usize = 512;
const LINES_PER_STREAM: usize = 32;
const REDACTED: &str = "[line redacted: possible credential]";
const TRUNCATED: &str = "[line omitted: over 512 bytes]";

static ANSI: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").unwrap());
static SENSITIVE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r"(?ix)
      \b[\w-]*(?:password|passwd|pwd|secret|token|api[-_\x20]?key|app_key|access_key(?:_id)?|credentials?|authorization|cookie)[\x22'\]\s]*[:=]
      | \b(?:bearer|basic)\s+[a-z0-9+/_.=-]{8,}
      | [a-z][a-z0-9+.-]*://[^\s/@]+@
      | \beyJ[a-z0-9_-]{5,}\.[a-z0-9_-]{6,}\.[a-z0-9_-]{6,}
      | \b(?:github_pat_|gh[pousr]_|sk-(?:proj-)?|AKIA|ASIA|xox[baprs]-)[a-z0-9_-]{12,}"
).unwrap()
});

pub struct OutputPipe<R> {
    reader: Option<R>,
    capture: Capture,
}

pub struct DrainResult {
    pub saw_output: bool,
    pub complete: bool,
}

impl<R: Read + AsRawFd> OutputPipe<R> {
    pub fn new(reader: R) -> io::Result<Self> {
        let fd = reader.as_raw_fd();
        // SAFETY: reader owns a valid pipe descriptor.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            reader: Some(reader),
            capture: Capture::default(),
        })
    }
}

impl<R: Read> OutputPipe<R> {
    /// Read at most 64 KiB per tick. Hitting the budget leaves the same
    /// capture open so a partial line can continue on the next tick.
    pub fn drain(
        &mut self,
        stream: OutputStream,
        status: &WorkerStatus,
        entries: &mut VecDeque<OutputSummary>,
        guardian_exited: bool,
    ) -> DrainResult {
        let Some(reader) = &mut self.reader else {
            return DrainResult {
                saw_output: false,
                complete: true,
            };
        };
        let mut buffer = [0; 8192];
        let mut result = DrainResult {
            saw_output: false,
            complete: false,
        };
        for _ in 0..8 {
            match reader.read(&mut buffer) {
                Ok(0) => {
                    result.complete = true;
                    break;
                }
                Ok(count) => {
                    result.saw_output = true;
                    self.capture
                        .append(&buffer[..count], stream, status, entries);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    // A writer outside the worker group may still hold the pipe open.
                    result.complete = guardian_exited;
                    break;
                }
                Err(_) => {
                    // The remaining bytes are inaccessible. Drop an incomplete line.
                    self.reader = None;
                    return DrainResult {
                        complete: true,
                        ..result
                    };
                }
            }
        }
        if result.complete {
            self.capture.finish(stream, status, entries);
            self.reader = None;
        }
        result
    }
}

#[derive(Default)]
struct Capture {
    pending: Vec<u8>,
    dropped: bool,
    bytes: u32,
    private_key: bool,
}

impl Capture {
    fn append(
        &mut self,
        chunk: &[u8],
        stream: OutputStream,
        status: &WorkerStatus,
        entries: &mut VecDeque<OutputSummary>,
    ) {
        for fragment in chunk.split_inclusive(|byte| *byte == b'\n') {
            let newline = fragment.last() == Some(&b'\n');
            let fragment = &fragment[..fragment.len() - usize::from(newline)];
            self.bytes = self
                .bytes
                .saturating_add(fragment.len() as u32 + u32::from(newline));
            if !self.dropped {
                let retain = fragment.len().min(LINE_LIMIT - self.pending.len());
                self.pending.extend_from_slice(&fragment[..retain]);
                self.dropped = retain < fragment.len();
            }
            if newline {
                self.emit(true, stream, status, entries);
            }
        }
    }

    fn finish(
        &mut self,
        stream: OutputStream,
        status: &WorkerStatus,
        entries: &mut VecDeque<OutputSummary>,
    ) {
        if self.bytes != 0 {
            self.emit(false, stream, status, entries);
        }
    }

    fn emit(
        &mut self,
        newline: bool,
        stream: OutputStream,
        status: &WorkerStatus,
        entries: &mut VecDeque<OutputSummary>,
    ) {
        let text = sanitize(&self.pending, self.dropped, &mut self.private_key);
        if entries.iter().filter(|line| line.stream == stream).count() == LINES_PER_STREAM
            && let Some(index) = entries.iter().position(|line| line.stream == stream)
        {
            entries.remove(index);
        }
        entries.push_back(OutputSummary {
            instance_id: status.id,
            generation: status.generation,
            at: unix_timestamp(),
            stream,
            bytes: self.bytes,
            newlines: u32::from(newline),
            text,
        });
        self.pending.clear();
        self.dropped = false;
        self.bytes = 0;
    }
}

fn sanitize(bytes: &[u8], dropped: bool, private_key: &mut bool) -> String {
    let Ok(raw) = std::str::from_utf8(bytes) else {
        return REDACTED.into();
    };
    let text = ANSI.replace_all(raw.trim_end_matches('\r'), "");
    let marker = text.to_ascii_uppercase();
    if marker.contains("-----BEGIN") && marker.contains("PRIVATE KEY-----") {
        *private_key = true;
    }
    if *private_key {
        if !dropped && marker.contains("-----END") && marker.contains("PRIVATE KEY-----") {
            *private_key = false;
        }
        return REDACTED.into();
    }
    if dropped {
        return TRUNCATED.into();
    }
    if text.chars().any(|ch| {
        (ch.is_control() && ch != '\t')
            || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    }) || SENSITIVE.is_match(&text)
    {
        return REDACTED.into();
    }
    text.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use roost_core::{DesiredState, ProjectId, WorkerProfileId, worker_runtime::ObservedState};

    fn status() -> WorkerStatus {
        let id = "3dd8a878-24e3-4ccf-b55f-cfcd05bb22ec".parse().unwrap();
        WorkerStatus {
            id,
            project_id: ProjectId(id),
            profile_id: WorkerProfileId(id),
            slot: 0,
            generation: 1,
            desired_state: DesiredState::Running,
            state: ObservedState::Running,
            identity: None,
            started_at: None,
            last_exit: None,
            restarts: 0,
            rapid_failures: 0,
            retry_in_ms: None,
            recovery_blocked: false,
            last_error: None,
            reason: None,
            launch_failure: None,
        }
    }

    #[test]
    fn redacts_known_credentials_while_preserving_messages_and_stack_frames() {
        let mut private_key = false;
        for input in [
            "DB_PASSWORD=\"secret value\"",
            "Authorization: Bearer sample.token.secret",
            "redis://user:password@localhost:6379",
            "-----BEGIN OPENSSH PRIVATE KEY-----",
            "a private key payload",
            "-----END OPENSSH PRIVATE KEY-----",
            "Cookie: session=value",
            "request body token\x1b[0m=secret",
        ] {
            assert_eq!(
                sanitize(input.as_bytes(), false, &mut private_key),
                REDACTED
            );
        }
        for input in [
            "RuntimeException: database is unreachable",
            "#0 src/Queue.php(20): run()",
        ] {
            assert_eq!(sanitize(input.as_bytes(), false, &mut private_key), input);
        }
    }

    #[test]
    fn bounds_partial_lines_and_keeps_stderr_under_stdout_flood() {
        let mut capture = Capture::default();
        let mut entries = VecDeque::new();
        let status = status();
        capture.append(b"DB_PASS", OutputStream::Stderr, &status, &mut entries);
        assert!(entries.is_empty());
        capture.append(
            b"WORD=split-secret\n#0 source.php",
            OutputStream::Stderr,
            &status,
            &mut entries,
        );
        capture.finish(OutputStream::Stderr, &status, &mut entries);
        assert_eq!(entries[0].text, REDACTED);
        assert_eq!(entries[1].text, "#0 source.php");
        let flood = vec![b'x'; 64 * 1024];
        capture.append(&flood, OutputStream::Stdout, &status, &mut entries);
        assert_eq!(capture.pending.len(), LINE_LIMIT);
        capture.append(b"\n", OutputStream::Stdout, &status, &mut entries);
        assert_eq!(entries.back().unwrap().text, TRUNCATED);
        for _ in 0..(LINES_PER_STREAM * 2) {
            capture.append(b"line\n", OutputStream::Stdout, &status, &mut entries);
        }
        assert_eq!(entries.len(), LINES_PER_STREAM + 2);
        assert_eq!(entries[1].text, "#0 source.php");
    }

    #[test]
    fn exit_drain_keeps_tail_and_redacts_a_line_split_at_the_read_budget() {
        use std::io::Cursor;

        // Put the credential key across the 64 KiB boundary. The next reads
        // must complete the line before applying redaction and retain the stack tail.
        let mut data = b"padding\n".repeat(8191);
        data.extend_from_slice(b"DB_PASSW");
        assert_eq!(data.len(), 64 * 1024);
        data.extend_from_slice(
            b"ORD=sample-private-value\nRuntimeException: failed\n#0 worker.php:42",
        );
        let mut pipe = OutputPipe {
            reader: Some(Cursor::new(data)),
            capture: Capture::default(),
        };
        let status = status();
        let mut entries = VecDeque::new();

        let first = pipe.drain(OutputStream::Stderr, &status, &mut entries, true);
        assert!(first.saw_output && !first.complete);
        assert_eq!(entries.back().unwrap().text, "padding");

        let second = pipe.drain(OutputStream::Stderr, &status, &mut entries, true);
        assert!(second.saw_output && second.complete);
        let tail: Vec<_> = entries
            .iter()
            .rev()
            .take(3)
            .map(|line| line.text.as_str())
            .collect();
        assert_eq!(
            tail,
            ["#0 worker.php:42", "RuntimeException: failed", REDACTED]
        );
        assert_eq!(entries.back().unwrap().newlines, 0);

        let third = pipe.drain(OutputStream::Stderr, &status, &mut entries, true);
        assert!(!third.saw_output && third.complete);
        assert_eq!(
            entries
                .iter()
                .filter(|line| line.text == "#0 worker.php:42")
                .count(),
            1
        );
    }

    #[test]
    fn exit_drain_finishes_when_an_inherited_writer_keeps_the_pipe_open() {
        use std::{io::Write, os::unix::net::UnixStream};

        let (reader, mut writer) = UnixStream::pair().unwrap();
        let mut pipe = OutputPipe::new(reader).unwrap();
        writer.write_all(b"#0 worker.php:42").unwrap();
        let status = status();
        let mut entries = VecDeque::new();

        let running = pipe.drain(OutputStream::Stderr, &status, &mut entries, false);
        assert!(running.saw_output && !running.complete);
        assert!(entries.is_empty());

        let exited = pipe.drain(OutputStream::Stderr, &status, &mut entries, true);
        assert!(!exited.saw_output && exited.complete);
        assert_eq!(entries[0].text, "#0 worker.php:42");
        assert_eq!(entries[0].newlines, 0);
    }
}
