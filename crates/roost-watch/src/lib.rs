#![cfg(unix)]

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use roost_core::WatchRules;

pub const MAX_PROJECTS: usize = 16;
const MAX_ENTRIES: usize = 32_768;
const MAX_FILES: usize = 8_192;
const MAX_DEPTH: usize = 64;
const DEBOUNCE: Duration = Duration::from_millis(500);
const MAX_BATCH: Duration = Duration::from_secs(2);
const RECONCILE: Duration = Duration::from_secs(2);
const DIRTY: u8 = 1;
const BACKEND_ERROR: u8 = 2;

#[derive(Debug, thiserror::Error)]
#[error("watch rules must be valid project-relative globs (at most 64 rules of 256 bytes)")]
pub struct InvalidRules;

pub struct Rules {
    include: GlobSet,
    exclude: GlobSet,
}

impl Rules {
    pub fn compile(rules: &WatchRules) -> Result<Self, InvalidRules> {
        if rules.include.len().saturating_add(rules.exclude.len()) > 64 {
            return Err(InvalidRules);
        }
        let include = if rules.include.is_empty() {
            globs([
                "**/*.php",
                ".env",
                "artisan",
                "composer.json",
                "composer.lock",
            ])?
        } else {
            globs(rules.include.iter().map(String::as_str))?
        };
        let exclude = globs(rules.exclude.iter().map(String::as_str))?;
        Ok(Self { include, exclude })
    }

    fn excluded(&self, path: &Path) -> bool {
        excluded_by_default(path) || self.exclude.is_match(path)
    }
}

pub fn excluded_by_default(path: &Path) -> bool {
    path.components().any(|part| {
        matches!(part, Component::Normal(name) if [".git", "vendor", "node_modules", "build", "dist", "target"].iter().any(|dir| name == *dir))
    }) || path.ancestors().any(|ancestor| {
        ancestor.ends_with("storage/logs") || ancestor.ends_with("storage/framework")
    })
}

fn globs(patterns: impl IntoIterator<Item = impl AsRef<str>>) -> Result<GlobSet, InvalidRules> {
    let mut set = GlobSetBuilder::new();
    for pattern in patterns {
        let pattern = pattern.as_ref();
        let path = Path::new(pattern);
        if pattern.is_empty()
            || pattern.len() > 256
            || pattern.trim() != pattern
            || pattern.contains('\\')
            || pattern.chars().any(char::is_control)
            || path
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(InvalidRules);
        }
        set.add(
            GlobBuilder::new(pattern)
                .literal_separator(true)
                .build()
                .map_err(|_| InvalidRules)?,
        );
    }
    set.build().map_err(|_| InvalidRules)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl From<fs::Metadata> for Stamp {
    fn from(meta: fs::Metadata) -> Self {
        Self {
            device: meta.dev(),
            inode: meta.ino(),
            bytes: meta.len(),
            modified: (meta.mtime(), meta.mtime_nsec()),
            changed: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Problem {
    Backend,
    RootUnavailable,
    ScanFailed,
    ScanLimit,
}

impl Problem {
    pub fn message(self) -> &'static str {
        match self {
            Self::Backend => "native watcher unavailable; using periodic reconciliation",
            Self::RootUnavailable => "project root is unavailable",
            Self::ScanFailed => "could not reconcile project files",
            Self::ScanLimit => "project exceeded the bounded watch scan limits",
        }
    }
}

#[derive(Debug, Default)]
struct Batch {
    range: Option<(Instant, Instant)>,
}

impl Batch {
    fn record(&mut self, now: Instant) {
        let first = self.range.map_or(now, |(first, _)| first);
        self.range = Some((first, now));
    }

    fn take_due(&mut self, now: Instant) -> Option<Instant> {
        let (first, last) = self.range?;
        if now.duration_since(last) >= DEBOUNCE || now.duration_since(first) >= MAX_BATCH {
            self.range = None;
            return Some(last);
        }
        None
    }
}

pub struct Poll {
    pub ready: bool,
    pub pending: bool,
    pub problem: Option<Problem>,
    pub changed: bool,
    pub changed_at: Option<Instant>,
}

pub struct ProjectWatch {
    root: PathBuf,
    rules: Arc<Rules>,
    backend: Option<RecommendedWatcher>,
    signal: Arc<AtomicU8>,
    next_scan: Instant,
    next_attach: Instant,
    root_identity: Option<(u64, u64)>,
    baseline: Option<HashMap<PathBuf, Stamp>>,
    scan_problem: Option<Problem>,
    batch: Batch,
}

impl ProjectWatch {
    pub fn new(root: PathBuf, rules: Rules, now: Instant) -> Self {
        Self {
            root,
            rules: Arc::new(rules),
            backend: None,
            signal: Arc::new(AtomicU8::new(0)),
            next_scan: now,
            next_attach: now,
            root_identity: None,
            baseline: None,
            scan_problem: None,
            batch: Batch::default(),
        }
    }

    pub fn poll(&mut self, now: Instant, other_roots: &HashSet<PathBuf>) -> Poll {
        let signal = self.signal.swap(0, Ordering::AcqRel);
        let mut changed = false;
        if signal & BACKEND_ERROR != 0 {
            self.backend = None;
        }
        if signal != 0 || now >= self.next_scan {
            self.next_scan = now + RECONCILE;
            let identity = self
                .root
                .canonicalize()
                .ok()
                .filter(|real| real == &self.root)
                .and_then(|path| path.symlink_metadata().ok())
                .filter(fs::Metadata::is_dir)
                .map(|metadata| (metadata.dev(), metadata.ino()));
            if identity != self.root_identity {
                self.root_identity = identity;
                self.backend = None;
                self.next_attach = now;
            }
            if self.backend.is_none() && now >= self.next_attach && identity.is_some() {
                self.next_attach = now + RECONCILE;
                self.backend = attach(&self.root, &self.rules, &self.signal).ok();
            }
            let result = identity
                .ok_or(Problem::RootUnavailable)
                .and_then(|_| snapshot(&self.root, &self.rules, other_roots));
            match result {
                Ok(current) => {
                    if self
                        .baseline
                        .as_ref()
                        .is_some_and(|previous| *previous != current)
                    {
                        changed = true;
                        self.batch.record(now);
                    }
                    self.baseline = Some(current);
                    self.scan_problem = None;
                }
                Err(problem) => {
                    self.scan_problem = Some(problem);
                    if problem == Problem::RootUnavailable {
                        self.backend = None;
                    }
                }
            }
        }
        let problem = self
            .scan_problem
            .or(self.backend.is_none().then_some(Problem::Backend));
        let changed_at = if self.scan_problem.is_none() {
            self.batch.take_due(now)
        } else {
            None
        };
        Poll {
            ready: self.baseline.is_some(),
            pending: self.batch.range.is_some(),
            problem,
            changed,
            changed_at,
        }
    }
}

fn attach(
    root: &Path,
    rules: &Arc<Rules>,
    signal: &Arc<AtomicU8>,
) -> notify::Result<RecommendedWatcher> {
    let watched_root = root.to_owned();
    let rules = Arc::clone(rules);
    let signal = Arc::clone(signal);
    let handler = move |event: notify::Result<notify::Event>| {
        let flags = match event {
            Ok(event) if event.need_rescan() => DIRTY,
            Ok(event)
                if !matches!(event.kind, EventKind::Access(_))
                    && (event.paths.is_empty()
                        || event.paths.iter().any(|path| {
                            path.strip_prefix(&watched_root).is_ok_and(|relative| {
                                !relative
                                    .ancestors()
                                    .any(|ancestor| rules.excluded(ancestor))
                            })
                        })) =>
            {
                DIRTY
            }
            Ok(_) => return,
            Err(_) => DIRTY | BACKEND_ERROR,
        };
        signal.fetch_or(flags, Ordering::Release);
    };
    let config = notify::Config::default().with_follow_symlinks(false);
    let mut watcher = RecommendedWatcher::new(handler, config)?;
    watcher.watch(root, RecursiveMode::Recursive)?;
    Ok(watcher)
}

fn snapshot(
    root: &Path,
    rules: &Rules,
    other_roots: &HashSet<PathBuf>,
) -> Result<HashMap<PathBuf, Stamp>, Problem> {
    if !root
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.is_dir())
    {
        return Err(Problem::RootUnavailable);
    }
    let mut files = HashMap::new();
    let mut directories = vec![(root.to_owned(), 0)];
    let mut entries = 0;
    while let Some((directory, depth)) = directories.pop() {
        if depth > MAX_DEPTH {
            return Err(Problem::ScanLimit);
        }
        let children = match fs::read_dir(&directory) {
            Ok(children) => children,
            Err(error) if directory != root && error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(Problem::ScanFailed),
        };
        for entry in children {
            entries += 1;
            if entries > MAX_ENTRIES {
                return Err(Problem::ScanLimit);
            }
            let entry = entry.map_err(|_| Problem::ScanFailed)?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .expect("walk starts at project root");
            if rules.excluded(relative) || other_roots.contains(&path) {
                continue;
            }
            let kind = match entry.file_type() {
                Ok(kind) => kind,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => return Err(Problem::ScanFailed),
            };
            if kind.is_dir() {
                directories.push((path, depth + 1));
            } else if kind.is_file() && rules.include.is_match(relative) {
                match entry.metadata() {
                    Ok(metadata) if metadata.is_file() => {
                        if files.len() == MAX_FILES {
                            return Err(Problem::ScanLimit);
                        }
                        files.insert(relative.to_owned(), metadata.into());
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(_) => return Err(Problem::ScanFailed),
                }
            }
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("roost-watch-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }

        fn write(&self, relative: &str) {
            let path = self.0.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"fixture").unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn rules_match_only_project_files_and_prune_excluded_directories() {
        let project = Fixture::new();
        let outside = Fixture::new();
        outside.write("external.php");
        let included = [
            ".env",
            "artisan",
            "composer.json",
            "composer.lock",
            "root.php",
            "app/Job.php",
        ];
        for path in included.into_iter().chain([
            "vendor/package/vendor.php",
            "ui/node_modules/src.php",
            "storage/logs/log.php",
            "storage/framework/view.php",
            ".git/hook.php",
            "ui/build/build.php",
            "ui/dist/bundle.php",
            "target/bin.php",
            "ui/package.json",
            "apps/child/app/Job.php",
        ]) {
            project.write(path);
        }
        std::os::unix::fs::symlink(&outside.0, project.0.join("outside")).unwrap();
        std::os::unix::fs::symlink(
            outside.0.join("external.php"),
            project.0.join("symlink.php"),
        )
        .unwrap();
        let nested = HashSet::from([project.0.join("apps/child")]);
        let rules = Rules::compile(&WatchRules::default()).unwrap();
        let files = snapshot(&project.0, &rules, &nested).unwrap();
        assert_eq!(
            files.into_keys().collect::<HashSet<_>>(),
            included.map(PathBuf::from).into()
        );
        let rules = Rules::compile(&WatchRules {
            include: vec!["**/*.json".into(), "app/**/*.php".into()],
            exclude: vec!["composer.json".into(), "app".into()],
        })
        .unwrap();
        let files = snapshot(&project.0, &rules, &nested).unwrap();
        assert_eq!(
            files.into_keys().collect::<Vec<_>>(),
            [PathBuf::from("ui/package.json")]
        );
        for rule in ["", "/app/**/*.php", "../*.php", "app/../*.php", "[", "a\\b"] {
            assert!(
                Rules::compile(&WatchRules {
                    include: vec![rule.into()],
                    exclude: vec![]
                })
                .is_err()
            );
        }
    }

    #[test]
    fn debounce_respects_quiet_time_and_caps_continuous_changes() {
        let start = Instant::now();
        let at = |ms| start + Duration::from_millis(ms);
        let mut batch = Batch::default();
        batch.record(at(0));
        batch.record(at(400));
        assert_eq!(batch.take_due(at(899)), None);
        assert_eq!(batch.take_due(at(900)), Some(at(400)));
        assert_eq!(batch.take_due(at(901)), None);
        batch.record(at(1000));
        for ms in (1200..=2800).step_by(200) {
            batch.record(at(ms));
            assert_eq!(batch.take_due(at(ms)), None);
        }
        batch.record(at(2999));
        assert_eq!(batch.take_due(at(2999)), None);
        assert_eq!(batch.take_due(at(3000)), Some(at(2999)));
    }

    #[test]
    fn reconciles_overflow_backend_error_and_deleted_root_without_restarting_from_baseline() {
        let project = Fixture::new();
        let root = project.0.join("app");
        fs::create_dir(&root).unwrap();
        let file = root.join("job.php");
        fs::write(&file, b"old").unwrap();
        let start = Instant::now();
        let at = |ms| start + Duration::from_millis(ms);
        let mut watch = ProjectWatch::new(
            root.clone(),
            Rules::compile(&WatchRules::default()).unwrap(),
            start,
        );
        let nested = HashSet::new();
        let first = watch.poll(start, &nested);
        assert!(first.ready && !first.pending && first.changed_at.is_none());

        let replacement = root.join("temporary");
        fs::write(&replacement, b"new").unwrap();
        fs::rename(replacement, &file).unwrap();
        // A rescan signal carries no path and recovers by comparing snapshots.
        watch.signal.fetch_or(DIRTY, Ordering::Release);
        assert!(watch.poll(at(100), &nested).pending);
        assert_eq!(watch.poll(at(599), &nested).changed_at, None);
        assert_eq!(watch.poll(at(600), &nested).changed_at, Some(at(100)));

        let moved = project.0.join("removed");
        fs::rename(&root, &moved).unwrap();
        std::os::unix::fs::symlink(&moved, &root).unwrap();
        watch
            .signal
            .fetch_or(DIRTY | BACKEND_ERROR, Ordering::Release);
        assert_eq!(
            watch.poll(at(700), &nested).problem,
            Some(Problem::RootUnavailable)
        );
        fs::remove_file(&root).unwrap();
        fs::create_dir(&root).unwrap();
        fs::write(&file, b"restored").unwrap();
        let periodic = watch.poll(at(3000), &nested);
        assert!(periodic.ready && periodic.pending);
        assert_eq!(watch.poll(at(3500), &nested).changed_at, Some(at(3000)));
    }
}
