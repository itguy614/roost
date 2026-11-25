use std::{
    collections::VecDeque,
    fs,
    os::unix::fs::MetadataExt,
    path::Path,
    time::{Duration, Instant},
};

use roost_core::{PhpProvider, Project, ProjectId, discovery::ResolvedPhp};

const TTL: Duration = Duration::from_secs(120);
const MAX_PROJECTS: usize = 64;

#[derive(Clone, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    length: u64,
    changed: (i64, i64),
}

impl FileIdentity {
    fn read(path: &Path) -> Option<Self> {
        let stat = fs::metadata(path).ok()?;
        Some(Self {
            device: stat.dev(),
            inode: stat.ino(),
            mode: stat.mode(),
            length: stat.len(),
            changed: (stat.ctime(), stat.ctime_nsec()),
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct ProjectFiles {
    selection: [Option<FileIdentity>; 4],
    launcher: Option<FileIdentity>,
}

impl ProjectFiles {
    pub(super) fn read(project: &Project) -> Self {
        let selection = [".php-version", ".herd.yml", "herd.yml", "composer.json"]
            .map(|name| FileIdentity::read(&project.path.join(name)));
        let launcher = if let Some(path) = &project.php_override {
            FileIdentity::read(path)
        } else if project.provider == PhpProvider::Herd {
            std::env::var_os("HOME").and_then(|home| {
                FileIdentity::read(
                    &Path::new(&home).join("Library/Application Support/Herd/bin/php"),
                )
            })
        } else {
            None
        };
        Self {
            selection,
            launcher,
        }
    }
}

pub(super) struct Resolved {
    pub(super) php: ResolvedPhp,
    files: ProjectFiles,
    executable: FileIdentity,
    expires: Instant,
}

impl Resolved {
    pub(super) fn new(php: ResolvedPhp, files: ProjectFiles, now: Instant) -> Option<Self> {
        let executable = FileIdentity::read(&php.path)?;
        Some(Self {
            php,
            files,
            executable,
            expires: now + TTL,
        })
    }

    pub(super) fn changed(&self, files: &ProjectFiles) -> bool {
        &self.files != files
            || FileIdentity::read(&self.php.path).as_ref() != Some(&self.executable)
    }

    pub(super) fn fresh(&self, now: Instant) -> bool {
        now < self.expires
    }

    pub(super) fn same_runtime(&self, other: &Self) -> bool {
        self.php.path == other.php.path && self.executable == other.executable
    }
}

#[derive(Default)]
pub(super) struct Cache(VecDeque<(ProjectId, Resolved)>);

impl Cache {
    pub(super) fn take(&mut self, id: ProjectId) -> Option<Resolved> {
        let index = self.0.iter().position(|(key, _)| *key == id)?;
        self.0.remove(index).map(|(_, value)| value)
    }

    pub(super) fn put(&mut self, id: ProjectId, resolved: Resolved) {
        self.take(id);
        if self.0.len() == MAX_PROJECTS {
            self.0.pop_front();
        }
        self.0.push_back((id, resolved));
    }

    pub(super) fn retain(&mut self, mut active: impl FnMut(ProjectId) -> bool) {
        self.0.retain(|(id, _)| active(*id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roost_core::discovery::PhpSource;

    #[test]
    fn fixed_expiration_fingerprints_and_lru_bound() {
        let root = std::env::temp_dir().join(format!("roost-php-cache-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        struct Fixture(std::path::PathBuf);
        impl Drop for Fixture {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let fixture = Fixture(root);
        fs::write(fixture.0.join("artisan"), "").unwrap();
        let project = Project::register(&fixture.0).unwrap();
        let php = fixture.0.join("php");
        fs::write(&php, "a").unwrap();
        let now = Instant::now();
        let make = || {
            Resolved::new(
                ResolvedPhp {
                    path: php.clone(),
                    source: PhpSource::Override,
                },
                ProjectFiles::read(&project),
                now,
            )
            .unwrap()
        };
        let mut cache = Cache::default();
        for i in 0..=MAX_PROJECTS {
            cache.put(ProjectId(uuid::Uuid::from_u128(i as u128)), make());
        }
        assert!(cache.take(ProjectId(uuid::Uuid::nil())).is_none());
        let newest = cache
            .take(ProjectId(uuid::Uuid::from_u128(MAX_PROJECTS as u128)))
            .unwrap();
        assert!(newest.fresh(now + TTL - Duration::from_secs(1)));
        assert!(!newest.fresh(now + TTL));
        assert!(!newest.changed(&ProjectFiles::read(&project)));
        fs::write(fixture.0.join(".php-version"), "8.4").unwrap();
        assert!(newest.changed(&ProjectFiles::read(&project)));
        let resolved = make();
        fs::write(php, "replacement").unwrap();
        assert!(resolved.changed(&ProjectFiles::read(&project)));
    }
}
