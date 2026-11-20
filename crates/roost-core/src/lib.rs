use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);
    };
}

id!(ProjectId);
id!(GroupId);
id!(WorkerProfileId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredState {
    Stopped,
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhpProvider {
    Herd,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigOwner {
    Local,
    File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashRestartPolicy {
    Never,
    OnFailure,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodeRestartPolicy {
    Never,
    OnChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WatchRules {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    pub id: GroupId,
    pub name: String,
    pub root_path: Option<PathBuf>,
    pub watch_rules: WatchRules,
    pub created_at: u64,
    pub updated_at: u64,
    pub revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub group_id: Option<GroupId>,
    pub name: String,
    pub path: PathBuf,
    pub provider: PhpProvider,
    pub php_override: Option<PathBuf>,
    pub desired_state: DesiredState,
    pub autostart: bool,
    pub watch_enabled: bool,
    pub watch_rules: WatchRules,
    pub config_owner: ConfigOwner,
    pub revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerProfile {
    pub id: WorkerProfileId,
    pub project_id: ProjectId,
    pub name: String,
    pub connection: String,
    pub queues: Vec<String>,
    pub desired_instances: u32,
    pub sleep_seconds: u32,
    pub tries: u32,
    pub timeout_seconds: u32,
    pub graceful_stop_seconds: u32,
    pub crash_restart: CrashRestartPolicy,
    pub code_restart: CodeRestartPolicy,
    pub notifications: bool,
    pub enabled: bool,
    pub revision: Revision,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ValidationError(pub &'static str);

impl Project {
    pub fn register(path: &Path) -> Result<Self, ValidationError> {
        let path = fs::canonicalize(path)
            .map_err(|_| ValidationError("project path cannot be resolved"))?;
        if !path.is_dir() {
            return Err(ValidationError("project path must be a directory"));
        }
        if !path.join("artisan").is_file() {
            return Err(ValidationError(
                "project directory must contain an artisan file",
            ));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(ValidationError("project directory must have a UTF-8 name"))?;
        let project = Self {
            id: ProjectId(Uuid::new_v4()),
            group_id: None,
            name: name.to_owned(),
            path,
            provider: PhpProvider::Herd,
            php_override: None,
            desired_state: DesiredState::Stopped,
            autostart: false,
            watch_enabled: false,
            watch_rules: WatchRules::default(),
            config_owner: ConfigOwner::Local,
            revision: Revision(1),
        };
        project.validate()?;
        Ok(project)
    }

    pub fn validate_location(&self) -> Result<(), ValidationError> {
        if !self.path.is_dir() || !self.path.join("artisan").is_file() {
            return Err(ValidationError(
                "project directory must contain an artisan file",
            ));
        }
        let canonical = fs::canonicalize(&self.path)
            .map_err(|_| ValidationError("project path cannot be resolved"))?;
        if canonical.as_os_str() != self.path.as_os_str() {
            return Err(ValidationError("project path must be canonical"));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_name(&self.name)?;
        validate_path(&self.path)?;
        if let Some(path) = &self.php_override {
            validate_path(path)?;
        }
        self.watch_rules.validate()
    }
}

impl Group {
    pub fn new(name: String) -> Result<Self, ValidationError> {
        let now = unix_timestamp();
        let group = Self {
            id: GroupId(Uuid::new_v4()),
            name,
            root_path: None,
            watch_rules: WatchRules::default(),
            created_at: now,
            updated_at: now,
            revision: Revision(1),
        };
        group.validate()?;
        Ok(group)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_name(&self.name)?;
        if let Some(path) = &self.root_path {
            validate_path(path)?;
        }
        if self.updated_at < self.created_at {
            return Err(ValidationError("group update timestamp predates creation"));
        }
        self.watch_rules.validate()
    }
}

impl WorkerProfile {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_name(&self.name)?;
        validate_name(&self.connection)?;
        if self.connection.starts_with('-') {
            return Err(ValidationError("connection must not start with a hyphen"));
        }
        for (index, queue) in self.queues.iter().enumerate() {
            validate_name(queue)?;
            if queue.contains(',') || self.queues[..index].contains(queue) {
                return Err(ValidationError(
                    "queue names must be unique and cannot contain commas",
                ));
            }
        }
        if self.graceful_stop_seconds == 0 {
            return Err(ValidationError("graceful stop timeout must be positive"));
        }
        Ok(())
    }
}

impl WatchRules {
    fn validate(&self) -> Result<(), ValidationError> {
        for rule in self.include.iter().chain(&self.exclude) {
            validate_name(rule)?;
        }
        Ok(())
    }
}

fn validate_name(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(ValidationError(
            "names and rules must be nonempty and cannot contain control characters or surrounding whitespace",
        ));
    }
    Ok(())
}

fn validate_path(path: &Path) -> Result<(), ValidationError> {
    if !path.is_absolute() || path.to_str().is_none_or(|path| path.contains('\0')) {
        return Err(ValidationError(
            "paths must be absolute UTF-8 paths without NUL bytes",
        ));
    }
    Ok(())
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_canonical_artisan_directory_with_safe_defaults() {
        let root = std::env::temp_dir().join(format!("roost-core-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let alias = root.join("alias");
        let app = root.join("app");
        fs::create_dir(&app).unwrap();
        assert!(Project::register(&app.join("missing")).is_err());
        assert!(Project::register(&app).is_err());
        fs::create_dir(app.join("artisan")).unwrap();
        assert!(Project::register(&app).is_err());
        fs::remove_dir(app.join("artisan")).unwrap();
        fs::write(app.join("artisan"), "").unwrap();
        assert!(Project::register(&app.join("artisan")).is_err());
        std::os::unix::fs::symlink(&app, &alias).unwrap();

        let project = Project::register(&alias).unwrap();
        assert_eq!(project.path, fs::canonicalize(&app).unwrap());
        assert_eq!(project.name, "app");
        assert_eq!(project.revision, Revision(1));
        assert_eq!(project.desired_state, DesiredState::Stopped);
        assert_eq!(project.config_owner, ConfigOwner::Local);
        assert_eq!(project.provider, PhpProvider::Herd);
        assert!(!project.autostart);
        assert!(!project.watch_enabled);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn validates_names_paths_and_watch_rules() {
        assert!(Group::new(" ".into()).is_err());
        assert!(Group::new("bad\nname".into()).is_err());
        let mut group = Group::new("Monorepo".into()).unwrap();
        group.root_path = Some("relative".into());
        assert!(group.validate().is_err());
        group.root_path = Some("/absolute".into());
        group.watch_rules.include.push(String::new());
        assert!(group.validate().is_err());
        group.watch_rules.include = vec!["app/**/*.php".into()];
        assert!(group.validate().is_ok());
        group.updated_at = 0;
        assert!(group.validate().is_err());
    }

    #[test]
    fn validates_worker_arguments_and_preserves_queue_order() {
        let mut profile = WorkerProfile {
            id: WorkerProfileId(Uuid::new_v4()),
            project_id: ProjectId(Uuid::new_v4()),
            name: "Emails".into(),
            connection: "redis".into(),
            queues: vec!["high".into(), "default".into()],
            desired_instances: 0,
            sleep_seconds: 3,
            tries: 0,
            timeout_seconds: 0,
            graceful_stop_seconds: 10,
            crash_restart: CrashRestartPolicy::OnFailure,
            code_restart: CodeRestartPolicy::Never,
            notifications: false,
            enabled: false,
            revision: Revision(1),
        };
        assert!(profile.validate().is_ok());
        assert_eq!(profile.queues, ["high", "default"]);
        profile.queues.push("high".into());
        assert!(profile.validate().is_err());
        profile.queues[2] = "other,low".into();
        assert!(profile.validate().is_err());
        profile.queues.clear();
        assert!(profile.validate().is_ok());
        profile.connection = "--help".into();
        assert!(profile.validate().is_err());
        profile.connection = "database".into();
        profile.graceful_stop_seconds = 0;
        assert!(profile.validate().is_err());
    }
}

pub mod worker_runtime;

pub mod discovery;

pub mod scheduler;
