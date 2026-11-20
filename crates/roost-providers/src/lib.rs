mod connections;
mod suggestions;

use std::{
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use roost_core::{
    PhpProvider, Project,
    discovery::{PhpSource, ProviderDetection, ResolvedPhp},
};
use roost_process::ProbeError;

pub use connections::discover_connections;
pub use suggestions::suggest_queues;

pub const PROVIDERS: [PhpProvider; 2] = [PhpProvider::Herd, PhpProvider::System];
const PROBE_PATH: &str = "/usr/bin:/bin:/usr/sbin:/sbin";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("project location no longer contains a Laravel artisan file")]
    Project,
    #[error("choose either a PHP executable override or a provider")]
    ConflictingSelection,
    #[error("PHP override must be an absolute path to an executable file")]
    InvalidExecutable,
    #[error("Laravel Herd's PHP or herd.phar is unavailable")]
    HerdUnavailable,
    #[error("Herd did not return a usable project PHP executable")]
    HerdResolution,
    #[error("system PHP is unavailable on the daemon's PATH or standard installation paths")]
    SystemUnavailable,
    #[error("Laravel dependencies or bootstrap files are missing")]
    BootstrapFiles,
    #[error("queue discovery returned an invalid result")]
    InvalidOutput,
    #[error("Herd PHP lookup failed: {0}")]
    HerdProbe(ProbeError),
    #[error("Laravel connection discovery failed: {0}")]
    DiscoveryProbe(ProbeError),
}

pub struct Resolver {
    home: Option<PathBuf>,
    temp: PathBuf,
    path: Vec<PathBuf>,
}

impl Default for Resolver {
    fn default() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute());
        let path = std::env::var_os("PATH")
            .map(|path| std::env::split_paths(&path).collect())
            .unwrap_or_default();
        Self {
            home,
            temp: std::env::temp_dir(),
            path,
        }
    }
}

impl Resolver {
    pub fn resolve(
        &self,
        project: &Project,
        selected: Option<PhpProvider>,
        explicit: Option<&Path>,
    ) -> Result<ResolvedPhp, Error> {
        project.validate_location().map_err(|_| Error::Project)?;
        if selected.is_some() && explicit.is_some() {
            return Err(Error::ConflictingSelection);
        }
        let configured = if selected.is_none() {
            project.php_override.as_deref()
        } else {
            None
        };
        if let Some(path) = explicit.or(configured) {
            return Ok(ResolvedPhp {
                path: executable(path).ok_or(Error::InvalidExecutable)?,
                source: PhpSource::Override,
            });
        }
        match selected.unwrap_or(project.provider) {
            PhpProvider::Herd => self.herd(&project.path).map(|path| ResolvedPhp {
                path,
                source: PhpSource::Herd,
            }),
            PhpProvider::System => self.system(&project.path).map(|path| ResolvedPhp {
                path,
                source: PhpSource::System,
            }),
        }
    }

    pub fn detect(&self, project: &Project) -> Result<ProviderDetection, Error> {
        project.validate_location().map_err(|_| Error::Project)?;
        Ok(ProviderDetection {
            preferred: project.provider,
            herd_available: self.herd(&project.path).is_ok(),
            system_available: self.system(&project.path).is_ok(),
            override_configured: project.php_override.is_some(),
        })
    }

    fn herd_dir(&self) -> Option<PathBuf> {
        self.home
            .as_ref()
            .map(|home| home.join("Library/Application Support/Herd/bin"))
    }

    fn herd(&self, project: &Path) -> Result<PathBuf, Error> {
        let dir = self.herd_dir().ok_or(Error::HerdUnavailable)?;
        let bootstrap = executable(&dir.join("php")).ok_or(Error::HerdUnavailable)?;
        let phar = dir.join("herd.phar");
        if !phar.is_file() {
            return Err(Error::HerdUnavailable);
        }
        let mut command = self.command(&bootstrap, project);
        command.arg(phar).arg("which-php");
        let stdout = roost_process::run_probe(
            command,
            &self.environment(),
            Duration::from_millis(1200),
            32 * 1024,
        )
        .map_err(Error::HerdProbe)?;
        let path = std::str::from_utf8(&stdout)
            .ok()
            .map(str::trim_end)
            .filter(|path| !path.is_empty() && !path.chars().any(char::is_control))
            .and_then(|path| executable(Path::new(path)))
            .ok_or(Error::HerdResolution)?;
        let dir = dir.canonicalize().map_err(|_| Error::HerdResolution)?;
        if path.parent() != Some(dir.as_path()) {
            return Err(Error::HerdResolution);
        }
        Ok(path)
    }

    fn system(&self, project: &Path) -> Result<PathBuf, Error> {
        let herd = self.herd_dir().and_then(|dir| dir.canonicalize().ok());
        let standard = ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"];
        self.path
            .iter()
            .cloned()
            .chain(standard.map(PathBuf::from))
            .filter(|dir| dir.is_absolute())
            .filter_map(|dir| dir.canonicalize().ok())
            .filter(|dir| !dir.starts_with(project))
            .filter_map(|dir| executable(&dir.join("php")))
            .find(|path| {
                !path.starts_with(project) && herd.as_ref().is_none_or(|dir| !path.starts_with(dir))
            })
            .ok_or(Error::SystemUnavailable)
    }

    fn environment(&self) -> Vec<(&str, OsString)> {
        let mut environment = vec![
            ("PATH", OsString::from(PROBE_PATH)),
            ("TMPDIR", self.temp.clone().into_os_string()),
        ];
        if let Some(home) = &self.home {
            environment.push(("HOME", home.clone().into_os_string()));
        }
        environment
    }

    fn command(&self, program: &Path, project: &Path) -> Command {
        let mut command = Command::new(program);
        command
            .current_dir(project)
            .args(["-d", "display_errors=stderr", "-d", "log_errors=0"]);
        command
    }
}

fn executable(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let path = fs::canonicalize(path).ok()?;
    let metadata = path.metadata().ok()?;
    (metadata.is_file() && metadata.permissions().mode() & 0o111 != 0).then_some(path)
}

fn safe_identifier(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    !name.is_empty()
        && name.len() <= 96
        && (name.as_bytes()[0].is_ascii_alphanumeric() || name.starts_with('{'))
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:{}".contains(&byte))
        && !["ghp_", "github_pat_", "sk-", "xox", "akia", "asia", "eyj"]
            .iter()
            .any(|prefix| lower.starts_with(prefix))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn make_executable(path: &Path) {
        fs::write(path, "").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn resolves_herd_per_project_and_requires_explicit_system_selection() {
        let path = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("roost-provider-{}", std::process::id()));
        fs::create_dir(&path).unwrap();
        let scratch = Scratch(path);
        let bin = scratch.0.join("Library/Application Support/Herd/bin");
        let system = scratch.0.join("system");
        fs::create_dir_all(&bin).unwrap();
        fs::create_dir(&system).unwrap();
        fs::write(bin.join("herd.phar"), "").unwrap();
        make_executable(&bin.join("php84"));
        make_executable(&bin.join("php85"));
        make_executable(&system.join("php"));
        let source = scratch.0.join("herd_fixture.rs");
        fs::write(
            &source,
            r#"
            fn main() {
                assert_eq!(std::env::args().last().as_deref(), Some("which-php"));
                eprintln!("fixture warning");
                let version = std::fs::read_to_string("artisan").unwrap();
                let home = std::env::var("HOME").unwrap();
                println!("{home}/Library/Application Support/Herd/bin/php{version}");
            }
        "#,
        )
        .unwrap();
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        assert!(
            Command::new(rustc)
                .arg("--edition=2024")
                .arg(&source)
                .arg("-o")
                .arg(bin.join("php"))
                .status()
                .unwrap()
                .success()
        );

        let app = scratch.0.join("app");
        let other = scratch.0.join("other");
        fs::create_dir(&app).unwrap();
        fs::create_dir(&other).unwrap();
        fs::write(app.join("artisan"), "84").unwrap();
        fs::write(other.join("artisan"), "85").unwrap();
        make_executable(&app.join("php"));
        let resolver = Resolver {
            home: Some(scratch.0.clone()),
            temp: std::env::temp_dir(),
            path: vec![app.clone(), bin.clone(), system.clone()],
        };
        let mut project = Project::register(&app).unwrap();
        let other = Project::register(&other).unwrap();
        assert_eq!(
            resolver.resolve(&project, None, None).unwrap().path,
            bin.join("php84")
        );
        assert_eq!(
            resolver.resolve(&other, None, None).unwrap().path,
            bin.join("php85")
        );

        project.php_override = Some(bin.join("php85"));
        assert_eq!(
            resolver.resolve(&project, None, None).unwrap().source,
            PhpSource::Override
        );
        assert_eq!(
            resolver
                .resolve(&project, Some(PhpProvider::System), None)
                .unwrap()
                .path,
            system.join("php")
        );
        assert_eq!(
            resolver
                .resolve(&project, None, Some(&bin.join("php84")))
                .unwrap()
                .path,
            bin.join("php84")
        );
        assert!(matches!(
            resolver.resolve(&project, None, Some(Path::new("php"))),
            Err(Error::InvalidExecutable)
        ));

        fs::remove_file(bin.join("herd.phar")).unwrap();
        assert!(matches!(
            resolver.resolve(&other, None, None),
            Err(Error::HerdUnavailable)
        ));
    }
}
