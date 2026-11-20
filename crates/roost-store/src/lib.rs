mod leases;
mod runtime;
mod sqlite;

use std::{ffi::CStr, path::Path};

use roost_core::{
    Group, GroupId, Project, ProjectId, Revision, ValidationError, WorkerProfile, WorkerProfileId,
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

const INITIAL: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/001_initial.sql"), "\0").as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const WORKER_LEASES: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/002_worker_leases.sql"), "\0").as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const RUNTIME: &CStr = match CStr::from_bytes_with_nul(
    concat!(
        include_str!("../migrations/003_intent_and_diagnostics.sql"),
        "\0"
    )
    .as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("record was not found")]
    NotFound,
    #[error("record already exists")]
    Duplicate,
    #[error("referenced record is missing or is still in use")]
    Reference,
    #[error("config revision changed")]
    RevisionConflict,
    #[error("idempotency key already used for a different request")]
    IdempotencyConflict,
    #[error("unsupported database schema version")]
    Schema,
    #[error("database encoding failed")]
    Encoding(#[from] serde_json::Error),
    #[error(transparent)]
    Sql(sqlite::Error),
}

impl From<sqlite::Error> for Error {
    fn from(error: sqlite::Error) -> Self {
        match error.code {
            1555 | 2067 => Self::Duplicate,
            787 | 1811 => Self::Reference,
            _ => Self::Sql(error),
        }
    }
}

pub struct Store(sqlite::Connection);

impl Store {
    pub fn initialize(path: &Path) -> Result<(), Error> {
        let store = Self(sqlite::Connection::open(path, true)?);
        store.transaction(|store| {
            match store
                .0
                .query(c"PRAGMA user_version", &[])?
                .first()
                .map(String::as_str)
            {
                Some("0") => {
                    store.0.exec(INITIAL)?;
                    store.0.exec(WORKER_LEASES)?;
                    store.0.exec(RUNTIME)?;
                }
                Some("1") => {
                    store.0.exec(WORKER_LEASES)?;
                    store.0.exec(RUNTIME)?;
                }
                Some("2") => store.0.exec(RUNTIME)?,
                Some("3") => (),
                _ => return Err(Error::Schema),
            }
            Ok(())
        })
    }

    pub fn open(path: &Path) -> Result<Self, Error> {
        Ok(Self(sqlite::Connection::open(path, false)?))
    }

    fn transaction<T, E: From<Error>>(
        &self,
        work: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.0.exec(c"BEGIN IMMEDIATE").map_err(Error::from)?;
        let result = work(self).and_then(|value| {
            self.0.exec(c"COMMIT").map_err(Error::from)?;
            Ok(value)
        });
        if result.is_err() {
            self.0.exec(c"ROLLBACK").map_err(Error::from)?;
        }
        result
    }

    /// Run a mutation and record its successful result atomically. A retry can
    /// recover the same result even after the affected record has changed.
    pub fn mutate<T: Serialize + DeserializeOwned>(
        &self,
        key: Option<Uuid>,
        request: &str,
        work: impl FnOnce(&Self) -> Result<T, Error>,
    ) -> Result<(Uuid, T), Error> {
        self.try_mutate(key, request, work)
    }

    pub fn try_mutate<T: Serialize + DeserializeOwned, E: From<Error>>(
        &self,
        key: Option<Uuid>,
        request: &str,
        work: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<(Uuid, T), E> {
        self.transaction(|store| {
            let id = key.unwrap_or_else(Uuid::new_v4);
            let id_text = id.to_string();
            if key.is_some() {
                let original: Option<String> = store.one(
                    c"SELECT request FROM mutation_receipts WHERE id = ?1",
                    &id_text,
                )?;
                if let Some(original) = original {
                    if original != request {
                        return Err(Error::IdempotencyConflict.into());
                    }
                    let result = store
                        .one(
                            c"SELECT result FROM mutation_receipts WHERE id = ?1",
                            &id_text,
                        )?
                        .ok_or(Error::NotFound)?;
                    return Ok((id, result));
                }
            }
            let result = work(store)?;
            if key.is_some() {
                let encoded = serde_json::to_string(&result).map_err(Error::from)?;
                store
                    .0
                    .query(
                        c"INSERT INTO mutation_receipts (id, request, result) VALUES (?1, ?2, ?3)",
                        &[
                            Some(&id_text),
                            Some(&serde_json::to_string(request).map_err(Error::from)?),
                            Some(&encoded),
                        ],
                    )
                    .map_err(Error::from)?;
            }
            Ok((id, result))
        })
    }

    fn one<T: DeserializeOwned>(&self, sql: &CStr, id: &str) -> Result<Option<T>, Error> {
        self.0
            .query(sql, &[Some(id)])?
            .first()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .transpose()
    }

    fn all<T: DeserializeOwned>(&self, sql: &CStr) -> Result<Vec<T>, Error> {
        self.0
            .query(sql, &[])?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    fn delete(&self, sql: &CStr, id: Uuid) -> Result<(), Error> {
        self.0.query(sql, &[Some(&id.to_string())])?;
        if self.0.changes() == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }
}

fn next_revision(revision: Revision) -> Result<Revision, Error> {
    revision
        .0
        .checked_add(1)
        .filter(|&next| next > 1)
        .map(Revision)
        .ok_or(Error::RevisionConflict)
}

fn initial_revision(revision: Revision) -> Result<(), Error> {
    if revision != Revision(1) {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}

impl Store {
    pub fn create_group(&self, group: &Group) -> Result<(), Error> {
        group.validate()?;
        initial_revision(group.revision)?;
        self.0.query(
            c"INSERT INTO groups (id, revision, data) VALUES (?1, 1, ?2)",
            &[
                Some(&group.id.0.to_string()),
                Some(&serde_json::to_string(group)?),
            ],
        )?;
        Ok(())
    }

    pub fn get_group(&self, id: GroupId) -> Result<Option<Group>, Error> {
        self.one(c"SELECT data FROM groups WHERE id = ?1", &id.0.to_string())
    }

    pub fn list_groups(&self) -> Result<Vec<Group>, Error> {
        self.all(c"SELECT data FROM groups ORDER BY id")
    }

    pub fn update_group(&self, mut group: Group) -> Result<Group, Error> {
        group.validate()?;
        let expected = group.revision;
        group.revision = next_revision(expected)?;
        self.0.query(
            c"UPDATE groups SET revision = ?2, data = ?3 WHERE id = ?1 AND revision = ?4",
            &[
                Some(&group.id.0.to_string()),
                Some(&group.revision.0.to_string()),
                Some(&serde_json::to_string(&group)?),
                Some(&expected.0.to_string()),
            ],
        )?;
        self.updated(group)
    }

    pub fn delete_group(&self, id: GroupId) -> Result<(), Error> {
        self.delete(c"DELETE FROM groups WHERE id = ?1", id.0)
    }

    pub fn create_project(&self, project: &Project) -> Result<(), Error> {
        project.validate()?;
        project.validate_location()?;
        initial_revision(project.revision)?;
        let group = project.group_id.map(|id| id.0.to_string());
        self.0.query(
            c"INSERT INTO projects (id, path, group_id, revision, data) VALUES (?1, ?2, ?3, 1, ?4)",
            &[
                Some(&project.id.0.to_string()),
                project.path.to_str(),
                group.as_deref(),
                Some(&serde_json::to_string(project)?),
            ],
        )?;
        Ok(())
    }

    pub fn get_project(&self, id: ProjectId) -> Result<Option<Project>, Error> {
        self.one(
            c"SELECT data FROM projects WHERE id = ?1",
            &id.0.to_string(),
        )
    }

    pub fn list_projects(&self) -> Result<Vec<Project>, Error> {
        self.all(c"SELECT data FROM projects ORDER BY path")
    }

    pub fn update_project(&self, mut project: Project) -> Result<Project, Error> {
        project.validate()?;
        let current = self.get_project(project.id)?.ok_or(Error::NotFound)?;
        if current.revision != project.revision {
            return Err(Error::RevisionConflict);
        }
        if current.path.as_os_str() != project.path.as_os_str() {
            project.validate_location()?;
        }
        let expected = project.revision;
        project.revision = next_revision(expected)?;
        let group = project.group_id.map(|id| id.0.to_string());
        self.0.query(c"UPDATE projects SET path = ?2, group_id = ?3, revision = ?4, data = ?5 WHERE id = ?1 AND revision = ?6", &[
            Some(&project.id.0.to_string()), project.path.to_str(), group.as_deref(),
            Some(&project.revision.0.to_string()), Some(&serde_json::to_string(&project)?), Some(&expected.0.to_string()),
        ])?;
        self.updated(project)
    }

    pub fn delete_project(&self, id: ProjectId) -> Result<(), Error> {
        self.delete(c"DELETE FROM projects WHERE id = ?1", id.0)
    }

    pub fn create_worker_profile(&self, profile: &WorkerProfile) -> Result<(), Error> {
        profile.validate()?;
        initial_revision(profile.revision)?;
        self.0.query(
            c"INSERT INTO worker_profiles (id, project_id, revision, data) VALUES (?1, ?2, 1, ?3)",
            &[
                Some(&profile.id.0.to_string()),
                Some(&profile.project_id.0.to_string()),
                Some(&serde_json::to_string(profile)?),
            ],
        )?;
        Ok(())
    }

    pub fn get_worker_profile(&self, id: WorkerProfileId) -> Result<Option<WorkerProfile>, Error> {
        self.one(
            c"SELECT data FROM worker_profiles WHERE id = ?1",
            &id.0.to_string(),
        )
    }

    pub fn list_worker_profiles(&self, project_id: ProjectId) -> Result<Vec<WorkerProfile>, Error> {
        self.0
            .query(
                c"SELECT data FROM worker_profiles WHERE project_id = ?1 ORDER BY id",
                &[Some(&project_id.0.to_string())],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    pub fn worker_profiles_for_suggestions(
        &self,
        project_id: ProjectId,
    ) -> Result<Vec<WorkerProfile>, Error> {
        self.0
            .query(
                c"SELECT data FROM worker_profiles WHERE project_id = ?1 ORDER BY id LIMIT 129",
                &[Some(&project_id.0.to_string())],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    pub fn update_worker_profile(
        &self,
        mut profile: WorkerProfile,
    ) -> Result<WorkerProfile, Error> {
        profile.validate()?;
        let expected = profile.revision;
        profile.revision = next_revision(expected)?;
        self.0.query(c"UPDATE worker_profiles SET project_id = ?2, revision = ?3, data = ?4 WHERE id = ?1 AND revision = ?5", &[
            Some(&profile.id.0.to_string()), Some(&profile.project_id.0.to_string()),
            Some(&profile.revision.0.to_string()), Some(&serde_json::to_string(&profile)?), Some(&expected.0.to_string()),
        ])?;
        self.updated(profile)
    }

    pub fn delete_worker_profile(&self, id: WorkerProfileId) -> Result<(), Error> {
        self.delete(c"DELETE FROM worker_profiles WHERE id = ?1", id.0)
    }

    fn updated<T>(&self, record: T) -> Result<T, Error> {
        if self.0.changes() == 0 {
            return Err(Error::RevisionConflict);
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use roost_core::{CodeRestartPolicy, CrashRestartPolicy};

    use super::*;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("roost-store-{}", Uuid::new_v4()));
            fs::create_dir(&dir).unwrap();
            Self(fs::canonicalize(dir).unwrap())
        }

        fn db(&self) -> PathBuf {
            self.0.join("state.sqlite3")
        }

        fn project(&self) -> Project {
            let dir = self.0.join("app");
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("artisan"), "").unwrap();
            Project::register(&dir).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn initializes_reopens_and_rejects_newer_schema() {
        let fixture = Fixture::new();
        let db = fixture.db();
        assert!(Store::open(&db).is_err());
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(store.0.query(c"PRAGMA user_version", &[]).unwrap(), ["3"]);
        assert_eq!(store.0.query(c"PRAGMA foreign_keys", &[]).unwrap(), ["1"]);
        let project = fixture.project();
        store.create_project(&project).unwrap();
        drop(store);
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(store.list_projects().unwrap(), [project]);
        store.0.exec(c"PRAGMA user_version = 4").unwrap();
        assert!(matches!(Store::initialize(&db), Err(Error::Schema)));
        assert_eq!(store.0.query(c"PRAGMA user_version", &[]).unwrap(), ["4"]);
    }

    #[test]
    fn upgrades_milestone_one_database_without_losing_projects() {
        let fixture = Fixture::new();
        let db = fixture.db();
        let old = Store(sqlite::Connection::open(&db, true).unwrap());
        old.0.exec(INITIAL).unwrap();
        let project = fixture.project();
        old.create_project(&project).unwrap();
        assert_eq!(old.0.query(c"PRAGMA user_version", &[]).unwrap(), ["1"]);
        Store::initialize(&db).unwrap();
        assert_eq!(old.0.query(c"PRAGMA user_version", &[]).unwrap(), ["3"]);
        assert_eq!(old.list_projects().unwrap(), std::slice::from_ref(&project));
        assert!(old.project_worker_leases(project.id).unwrap().is_empty());
    }

    #[test]
    fn upgrades_v2_preserving_unresolved_worker_ownership() {
        use roost_core::worker_runtime::{
            DaemonStartup, ProjectIntent, WorkerLease, WorkerLeaseState,
        };

        let fixture = Fixture::new();
        let db = fixture.db();
        let old = Store(sqlite::Connection::open(&db, true).unwrap());
        old.0.exec(INITIAL).unwrap();
        old.0.exec(WORKER_LEASES).unwrap();
        let project = fixture.project();
        old.create_project(&project).unwrap();
        let profile = WorkerProfile {
            id: WorkerProfileId(Uuid::new_v4()),
            project_id: project.id,
            name: "Worker".into(),
            connection: "database".into(),
            queues: Vec::new(),
            desired_instances: 1,
            sleep_seconds: 3,
            tries: 1,
            timeout_seconds: 60,
            graceful_stop_seconds: 1,
            crash_restart: CrashRestartPolicy::Never,
            code_restart: CodeRestartPolicy::Never,
            notifications: false,
            enabled: true,
            revision: Revision(1),
        };
        old.create_worker_profile(&profile).unwrap();
        let lease = WorkerLease {
            id: Uuid::new_v4(),
            project_id: project.id,
            profile_id: profile.id,
            slot: 0,
            state: WorkerLeaseState::Reserved,
        };
        old.reserve_worker(&lease).unwrap();
        let claimed = old.claim_worker(lease).unwrap();

        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(
            store.project_intent(project.id).unwrap(),
            ProjectIntent::Stopped
        );
        assert_eq!(store.worker_lease(claimed.id).unwrap(), Some(claimed));
        assert_eq!(store.begin_daemon_run().unwrap(), DaemonStartup::FirstStart);
        assert!(!store.finish_daemon_run().unwrap());
        assert_eq!(
            store.begin_daemon_run().unwrap(),
            DaemonStartup::AfterInterruptedShutdown
        );
    }

    #[test]
    fn rolls_back_a_partially_applied_migration() {
        let fixture = Fixture::new();
        let db = fixture.db();
        let conn = sqlite::Connection::open(&db, true).unwrap();
        conn.exec(c"CREATE TABLE projects (id TEXT)").unwrap();
        assert!(Store::initialize(&db).is_err());
        assert_eq!(conn.query(c"PRAGMA user_version", &[]).unwrap(), ["0"]);
        assert!(
            conn.query(c"SELECT name FROM sqlite_master WHERE name = 'groups'", &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn crud_enforces_revisions_uniqueness_and_foreign_keys() {
        let fixture = Fixture::new();
        Store::initialize(&fixture.db()).unwrap();
        let store = Store::open(&fixture.db()).unwrap();
        let mut group = Group::new("Apps".into()).unwrap();
        let mut project = fixture.project();
        project.group_id = Some(group.id);
        assert!(matches!(
            store.create_project(&project),
            Err(Error::Reference)
        ));
        store.create_group(&group).unwrap();
        assert_eq!(store.get_group(group.id).unwrap(), Some(group.clone()));
        group.name = "Monorepo".into();
        group = store.update_group(group.clone()).unwrap();
        assert_eq!(group.revision, Revision(2));
        assert_eq!(store.list_groups().unwrap(), [group.clone()]);

        store.create_project(&project).unwrap();
        assert!(matches!(
            store.create_project(&fixture.project()),
            Err(Error::Duplicate)
        ));
        assert!(matches!(
            store.delete_group(group.id),
            Err(Error::Reference)
        ));
        let stale = project.clone();
        project.name = "App".into();
        let project = store.update_project(project).unwrap();
        assert_eq!(project.revision, Revision(2));
        assert_eq!(
            store.get_project(project.id).unwrap(),
            Some(project.clone())
        );
        assert!(matches!(
            store.update_project(stale),
            Err(Error::RevisionConflict)
        ));
        let mut profile = WorkerProfile {
            id: WorkerProfileId(Uuid::new_v4()),
            project_id: project.id,
            name: "Worker".into(),
            connection: "redis".into(),
            queues: vec!["high".into(), "default".into()],
            desired_instances: 1,
            sleep_seconds: 3,
            tries: 1,
            timeout_seconds: 60,
            graceful_stop_seconds: 10,
            crash_restart: CrashRestartPolicy::OnFailure,
            code_restart: CodeRestartPolicy::Never,
            notifications: false,
            enabled: false,
            revision: Revision(1),
        };
        store.create_worker_profile(&profile).unwrap();
        profile.desired_instances = 2;
        let stale = profile.clone();
        profile = store.update_worker_profile(profile).unwrap();
        assert_eq!(profile.revision, Revision(2));
        assert!(matches!(
            store.update_worker_profile(stale),
            Err(Error::RevisionConflict)
        ));
        assert_eq!(
            store.list_worker_profiles(project.id).unwrap(),
            [profile.clone()]
        );
        store.delete_worker_profile(profile.id).unwrap();
        assert!(store.get_worker_profile(profile.id).unwrap().is_none());
        profile.revision = Revision(1);
        store.create_worker_profile(&profile).unwrap();

        store.delete_project(project.id).unwrap();
        assert!(store.get_project(project.id).unwrap().is_none());
        assert!(store.get_worker_profile(profile.id).unwrap().is_none());
        assert!(matches!(
            store.delete_project(project.id),
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store.create_worker_profile(&profile),
            Err(Error::Reference)
        ));
        store.delete_group(group.id).unwrap();
        assert!(store.list_groups().unwrap().is_empty());
    }

    #[test]
    fn rolls_back_failed_mutation_and_replays_success_after_reopen() {
        let fixture = Fixture::new();
        let db = fixture.db();
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        let project = fixture.project();
        let key = Some(Uuid::new_v4());
        let result = store.mutate::<Project>(key, "add", |store| {
            store.create_project(&project)?;
            Err(Error::NotFound)
        });
        assert!(matches!(result, Err(Error::NotFound)));
        assert!(store.list_projects().unwrap().is_empty());
        let receipt = store
            .mutate(key, "add", |store| {
                store.create_project(&project)?;
                Ok(project.clone())
            })
            .unwrap();
        drop(store);

        let store = Store::open(&db).unwrap();
        store.delete_project(project.id).unwrap();
        let replay = store
            .mutate::<Project>(key, "add", |_| panic!("replay must not execute"))
            .unwrap();
        assert_eq!(receipt, replay);
        let conflict =
            store.mutate::<String>(key, "remove", |_| panic!("conflict must not execute"));
        assert!(matches!(conflict, Err(Error::IdempotencyConflict)));
    }
}
