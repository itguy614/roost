pub mod alerts;
pub mod events;
mod leases;
pub mod metrics;
pub mod queue_metrics;
mod runtime;
mod scheduler;
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

const SCHEDULER: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/004_scheduler.sql"), "\0").as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const PROCESS_METRICS: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/005_process_metrics.sql"), "\0").as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const QUEUE_METRICS: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/006_queue_metrics.sql"), "\0").as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const QUEUE_HISTORY_COVERAGE: &CStr = match CStr::from_bytes_with_nul(
    concat!(
        include_str!("../migrations/007_queue_history_coverage.sql"),
        "\0"
    )
    .as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const EVENTS: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/008_events.sql"), "\0").as_bytes(),
) {
    Ok(sql) => sql,
    Err(_) => panic!("migration contains NUL"),
};

const ALERT_CLOCK: &CStr = match CStr::from_bytes_with_nul(
    concat!(include_str!("../migrations/009_alert_clock.sql"), "\0").as_bytes(),
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
            let versions = store.0.query(c"PRAGMA user_version", &[])?;
            let version = versions.first().map(String::as_str);
            match version {
                Some("0") => {
                    store.0.exec(INITIAL)?;
                    store.0.exec(WORKER_LEASES)?;
                    store.0.exec(RUNTIME)?;
                    store.0.exec(SCHEDULER)?;
                }
                Some("1") => {
                    store.0.exec(WORKER_LEASES)?;
                    store.0.exec(RUNTIME)?;
                    store.0.exec(SCHEDULER)?;
                }
                Some("2") => {
                    store.0.exec(RUNTIME)?;
                    store.0.exec(SCHEDULER)?;
                }
                Some("3") => store.0.exec(SCHEDULER)?,
                Some("4" | "5" | "6" | "7" | "8" | "9") => (),
                _ => return Err(Error::Schema),
            }
            if !matches!(version, Some("5" | "6" | "7" | "8" | "9")) {
                store.0.exec(PROCESS_METRICS)?;
            }
            if !matches!(version, Some("6" | "7" | "8" | "9")) {
                store.0.exec(QUEUE_METRICS)?;
            }
            if !matches!(version, Some("7" | "8" | "9")) {
                store.0.exec(QUEUE_HISTORY_COVERAGE)?;
            }
            if !matches!(version, Some("8" | "9")) {
                store.0.exec(EVENTS)?;
            }
            if version != Some("9") {
                store.0.exec(ALERT_CLOCK)?;
            }
            Ok(())
        })
    }

    pub fn open(path: &Path) -> Result<Self, Error> {
        Ok(Self(sqlite::Connection::open(path, false)?))
    }

    pub fn open_without_busy_wait(path: &Path) -> Result<Self, Error> {
        let store = Self::open(path)?;
        store.0.exec(c"PRAGMA busy_timeout = 0")?;
        Ok(store)
    }

    fn transaction<T, E: From<Error>>(
        &self,
        work: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.transaction_with(c"BEGIN IMMEDIATE", work)
    }

    fn transaction_with<T, E: From<Error>>(
        &self,
        begin: &CStr,
        work: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.0.exec(begin).map_err(Error::from)?;
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

    pub fn group_projects(&self, group: GroupId) -> Result<Vec<Project>, Error> {
        self.0
            .query(
                c"SELECT data FROM projects WHERE group_id = ?1 ORDER BY path",
                &[Some(&group.0.to_string())],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
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
    fn crash_event_failure_rolls_back_report_and_retention_expires_pinned_cursor() {
        use crate::events::{EventPage, EventQuery};
        use roost_core::{
            scheduler::{SchedulerMode, SchedulerProfile, SchedulerProfileId},
            worker_runtime::{ProcessExit, ProcessLease, WorkerLeaseState},
        };
        let fixture = Fixture::new();
        let db = fixture.db();
        let project = fixture.project();
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        store.create_project(&project).unwrap();
        let profile_id = SchedulerProfileId(Uuid::new_v4());
        store
            .create_scheduler(&SchedulerProfile {
                id: profile_id,
                project_id: project.id,
                enabled: true,
                mode: SchedulerMode::ScheduleWork,
                crash_restart: CrashRestartPolicy::Never,
                graceful_stop_seconds: 1,
                revision: Revision(1),
            })
            .unwrap();
        let id = Uuid::new_v4();
        let lease = ProcessLease {
            id,
            profile_id,
            project_id: project.id,
            slot: 0,
            state: WorkerLeaseState::Reserved,
        };
        store.reserve_process(&lease).unwrap();
        let lease = store.claim_process(lease).unwrap();
        store
            .record_process_state(
                lease,
                WorkerLeaseState::Exited {
                    exit: ProcessExit {
                        code: Some(7),
                        signal: None,
                        at: 1_800_000_000,
                        requested_stop: false,
                        stop_cause: None,
                        forced: false,
                    },
                },
            )
            .unwrap();
        store.0.exec(c"CREATE TRIGGER fail_event BEFORE INSERT ON daemon_events BEGIN SELECT RAISE(ABORT,'fixture'); END").unwrap();
        assert!(
            store
                .recover_completed_processes::<SchedulerProfileId>()
                .is_err()
        );
        assert!(
            store
                .process_crash::<SchedulerProfileId>(id)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .process_lease::<SchedulerProfileId>(id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .process_lifecycle_totals(project.id, None)
                .unwrap()
                .crashes,
            0
        );
        store.0.exec(c"DROP TRIGGER fail_event").unwrap();
        store
            .recover_completed_processes::<SchedulerProfileId>()
            .unwrap();
        store
            .recover_completed_processes::<SchedulerProfileId>()
            .unwrap();
        assert!(
            store
                .process_lease::<SchedulerProfileId>(id)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .process_crash::<SchedulerProfileId>(id)
                .unwrap()
                .unwrap()
                .output
                .is_empty()
        );
        let EventPage::Rows { state, rows } = store
            .event_history(EventQuery {
                project: Some(project.id),
                lineage: None,
                after: 0,
                high: None,
                limit: 2,
            })
            .unwrap()
        else {
            panic!("expected event")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!(id));
        store.0.exec(c"WITH RECURSIVE samples(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM samples WHERE n<2048)
            INSERT INTO daemon_events(id,project_id,kind,data) SELECT 'seed-'||n,'other','process_crash',json_object('at',1800000000+n) FROM samples").unwrap();
        let event = roost_core::event::Event {
            id: Uuid::new_v4(),
            project_id: project.id,
            profile_id: profile_id.0,
            slot: 0,
            process_kind: roost_core::metrics::ProcessKind::Scheduler,
            kind: roost_core::event::EventKind::PermanentRecoveryFailure,
            at: 1_800_000_050,
            crash_id: None,
            actions_suppressed: false,
        };
        store.record_recovery_failure(&event).unwrap();
        let EventPage::Expired {
            state: retained,
            earliest: Some(earliest),
        } = store
            .event_history(EventQuery {
                project: Some(project.id),
                lineage: Some(&state.lineage),
                after: 1,
                high: Some(state.high),
                limit: 2,
            })
            .unwrap()
        else {
            panic!("expected expired cursor")
        };
        assert_eq!(retained.pruned, 2);
        assert_eq!(earliest.sequence, "3");
        assert_eq!(earliest.at, 1_800_000_002);
        let EventPage::Rows { rows, .. } = store
            .event_history(EventQuery {
                project: Some(project.id),
                lineage: Some(&state.lineage),
                after: 2,
                high: None,
                limit: 2,
            })
            .unwrap()
        else {
            panic!("watermark equality is safe")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!(event.id));
    }

    #[test]
    fn alert_attempt_budget_and_cooldown_survive_reopen_and_rule_replacement() {
        use roost_core::{
            alerts::{Episode, Evidence, Rule},
            metrics::{ProcessGeneration, ProcessKind},
        };
        let fixture = Fixture::new();
        let db = fixture.db();
        let project = fixture.project();
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        store.create_project(&project).unwrap();
        let mut profile = WorkerProfile {
            id: WorkerProfileId(Uuid::new_v4()),
            project_id: project.id,
            name: "fixture".into(),
            connection: "database".into(),
            queues: vec![],
            desired_instances: 1,
            sleep_seconds: 3,
            timeout_seconds: 60,
            tries: 1,
            graceful_stop_seconds: 1,
            crash_restart: CrashRestartPolicy::Never,
            code_restart: CodeRestartPolicy::Never,
            notifications: false,
            enabled: true,
            revision: Revision(1),
        };
        store.create_worker_profile(&profile).unwrap();
        let identity = ProcessGeneration {
            project_id: project.id,
            kind: ProcessKind::Worker,
            profile_id: profile.id.0,
            slot: 0,
            instance_id: Uuid::new_v4(),
            generation: 1,
        };
        let mut rule = Rule::RssBytes {
            clear_at: 0,
            breach_at: 1,
            restart: true,
        };
        let episode = |rule: &Rule, revision, at| Episode {
            id: Uuid::new_v4(),
            identity,
            rule: rule.clone(),
            rule_revision: revision,
            opened_at: at,
            breach_observed_at: at,
            breach_evidence: Evidence::RssBytes(3),
            actions_suppressed: false,
            restart_charged: false,
            restart_deferred: None,
        };
        let open = |store: &Store, rule: &Rule, revision, wall, elapsed| {
            store
                .stage_alert(episode(rule, revision, wall), elapsed)
                .unwrap()
                .unwrap()
                .commit()
                .unwrap()
        };
        store
            .transaction::<_, Error>(|store| {
                store.replace_alert_rules(&profile, &[], std::slice::from_ref(&rule), 10000)
            })
            .unwrap();
        let blocker = Store::open(&db).unwrap();
        blocker.0.exec(c"BEGIN").unwrap();
        blocker
            .0
            .query(c"SELECT count(*) FROM alert_episodes", &[])
            .unwrap();
        let writer = Store::open_without_busy_wait(&db).unwrap();
        let staged = writer
            .stage_alert(episode(&rule, profile.revision, 10001), 0)
            .unwrap()
            .unwrap();
        assert!(matches!(staged.commit(), Err(Error::Sql(error)) if error.code & 0xff == 5));
        assert_eq!(
            store
                .0
                .query(c"SELECT count(*) FROM alert_restart_attempts", &[])
                .unwrap(),
            ["0"]
        );
        assert!(store.list_alerts(project.id, None, 1).unwrap().is_empty());
        blocker.0.exec(c"ROLLBACK").unwrap();
        let first = open(&store, &rule, profile.revision, 10001, 0);
        assert!(first.restart_charged);
        let prior = store.alert_rules(&[profile.id]).unwrap();
        rule = Rule::RssBytes {
            clear_at: 0,
            breach_at: 2,
            restart: true,
        };
        profile = store
            .transaction(|store| {
                let profile = store.update_worker_profile(profile)?;
                store.replace_alert_rules(&profile, &prior, std::slice::from_ref(&rule), 10005)?;
                Ok::<_, Error>(profile)
            })
            .unwrap();
        assert_eq!(
            store.list_alerts(project.id, None, 1).unwrap()[0]["closed_reason"],
            "rule_updated"
        );
        drop(store);
        let store = Store::open(&db).unwrap();
        let suppressed = open(&store, &rule, profile.revision, 9_000_000, 10);
        assert!(suppressed.actions_suppressed && !suppressed.restart_charged);
        store.close_previous_alerts(10011).unwrap();
        let second = open(&store, &rule, profile.revision, 1, 302);
        assert!(second.restart_charged);
        store.close_alert(second.id, 10303, "recovered").unwrap();
        let capped = open(&store, &rule, profile.revision, 2, 603);
        assert!(!capped.actions_suppressed && !capped.restart_charged);
        store
            .close_alert(capped.id, 10604, "generation_ended")
            .unwrap();
        assert!(open(&store, &rule, profile.revision, 3, 3601).restart_charged);

        // An unreleased v8 installation may already have wall-clock charges.
        // Rebase that schema and keep every charge through a full fresh window.
        store.close_previous_alerts(20).unwrap();
        store
            .0
            .exec(
                c"DROP TABLE alert_clock; PRAGMA user_version = 8;
            UPDATE alert_restart_attempts SET charged_at=999999;
            UPDATE alert_cooldowns SET until_at=1000299",
            )
            .unwrap();
        drop(store);
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(store.alert_elapsed().unwrap(), 0);
        assert_eq!(
            store
                .0
                .query(
                    c"SELECT charged_at FROM alert_restart_attempts ORDER BY episode_id",
                    &[]
                )
                .unwrap(),
            ["0", "0"]
        );
        let cooldown = open(&store, &rule, profile.revision, 9_000_000, 299);
        assert!(cooldown.actions_suppressed && !cooldown.restart_charged);
        store
            .close_alert(cooldown.id, 9_000_001, "recovered")
            .unwrap();
        let capped = open(&store, &rule, profile.revision, 9_000_002, 3599);
        assert!(!capped.actions_suppressed && !capped.restart_charged);
        store
            .close_alert(capped.id, 9_000_003, "recovered")
            .unwrap();
        assert!(open(&store, &rule, profile.revision, 9_000_004, 3900).restart_charged);
    }

    #[test]
    fn initializes_reopens_and_rejects_newer_schema() {
        let fixture = Fixture::new();
        let db = fixture.db();
        assert!(Store::open(&db).is_err());
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(store.0.query(c"PRAGMA user_version", &[]).unwrap(), ["9"]);
        assert_eq!(store.0.query(c"PRAGMA foreign_keys", &[]).unwrap(), ["1"]);
        let project = fixture.project();
        store.create_project(&project).unwrap();
        drop(store);
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(store.list_projects().unwrap(), [project]);
        store.0.exec(c"PRAGMA user_version = 10").unwrap();
        assert!(matches!(Store::initialize(&db), Err(Error::Schema)));
        assert_eq!(store.0.query(c"PRAGMA user_version", &[]).unwrap(), ["10"]);
    }

    #[test]
    fn upgrades_milestone_one_database_without_losing_projects() {
        let fixture = Fixture::new();
        let db = fixture.db();
        let old = Store(sqlite::Connection::open(&db, true).unwrap());
        old.0.exec(INITIAL).unwrap();
        let group = Group::new("Legacy group".into()).unwrap();
        old.create_group(&group).unwrap();
        let group_json = old
            .0
            .query(c"SELECT data FROM groups", &[])
            .unwrap()
            .remove(0);
        assert!(!group_json.contains("shared_paths"));
        let mut project = fixture.project();
        project.group_id = Some(group.id);
        old.create_project(&project).unwrap();
        assert_eq!(old.0.query(c"PRAGMA user_version", &[]).unwrap(), ["1"]);
        Store::initialize(&db).unwrap();
        assert_eq!(old.0.query(c"PRAGMA user_version", &[]).unwrap(), ["9"]);
        assert_eq!(old.list_projects().unwrap(), std::slice::from_ref(&project));
        assert_eq!(old.get_group(group.id).unwrap(), Some(group.clone()));
        assert_eq!(
            old.group_projects(group.id).unwrap(),
            std::slice::from_ref(&project)
        );
        assert!(
            old.project_leases::<roost_core::WorkerProfileId>(project.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn metrics_migrate_v4_and_roll_back_batch_and_retention_faults() {
        use super::metrics::{HistoryPage, HistoryQuery, Resolution};
        use roost_core::metrics::{
            ProcessGeneration, ProcessKind, ProcessSample, RAW_SECONDS, ROLLUP_SECONDS,
        };

        let fixture = Fixture::new();
        let db = fixture.db();
        let old = Store(sqlite::Connection::open(&db, true).unwrap());
        for migration in [INITIAL, WORKER_LEASES, RUNTIME, SCHEDULER] {
            old.0.exec(migration).unwrap();
        }
        let project = fixture.project();
        old.create_project(&project).unwrap();
        drop(old);
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(
            store.get_project(project.id).unwrap(),
            Some(project.clone())
        );
        let key = ProcessGeneration {
            project_id: project.id,
            kind: ProcessKind::Worker,
            profile_id: Uuid::new_v4(),
            slot: 0,
            instance_id: Uuid::new_v4(),
            generation: 1,
        };
        let base = 1_800_000_000;
        let sample = |offset, cpu_percent| ProcessSample {
            observed_at: base + offset,
            identity: key,
            cpu_percent,
            rss_bytes: Some(4096),
            uptime_seconds: Some(offset),
            peak_rss_bytes: Some(4096),
        };
        store
            .0
            .exec(
                c"CREATE TRIGGER fail_second BEFORE INSERT ON process_metric_raw
            WHEN NEW.observed_at=1800000002 BEGIN SELECT RAISE(ABORT, 'fixture'); END",
            )
            .unwrap();
        assert!(
            store
                .save_process_samples(&[sample(0, None), sample(2, Some(20.0))])
                .is_err()
        );
        assert_eq!(
            store
                .0
                .query(c"SELECT count(*) FROM process_metric_series", &[])
                .unwrap(),
            ["0"]
        );
        store.0.exec(c"DROP TRIGGER fail_second").unwrap();
        store
            .save_process_samples(&[sample(0, None), sample(2, Some(20.0))])
            .unwrap();
        let page = |resolution, after, lineage| {
            store
                .process_history(HistoryQuery {
                    project_id: project.id,
                    profile: None,
                    resolution,
                    from: base,
                    to: base + 300,
                    after,
                    retention_floor: 0,
                    high: None,
                    lineage,
                    limit: 8,
                })
                .unwrap()
        };
        let HistoryPage::Rows { state, rows } = page(Resolution::Raw, 0, None) else {
            panic!("missing raw page")
        };
        assert_eq!(rows.len(), 2);
        store
            .0
            .exec(
                c"CREATE TRIGGER fail_rollup BEFORE INSERT ON process_metric_rollups
            BEGIN SELECT RAISE(ABORT, 'fixture'); END",
            )
            .unwrap();
        assert!(
            store
                .maintain_process_metrics(base + RAW_SECONDS + 300)
                .is_err()
        );
        assert!(
            matches!(page(Resolution::Raw,0,Some(&state.lineage)), HistoryPage::Rows { rows, .. } if rows.len() == 2)
        );
        // The read contract expires a cursor on time even if maintenance is stalled.
        let expired = store
            .process_history(HistoryQuery {
                project_id: project.id,
                profile: None,
                resolution: Resolution::Raw,
                from: base,
                to: base + 300,
                after: 1,
                retention_floor: base + 3,
                high: Some(state.high),
                lineage: Some(&state.lineage),
                limit: 8,
            })
            .unwrap();
        assert!(matches!(expired, HistoryPage::Expired(s) if s.pruned == 2));
        store.0.exec(c"DROP TRIGGER fail_rollup").unwrap();

        // A rollback-journal reader can let the writes proceed but block COMMIT.
        let reader = Store::open(&db).unwrap();
        reader.0.exec(c"BEGIN").unwrap();
        reader
            .0
            .query(c"SELECT count(*) FROM process_metric_raw", &[])
            .unwrap();
        let writer = Store::open_for_metric_writes(&db).unwrap();
        assert!(matches!(
            writer.maintain_process_metrics(base + RAW_SECONDS + 300),
            Err(Error::Sql(error)) if error.code == 5
        ));
        reader.0.exec(c"COMMIT").unwrap();
        assert_eq!(
            store
                .0
                .query(c"SELECT count(*) FROM process_metric_rollups", &[])
                .unwrap(),
            ["0"]
        );
        assert!(
            matches!(page(Resolution::Raw,0,Some(&state.lineage)), HistoryPage::Rows { rows, .. } if rows.len() == 2)
        );
        store
            .maintain_process_metrics(base + RAW_SECONDS + 300)
            .unwrap();
        assert!(
            matches!(page(Resolution::Raw,1,Some(&state.lineage)), HistoryPage::Expired(s) if s.pruned == 2)
        );
        assert!(
            matches!(page(Resolution::Raw,2,Some(&state.lineage)), HistoryPage::Rows { rows, .. } if rows.is_empty())
        );
        store
            .maintain_process_metrics(base + RAW_SECONDS + 300)
            .unwrap();
        let HistoryPage::Rows { rows, .. } = page(Resolution::FiveMinutes, 0, Some(&state.lineage))
        else {
            panic!("missing rollup")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["cpu_percent"]["samples"], 1);
        assert_eq!(rows[0]["cpu_percent"]["mean"], 20.0);
        assert_eq!(rows[0]["rss_bytes"]["samples"], 2);
        store
            .maintain_process_metrics(base + ROLLUP_SECONDS + 300)
            .unwrap();
        assert!(
            matches!(page(Resolution::FiveMinutes,0,Some(&state.lineage)), HistoryPage::Expired(s) if s.pruned == 1)
        );
    }

    #[test]
    fn history_enforces_timestamp_and_scope_inside_the_sequence_window() {
        use metrics::{HistoryPage, HistoryQuery, Resolution};

        let fixture = Fixture::new();
        let (project, other) = (fixture.project(), Fixture::new());
        let other_project = other.project();
        Store::initialize(&fixture.db()).unwrap();
        let store = Store::open(&fixture.db()).unwrap();
        store.create_project(&project).unwrap();
        store.create_project(&other_project).unwrap();
        for (id, project, profile) in [
            ("1", project.id, "target"),
            ("2", project.id, "sibling"),
            ("3", other_project.id, "target"),
        ] {
            store
                .0
                .query(
                    c"INSERT INTO process_metric_series VALUES (?1,?2,'worker',?3,0,?1,1)",
                    &[Some(id), Some(&project.0.to_string()), Some(profile)],
                )
                .unwrap();
        }

        // Put out-of-range records inside the sequence window explicitly. The
        // normal writers append in time order, so they cannot create this fixture.
        for (series, observed_at, bucket) in [
            ("1", "1800000300", "1800000300"),
            ("1", "1800000299", "1800000000"),
            ("3", "1800000301", "1800000300"),
            ("2", "1800000302", "1800000300"),
            ("1", "1800000900", "1800000900"),
            ("1", "1800000899", "1800000600"),
        ] {
            store
                .0
                .query(
                    c"INSERT INTO process_metric_raw (series_id,observed_at) VALUES (?1,?2)",
                    &[Some(series), Some(observed_at)],
                )
                .unwrap();
            store.0.query(
                c"INSERT INTO process_metric_rollups VALUES (NULL,?1,?2,0,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL)",
                &[Some(series), Some(bucket)],
            ).unwrap();
        }

        for resolution in [Resolution::Raw, Resolution::FiveMinutes] {
            let read = |profile, after, limit, to| {
                let HistoryPage::Rows { rows, .. } = store
                    .process_history(HistoryQuery {
                        project_id: project.id,
                        profile,
                        resolution,
                        from: 1_800_000_300,
                        to,
                        after,
                        retention_floor: 0,
                        high: Some(6),
                        lineage: None,
                        limit,
                    })
                    .unwrap()
                else {
                    panic!("expected a history page");
                };
                rows.into_iter()
                    .map(|row| row["sequence"].as_str().unwrap().to_owned())
                    .collect::<Vec<_>>()
            };
            assert_eq!(read(Some("target"), 0, 1, 1_800_000_900), ["1"]);
            assert_eq!(read(Some("target"), 1, 2, 1_800_000_900), ["6"]);
            assert_eq!(read(None, 0, 8, 1_800_000_900), ["1", "4", "6"]);
            assert_eq!(read(Some("target"), 0, 8, 1_800_000_899), ["1"]);
        }
    }

    #[test]
    fn migration_counts_replacements_for_preexisting_running_leases_only() {
        use roost_core::{
            scheduler::{SchedulerMode, SchedulerProfile, SchedulerProfileId},
            worker_runtime::{
                ProcessExit, ProcessIdentity, ProcessLease, ProcessProfileId, WorkerLeaseState,
            },
        };

        fn record_running<P: ProcessProfileId>(
            store: &Store,
            lease: ProcessLease<P>,
        ) -> ProcessLease<P> {
            store.reserve_process(&lease).unwrap();
            let claimed = store.claim_process(lease).unwrap();
            let running = WorkerLeaseState::Running {
                identity: ProcessIdentity {
                    pid: 123,
                    group: 123,
                    uid: 501,
                    started_seconds: 1,
                    started_microseconds: 0,
                },
            };
            store
                .record_process_state(claimed.clone(), running)
                .unwrap();
            store.process_lease(claimed.id).unwrap().unwrap()
        }

        fn retire<P: ProcessProfileId>(store: &Store, lease: ProcessLease<P>) -> ProcessLease<P> {
            let replacement = ProcessLease {
                id: Uuid::new_v4(),
                state: WorkerLeaseState::Reserved,
                ..lease.clone()
            };
            let id = lease.id;
            store
                .record_process_state(
                    lease,
                    WorkerLeaseState::Exited {
                        exit: ProcessExit {
                            code: Some(0),
                            signal: None,
                            at: 2,
                            requested_stop: true,
                            stop_cause: None,
                            forced: false,
                        },
                    },
                )
                .unwrap();
            store.finish_process::<P>(id, None).unwrap();
            replacement
        }

        let fixture = Fixture::new();
        let db = fixture.db();
        let old = Store(sqlite::Connection::open(&db, true).unwrap());
        for migration in [INITIAL, WORKER_LEASES, RUNTIME, SCHEDULER] {
            old.0.exec(migration).unwrap();
        }
        let project = fixture.project();
        old.create_project(&project).unwrap();
        let worker = WorkerProfile {
            id: WorkerProfileId(Uuid::new_v4()),
            project_id: project.id,
            name: "Worker".into(),
            connection: "database".into(),
            queues: Vec::new(),
            desired_instances: 2,
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
        let scheduler = SchedulerProfile {
            id: SchedulerProfileId(Uuid::new_v4()),
            project_id: project.id,
            enabled: true,
            mode: SchedulerMode::ScheduleWork,
            crash_restart: CrashRestartPolicy::Never,
            graceful_stop_seconds: 1,
            revision: Revision(1),
        };
        old.create_worker_profile(&worker).unwrap();
        old.create_scheduler(&scheduler).unwrap();

        let worker_lease = record_running(
            &old,
            ProcessLease {
                id: Uuid::new_v4(),
                project_id: project.id,
                profile_id: worker.id,
                slot: 0,
                state: WorkerLeaseState::Reserved,
            },
        );
        let pending = ProcessLease {
            id: Uuid::new_v4(),
            slot: 1,
            state: WorkerLeaseState::Reserved,
            ..worker_lease.clone()
        };
        old.reserve_process(&pending).unwrap();
        let pending = old.claim_process(pending).unwrap();
        let scheduler_lease = record_running(
            &old,
            ProcessLease {
                id: Uuid::new_v4(),
                project_id: project.id,
                profile_id: scheduler.id,
                slot: 0,
                state: WorkerLeaseState::Reserved,
            },
        );
        drop(old);

        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        let totals = || store.process_lifecycle_totals(project.id, None).unwrap();
        assert_eq!(totals().restarts, 0);
        let pending = retire(&store, pending);
        record_running(&store, pending);
        assert_eq!(totals().restarts, 0);
        let worker_lease = retire(&store, worker_lease);
        assert_eq!(totals().restarts, 0);
        let worker_lease = record_running(&store, worker_lease);
        store
            .record_process_state(worker_lease.clone(), worker_lease.state)
            .unwrap();
        assert_eq!(totals().restarts, 1);
        let scheduler_lease = retire(&store, scheduler_lease);
        record_running(&store, scheduler_lease);
        assert_eq!(totals().restarts, 2);
        for profile in [worker.id.0, scheduler.id.0] {
            assert_eq!(
                store
                    .process_lifecycle_totals(project.id, Some(&profile.to_string()))
                    .unwrap()
                    .restarts,
                1
            );
        }
        assert_eq!(totals().crashes, 0);
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
        old.reserve_process(&lease).unwrap();
        let claimed = old.claim_process(lease).unwrap();

        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(
            store.project_intent(project.id).unwrap(),
            ProjectIntent::Stopped
        );
        assert_eq!(
            store
                .process_lease::<roost_core::WorkerProfileId>(claimed.id)
                .unwrap(),
            Some(claimed)
        );
        assert_eq!(store.begin_daemon_run().unwrap(), DaemonStartup::FirstStart);
        assert!(!store.finish_daemon_run().unwrap());
        assert_eq!(
            store.begin_daemon_run().unwrap(),
            DaemonStartup::AfterInterruptedShutdown
        );
    }

    #[test]
    fn upgrades_v3_and_keeps_scheduler_ownership_unique() {
        use roost_core::{
            scheduler::{SchedulerIntent, SchedulerMode, SchedulerProfile, SchedulerProfileId},
            worker_runtime::{ProcessLease, WorkerLeaseState},
        };

        let fixture = Fixture::new();
        let db = fixture.db();
        let legacy = Store(sqlite::Connection::open(&db, true).unwrap());
        legacy.0.exec(INITIAL).unwrap();
        legacy.0.exec(WORKER_LEASES).unwrap();
        legacy.0.exec(RUNTIME).unwrap();
        let project = fixture.project();
        legacy.create_project(&project).unwrap();

        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(
            store.get_project(project.id).unwrap(),
            Some(project.clone())
        );
        assert_eq!(
            store.scheduler_intent(project.id).unwrap(),
            SchedulerIntent::Stopped
        );
        let profile = SchedulerProfile {
            id: SchedulerProfileId(Uuid::new_v4()),
            project_id: project.id,
            enabled: true,
            mode: SchedulerMode::ScheduleWork,
            crash_restart: CrashRestartPolicy::OnFailure,
            graceful_stop_seconds: 15,
            revision: Revision(1),
        };
        store.create_scheduler(&profile).unwrap();
        let duplicate = SchedulerProfile {
            id: SchedulerProfileId(Uuid::new_v4()),
            ..profile.clone()
        };
        assert!(matches!(
            store.create_scheduler(&duplicate),
            Err(Error::Duplicate)
        ));

        let lease = ProcessLease {
            id: Uuid::new_v4(),
            project_id: project.id,
            profile_id: profile.id,
            slot: 0,
            state: WorkerLeaseState::Reserved,
        };
        store.reserve_process(&lease).unwrap();
        let waiter = Store::open(&db).unwrap();
        let duplicate = ProcessLease {
            id: Uuid::new_v4(),
            ..lease.clone()
        };
        assert!(matches!(
            waiter.reserve_process(&duplicate),
            Err(Error::Duplicate)
        ));
        let claimed = store.claim_process(lease).unwrap();
        waiter
            .cancel_unclaimed_processes::<SchedulerProfileId>()
            .unwrap();
        assert_eq!(
            waiter
                .process_lease::<SchedulerProfileId>(claimed.id)
                .unwrap(),
            Some(claimed.clone())
        );
        assert!(matches!(
            waiter.delete_project(project.id),
            Err(Error::Reference)
        ));
        store
            .record_process_state(claimed.clone(), WorkerLeaseState::LaunchFailed)
            .unwrap();
        waiter
            .finish_process::<SchedulerProfileId>(claimed.id, None)
            .unwrap();
        waiter.reserve_process(&duplicate).unwrap();
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
