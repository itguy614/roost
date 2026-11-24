use std::ffi::CString;

use roost_core::{
    ProjectId,
    worker_runtime::{
        DaemonStartup, LaunchIntent, ProcessCrash, ProcessLease, ProcessProfileId, ProjectIntent,
        WorkerLeaseState,
    },
};
use uuid::Uuid;

use crate::{Error, Store};

impl Store {
    pub fn project_intent(&self, id: ProjectId) -> Result<ProjectIntent, Error> {
        Ok(self
            .one(
                c"SELECT data FROM project_intents WHERE project_id = ?1",
                &id.0.to_string(),
            )?
            .unwrap_or_default())
    }

    pub fn set_project_intent(&self, id: ProjectId, intent: &ProjectIntent) -> Result<(), Error> {
        self.0.query(
            c"INSERT INTO project_intents (project_id, data) VALUES (?1, ?2)
              ON CONFLICT(project_id) DO UPDATE SET data = excluded.data",
            &[
                Some(&id.0.to_string()),
                Some(&serde_json::to_string(intent)?),
            ],
        )?;
        Ok(())
    }

    pub fn autostart_intents(&self) -> Result<Vec<(ProjectId, LaunchIntent)>, Error> {
        let mut intents = Vec::new();
        for project in self
            .list_projects()?
            .into_iter()
            .filter(|project| project.autostart)
        {
            if let ProjectIntent::Running(intent) = self.project_intent(project.id)? {
                intents.push((project.id, intent));
            }
        }
        Ok(intents)
    }

    pub fn begin_daemon_run(&self) -> Result<DaemonStartup, Error> {
        self.transaction(|store| {
            let state = store.0.query(
                c"SELECT clean_shutdown FROM daemon_lifecycle WHERE singleton = 1",
                &[],
            )?;
            store.0.exec(
                c"INSERT INTO daemon_lifecycle VALUES (1, 0)
                ON CONFLICT(singleton) DO UPDATE SET clean_shutdown = 0",
            )?;
            Ok(match state.first().map(String::as_str) {
                Some("1") => DaemonStartup::AfterCleanShutdown,
                Some(_) => DaemonStartup::AfterInterruptedShutdown,
                None => DaemonStartup::FirstStart,
            })
        })
    }

    /// Only a shutdown with no unresolved ownership can be marked clean.
    pub fn finish_daemon_run(&self) -> Result<bool, Error> {
        self.0.query(
            c"UPDATE daemon_lifecycle SET clean_shutdown = 1 WHERE singleton = 1
                AND NOT EXISTS(SELECT 1 FROM worker_leases WHERE complete = 0)
                AND NOT EXISTS(SELECT 1 FROM scheduler_leases WHERE complete = 0)",
            &[],
        )?;
        Ok(self.0.changes() == 1)
    }

    pub fn finish_process<P: ProcessProfileId>(
        &self,
        id: Uuid,
        report: Option<&ProcessCrash<P>>,
    ) -> Result<(), Error> {
        self.transaction(|store| {
            store.release_completed_process::<P>(id)?;
            if let Some(report) = report {
                store.insert_crash_report(report)?;
                store.count_metric_crash(report)?;
            }
            Ok(())
        })
    }

    pub fn finish_previous_process<P: ProcessProfileId>(
        &self,
        lease: &ProcessLease<P>,
    ) -> Result<(), Error> {
        let report = match lease.state {
            WorkerLeaseState::Exited { exit } if !exit.requested_stop && exit.code != Some(0) => Some(ProcessCrash {
                id: lease.id, project_id: lease.project_id, profile_id: lease.profile_id, slot: lease.slot, exit,
                reason: "process exit recovered from its guardian lease; output unavailable after daemon interruption".into(),
                output: Vec::new(),
            }),
            _ => None,
        };
        self.finish_process(lease.id, report.as_ref())
    }

    pub fn recover_completed_processes<P: ProcessProfileId>(&self) -> Result<(), Error> {
        for lease in self.completed_processes::<P>()? {
            self.finish_previous_process(&lease)?;
        }
        Ok(())
    }

    pub fn recent_crashes(
        &self,
        id: ProjectId,
    ) -> Result<Vec<roost_core::worker_runtime::CrashReport>, Error> {
        self.recent_process_crashes(id)
    }

    pub fn recent_process_crashes<P: ProcessProfileId>(
        &self,
        id: ProjectId,
    ) -> Result<Vec<ProcessCrash<P>>, Error> {
        self.0
            .query(
                &crash_sql::<P>(
                    "SELECT data FROM {crashes} WHERE project_id = ?1 ORDER BY sequence DESC",
                ),
                &[Some(&id.0.to_string())],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    fn insert_crash_report<P: ProcessProfileId>(
        &self,
        report: &ProcessCrash<P>,
    ) -> Result<(), Error> {
        let project = report.project_id.0.to_string();
        self.0.query(
            &crash_sql::<P>("INSERT INTO {crashes} (id, project_id, data) VALUES (?1, ?2, ?3)"),
            &[
                Some(&report.id.to_string()),
                Some(&project),
                Some(&serde_json::to_string(report)?),
            ],
        )?;
        self.0.query(
            &crash_sql::<P>("DELETE FROM {crashes} WHERE project_id = ?1 AND sequence IN
                (SELECT sequence FROM {crashes} WHERE project_id = ?1 ORDER BY sequence DESC LIMIT -1 OFFSET 12)"),
            &[Some(&project)],
        )?;
        self.0.exec(&crash_sql::<P>(
            "DELETE FROM {crashes} WHERE sequence IN
            (SELECT sequence FROM {crashes} ORDER BY sequence DESC LIMIT -1 OFFSET 128)",
        ))?;
        Ok(())
    }
}

fn crash_sql<P: ProcessProfileId>(template: &str) -> CString {
    let table = if P::SCHEDULER {
        "scheduler_crashes"
    } else {
        "worker_crashes"
    };
    CString::new(template.replace("{crashes}", table)).expect("static crash SQL has no NUL")
}

#[cfg(test)]
mod tests {
    use super::*;
    use roost_core::{Project, WorkerProfileId, worker_runtime::ProcessExit};

    #[test]
    fn crash_retention_is_bounded_per_project_and_across_the_database() {
        let base = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("roost-crashes-{}", Uuid::new_v4()));
        std::fs::create_dir(&base).unwrap();
        struct Scratch(std::path::PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let scratch = Scratch(base);
        let db = scratch.0.join("store.sqlite3");
        Store::initialize(&db).unwrap();
        let store = Store::open(&db).unwrap();
        let mut ids = Vec::new();
        store
            .transaction::<_, Error>(|store| {
                for index in 0..12 {
                    let dir = scratch.0.join(format!("app-{index}"));
                    std::fs::create_dir(&dir).unwrap();
                    std::fs::write(dir.join("artisan"), "").unwrap();
                    let project = Project::register(&dir).unwrap();
                    store.create_project(&project)?;
                    ids.push(project.id);
                    for timestamp in 0..14 {
                        store.insert_crash_report(&ProcessCrash {
                            id: Uuid::new_v4(),
                            project_id: project.id,
                            profile_id: WorkerProfileId(Uuid::new_v4()),
                            slot: 0,
                            exit: ProcessExit {
                                code: Some(1),
                                signal: None,
                                at: timestamp,
                                requested_stop: false,
                                stop_cause: None,
                                forced: false,
                            },
                            reason: "fixture".into(),
                            output: Vec::new(),
                        })?;
                    }
                }
                Ok(())
            })
            .unwrap();
        let counts: Vec<_> = ids
            .into_iter()
            .map(|id| store.recent_crashes(id).unwrap())
            .collect();
        assert!(counts.iter().all(|reports| reports.len() <= 12));
        assert_eq!(counts.iter().map(Vec::len).sum::<usize>(), 128);
        assert_eq!(counts.last().unwrap()[0].exit.at, 13);
    }
}
