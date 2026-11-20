use std::ffi::CString;

use roost_core::{
    ProjectId,
    worker_runtime::{ProcessLease, ProcessProfileId, WorkerLeaseState},
};
use uuid::Uuid;

use crate::{Error, Store};

fn lease_sql<P: ProcessProfileId>(template: &str) -> CString {
    let table = if P::SCHEDULER {
        "scheduler_leases"
    } else {
        "worker_leases"
    };
    CString::new(template.replace("{leases}", table)).expect("static lease SQL has no NUL")
}

impl Store {
    pub fn reserve_process<P: ProcessProfileId>(
        &self,
        lease: &ProcessLease<P>,
    ) -> Result<(), Error> {
        if lease.state != WorkerLeaseState::Reserved {
            return Err(Error::RevisionConflict);
        }
        let sql = if P::SCHEDULER {
            c"INSERT INTO scheduler_leases (id, project_id, profile_id, slot, complete, data)
              SELECT ?1, ?2, id, ?4, 0, ?5 FROM scheduler_profiles WHERE id = ?3 AND project_id = ?2"
        } else {
            c"INSERT INTO worker_leases (id, project_id, profile_id, slot, complete, data)
              SELECT ?1, ?2, id, ?4, 0, ?5 FROM worker_profiles WHERE id = ?3 AND project_id = ?2"
        };
        self.0.query(
            sql,
            &[
                Some(&lease.id.to_string()),
                Some(&lease.project_id.0.to_string()),
                Some(&lease.profile_id.uuid().to_string()),
                Some(&lease.slot.to_string()),
                Some(&serde_json::to_string(lease)?),
            ],
        )?;
        if self.0.changes() == 0 {
            return Err(Error::Reference);
        }
        Ok(())
    }

    pub fn process_lease<P: ProcessProfileId>(
        &self,
        id: Uuid,
    ) -> Result<Option<ProcessLease<P>>, Error> {
        self.one(
            &lease_sql::<P>("SELECT data FROM {leases} WHERE id = ?1"),
            &id.to_string(),
        )
    }

    pub fn project_leases<P: ProcessProfileId>(
        &self,
        id: ProjectId,
    ) -> Result<Vec<ProcessLease<P>>, Error> {
        self.0
            .query(
                &lease_sql::<P>(
                    "SELECT data FROM {leases} WHERE project_id = ?1 ORDER BY profile_id, slot",
                ),
                &[Some(&id.0.to_string())],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    pub fn claim_process<P: ProcessProfileId>(
        &self,
        mut lease: ProcessLease<P>,
    ) -> Result<ProcessLease<P>, Error> {
        if lease.state != WorkerLeaseState::Reserved {
            return Err(Error::RevisionConflict);
        }
        let reserved = serde_json::to_string(&lease)?;
        lease.state = WorkerLeaseState::Claimed;
        self.0.query(
            &lease_sql::<P>(
                "UPDATE {leases} SET data = ?3 WHERE id = ?1 AND data = ?2 AND complete = 0",
            ),
            &[
                Some(&lease.id.to_string()),
                Some(&reserved),
                Some(&serde_json::to_string(&lease)?),
            ],
        )?;
        self.check_lease_change()?;
        Ok(lease)
    }

    /// Delete only the exact unclaimed state so a delayed guardian cannot spawn.
    pub fn cancel_unclaimed_processes<P: ProcessProfileId>(&self) -> Result<(), Error> {
        let leases: Vec<ProcessLease<P>> = self.all(&lease_sql::<P>(
            "SELECT data FROM {leases} WHERE complete = 0",
        ))?;
        for lease in leases
            .into_iter()
            .filter(|lease| lease.state == WorkerLeaseState::Reserved)
        {
            self.0.query(
                &lease_sql::<P>(
                    "DELETE FROM {leases} WHERE id = ?1 AND data = ?2 AND complete = 0",
                ),
                &[
                    Some(&lease.id.to_string()),
                    Some(&serde_json::to_string(&lease)?),
                ],
            )?;
        }
        Ok(())
    }

    pub fn record_process_state<P: ProcessProfileId>(
        &self,
        mut lease: ProcessLease<P>,
        state: WorkerLeaseState,
    ) -> Result<(), Error> {
        let complete = state.is_complete();
        lease.state = state;
        self.0.query(
            &lease_sql::<P>(
                "UPDATE {leases} SET complete = ?2, data = ?3 WHERE id = ?1 AND complete = 0",
            ),
            &[
                Some(&lease.id.to_string()),
                Some(if complete { "1" } else { "0" }),
                Some(&serde_json::to_string(&lease)?),
            ],
        )?;
        self.check_lease_change()
    }

    pub(crate) fn release_completed_process<P: ProcessProfileId>(
        &self,
        id: Uuid,
    ) -> Result<(), Error> {
        self.0.query(
            &lease_sql::<P>("DELETE FROM {leases} WHERE id = ?1 AND complete = 1"),
            &[Some(&id.to_string())],
        )?;
        self.check_lease_change()
    }

    pub(crate) fn completed_processes<P: ProcessProfileId>(
        &self,
    ) -> Result<Vec<ProcessLease<P>>, Error> {
        self.all(&lease_sql::<P>(
            "SELECT data FROM {leases} WHERE complete = 1",
        ))
    }

    fn check_lease_change(&self) -> Result<(), Error> {
        if self.0.changes() == 0 {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }
}
