use roost_core::{
    ProjectId,
    worker_runtime::{WorkerLease, WorkerLeaseState},
};
use uuid::Uuid;

use crate::{Error, Store};

impl Store {
    pub fn reserve_worker(&self, lease: &WorkerLease) -> Result<(), Error> {
        if lease.state != WorkerLeaseState::Reserved {
            return Err(Error::RevisionConflict);
        }
        self.0.query(
            c"INSERT INTO worker_leases (id, project_id, profile_id, slot, complete, data)
              SELECT ?1, ?2, id, ?4, 0, ?5 FROM worker_profiles WHERE id = ?3 AND project_id = ?2",
            &[
                Some(&lease.id.to_string()),
                Some(&lease.project_id.0.to_string()),
                Some(&lease.profile_id.0.to_string()),
                Some(&lease.slot.to_string()),
                Some(&serde_json::to_string(lease)?),
            ],
        )?;
        if self.0.changes() == 0 {
            return Err(Error::Reference);
        }
        Ok(())
    }

    pub fn worker_lease(&self, id: Uuid) -> Result<Option<WorkerLease>, Error> {
        self.one(
            c"SELECT data FROM worker_leases WHERE id = ?1",
            &id.to_string(),
        )
    }

    pub fn project_worker_leases(&self, id: ProjectId) -> Result<Vec<WorkerLease>, Error> {
        self.0
            .query(
                c"SELECT data FROM worker_leases WHERE project_id = ?1 ORDER BY profile_id, slot",
                &[Some(&id.0.to_string())],
            )?
            .iter()
            .map(|row| serde_json::from_str(row).map_err(Error::from))
            .collect()
    }

    pub fn record_worker_state(
        &self,
        mut lease: WorkerLease,
        state: WorkerLeaseState,
    ) -> Result<(), Error> {
        let complete = matches!(
            state,
            WorkerLeaseState::Exited { .. } | WorkerLeaseState::LaunchFailed
        );
        lease.state = state;
        self.0.query(
            c"UPDATE worker_leases SET complete = ?2, data = ?3 WHERE id = ?1 AND complete = 0",
            &[
                Some(&lease.id.to_string()),
                Some(if complete { "1" } else { "0" }),
                Some(&serde_json::to_string(&lease)?),
            ],
        )?;
        if self.0.changes() == 0 {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }

    pub fn release_worker(&self, id: Uuid) -> Result<(), Error> {
        self.0.query(
            c"DELETE FROM worker_leases WHERE id = ?1 AND complete = 1",
            &[Some(&id.to_string())],
        )?;
        if self.0.changes() == 0 {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }

    /// After a daemon restart only the guardian may certify its previous group is empty.
    pub fn clear_completed_worker_leases(&self) -> Result<(), Error> {
        self.0
            .query(c"DELETE FROM worker_leases WHERE complete = 1", &[])?;
        Ok(())
    }
}
