use std::path::PathBuf;

use roost_core::{
    ProjectId,
    scheduler::{SchedulerIntent, SchedulerProfile, SchedulerProfileId},
};

use crate::{Error, Store, initial_revision, next_revision};

impl Store {
    pub fn create_scheduler(&self, profile: &SchedulerProfile) -> Result<(), Error> {
        profile.validate()?;
        initial_revision(profile.revision)?;
        self.0.query(
            c"INSERT INTO scheduler_profiles (id, project_id, revision, data) VALUES (?1, ?2, 1, ?3)",
            &[Some(&profile.id.0.to_string()), Some(&profile.project_id.0.to_string()), Some(&serde_json::to_string(profile)?)],
        )?;
        Ok(())
    }

    pub fn scheduler(&self, id: SchedulerProfileId) -> Result<Option<SchedulerProfile>, Error> {
        self.one(
            c"SELECT data FROM scheduler_profiles WHERE id = ?1",
            &id.0.to_string(),
        )
    }

    pub fn project_scheduler(&self, project: ProjectId) -> Result<Option<SchedulerProfile>, Error> {
        self.one(
            c"SELECT data FROM scheduler_profiles WHERE project_id = ?1",
            &project.0.to_string(),
        )
    }

    pub fn update_scheduler(
        &self,
        mut profile: SchedulerProfile,
    ) -> Result<SchedulerProfile, Error> {
        profile.validate()?;
        let expected = profile.revision;
        profile.revision = next_revision(expected)?;
        self.0.query(
            c"UPDATE scheduler_profiles SET revision = ?3, data = ?4 WHERE id = ?1 AND project_id = ?2 AND revision = ?5",
            &[
                Some(&profile.id.0.to_string()), Some(&profile.project_id.0.to_string()),
                Some(&profile.revision.0.to_string()), Some(&serde_json::to_string(&profile)?),
                Some(&expected.0.to_string()),
            ],
        )?;
        self.updated(profile)
    }

    pub fn scheduler_intent(&self, project: ProjectId) -> Result<SchedulerIntent, Error> {
        Ok(self
            .one(
                c"SELECT data FROM scheduler_intents WHERE project_id = ?1",
                &project.0.to_string(),
            )?
            .unwrap_or_default())
    }

    pub fn set_scheduler_intent(
        &self,
        project: ProjectId,
        intent: &SchedulerIntent,
    ) -> Result<(), Error> {
        self.0.query(
            c"INSERT INTO scheduler_intents (project_id, data) VALUES (?1, ?2)
              ON CONFLICT(project_id) DO UPDATE SET data = excluded.data",
            &[
                Some(&project.0.to_string()),
                Some(&serde_json::to_string(intent)?),
            ],
        )?;
        Ok(())
    }

    pub fn autostart_schedulers(&self) -> Result<Vec<(ProjectId, PathBuf)>, Error> {
        let mut intents = Vec::new();
        for project in self
            .list_projects()?
            .into_iter()
            .filter(|project| project.autostart)
        {
            if let SchedulerIntent::Running { php } = self.scheduler_intent(project.id)? {
                intents.push((project.id, php));
            }
        }
        Ok(intents)
    }
}
