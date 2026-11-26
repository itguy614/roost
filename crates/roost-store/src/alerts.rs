use roost_core::{
    ProjectId, WorkerProfile, WorkerProfileId,
    alerts::{ConfiguredRule, Episode, Metric, Rule},
    event::{Event, EventKind},
};
use serde_json::Value;
use uuid::Uuid;

use crate::{Error, Store};

impl Store {
    pub fn alert_rules(&self, profiles: &[WorkerProfileId]) -> Result<Vec<ConfiguredRule>, Error> {
        self.0.query(c"SELECT data FROM alert_rules WHERE profile_id IN (SELECT value FROM json_each(?1)) ORDER BY profile_id,metric",
            &[Some(&serde_json::to_string(profiles)?)])?.iter().map(|row| serde_json::from_str(row).map_err(Error::from)).collect()
    }

    pub fn alert_settings(
        &self,
        profile_id: WorkerProfileId,
    ) -> Result<(WorkerProfile, Vec<ConfiguredRule>), Error> {
        self.transaction_with(c"BEGIN", |store| {
            let profile = store
                .get_worker_profile(profile_id)?
                .ok_or(Error::NotFound)?;
            let rules = store.alert_rules(&[profile_id])?;
            Ok((profile, rules))
        })
    }

    /// The caller commits the profile revision, rule edits, closures and receipt together.
    pub fn replace_alert_rules(
        &self,
        profile: &WorkerProfile,
        before: &[ConfiguredRule],
        rules: &[Rule],
        at: u64,
    ) -> Result<(), Error> {
        let project = profile.project_id.0.to_string();
        let id = profile.id.0.to_string();
        let now = at.to_string();
        for metric in [Metric::CpuPercent, Metric::RssBytes] {
            let old = before
                .iter()
                .find(|row| row.rule.metric() == metric)
                .map(|row| &row.rule);
            let new = rules.iter().find(|rule| rule.metric() == metric);
            if old == new {
                continue;
            }
            let name = metric.name();
            self.0.query(
                c"UPDATE alert_episodes SET closed_at=?1,closed_reason=?2
                WHERE project_id=?3 AND profile_id=?4 AND metric=?5 AND closed_at IS NULL",
                &[
                    Some(&now),
                    Some(if new.is_some() {
                        "rule_updated"
                    } else {
                        "rule_removed"
                    }),
                    Some(&project),
                    Some(&id),
                    Some(name),
                ],
            )?;
            if let Some(rule) = new {
                let row = ConfiguredRule {
                    profile_id: profile.id,
                    revision: profile.revision,
                    configured_at: at,
                    rule: rule.clone(),
                };
                self.0.query(
                    c"INSERT INTO alert_rules VALUES (?1,?2,?3)
                    ON CONFLICT(profile_id,metric) DO UPDATE SET data=excluded.data",
                    &[Some(&id), Some(name), Some(&serde_json::to_string(&row)?)],
                )?;
            } else {
                self.0.query(
                    c"DELETE FROM alert_rules WHERE profile_id=?1 AND metric=?2",
                    &[Some(&id), Some(name)],
                )?;
            }
        }
        Ok(())
    }

    pub fn alert_elapsed(&self) -> Result<u64, Error> {
        self.0
            .query(c"SELECT elapsed FROM alert_clock", &[])?
            .first()
            .and_then(|value| value.parse().ok())
            .ok_or(Error::Schema)
    }

    pub fn stage_alert(
        &self,
        episode: Episode,
        elapsed: u64,
    ) -> Result<Option<AlertWrite<'_>>, Error> {
        self.0.exec(c"BEGIN IMMEDIATE")?;
        let mut write = AlertWrite {
            store: self,
            committed: false,
            episode,
        };
        let current = self.alert_rules(&[WorkerProfileId(write.episode.identity.profile_id)])?;
        if !current.iter().any(|rule| {
            rule.revision == write.episode.rule_revision && rule.rule == write.episode.rule
        }) {
            return Ok(None);
        }
        self.insert_alert(&mut write.episode, elapsed)?;
        Ok(Some(write))
    }

    fn insert_alert(&self, episode: &mut Episode, elapsed: u64) -> Result<(), Error> {
        let store = self;
        let profile = episode.identity.profile_id.to_string();
        let project = episode.identity.project_id.0.to_string();
        let slot = episode.identity.slot.to_string();
        let metric = episode.rule.metric().name();
        let at = episode.opened_at;
        let elapsed = store.alert_elapsed()?.max(elapsed);
        let now = elapsed.to_string();
        let id = episode.id.to_string();
        store.0.query(
            c"UPDATE alert_clock SET elapsed=?1 WHERE singleton=1",
            &[Some(&now)],
        )?;
        episode.actions_suppressed = store.0.query(
            c"SELECT 1 FROM alert_cooldowns
                WHERE profile_id=?1 AND slot=?2 AND metric=?3 AND until_at>?4",
            &[Some(&profile), Some(&slot), Some(metric), Some(&now)],
        )? == ["1"];
        if !episode.actions_suppressed {
            let until = elapsed.saturating_add(300).to_string();
            store.0.query(
                c"INSERT INTO alert_cooldowns VALUES (?1,?2,?3,?4)
                    ON CONFLICT(profile_id,slot,metric) DO UPDATE SET until_at=excluded.until_at",
                &[Some(&profile), Some(&slot), Some(metric), Some(&until)],
            )?;
            if episode.rule.restart() && episode.restart_deferred.is_none() {
                if let Some(floor) = elapsed.checked_sub(3600) {
                    store.0.query(
                        c"DELETE FROM alert_restart_attempts WHERE charged_at<=?1",
                        &[Some(&floor.to_string())],
                    )?;
                }
                store.0.query(c"INSERT INTO alert_restart_attempts(episode_id,profile_id,slot,charged_at)
                        SELECT ?1,?2,?3,?4 WHERE (SELECT count(*) FROM alert_restart_attempts WHERE profile_id=?2 AND slot=?3)<2",
                        &[Some(&id),Some(&profile),Some(&slot),Some(&now)])?;
                episode.restart_charged = store.0.changes() == 1;
            }
        }
        store.0.query(c"INSERT INTO alert_episodes(id,project_id,profile_id,slot,metric,data) VALUES (?1,?2,?3,?4,?5,?6)",
                &[Some(&id),Some(&project),Some(&profile),Some(&slot),Some(metric),Some(&serde_json::to_string(&episode)?)])?;
        store.append_event(&Event {
            id: episode.id,
            project_id: episode.identity.project_id,
            profile_id: episode.identity.profile_id,
            slot: episode.identity.slot,
            process_kind: episode.identity.kind,
            kind: EventKind::ResourceThreshold,
            at,
            crash_id: None,
            actions_suppressed: episode.actions_suppressed,
        })?;
        store.0.exec(c"DELETE FROM alert_episodes WHERE sequence IN (
                SELECT sequence FROM alert_episodes WHERE closed_at IS NOT NULL ORDER BY sequence DESC LIMIT -1 OFFSET 2048)")?;
        Ok(())
    }

    pub fn close_alert(&self, id: Uuid, at: u64, reason: &str) -> Result<(), Error> {
        self.0.query(c"UPDATE alert_episodes SET closed_at=?1,closed_reason=?2 WHERE id=?3 AND closed_at IS NULL",
            &[Some(&at.to_string()),Some(reason),Some(&id.to_string())])?;
        Ok(())
    }

    pub fn close_previous_alerts(&self, at: u64) -> Result<(), Error> {
        self.0.query(c"UPDATE alert_episodes SET closed_at=?1,closed_reason='daemon_restarted' WHERE closed_at IS NULL",
            &[Some(&at.to_string())])?;
        Ok(())
    }

    pub fn list_alerts(
        &self,
        project: ProjectId,
        before: Option<u64>,
        limit: u32,
    ) -> Result<Vec<Value>, Error> {
        self.0.query(c"SELECT json_set(data,'$.sequence',CAST(sequence AS TEXT),'$.closed_at',closed_at,'$.closed_reason',closed_reason)
            FROM alert_episodes WHERE project_id=?1 AND (?2 IS NULL OR sequence < ?2) ORDER BY sequence DESC LIMIT ?3",
            &[Some(&project.0.to_string()),before.map(|value| value.to_string()).as_deref(),Some(&limit.to_string())])?.iter().map(|row| serde_json::from_str(row).map_err(Error::from)).collect()
    }
}

/// A dropped or rejected episode rolls back its event and attempt charge.
pub struct AlertWrite<'a> {
    store: &'a Store,
    committed: bool,
    episode: Episode,
}

impl AlertWrite<'_> {
    pub fn commit(mut self) -> Result<Episode, Error> {
        self.store.0.exec(c"COMMIT")?;
        self.committed = true;
        Ok(self.episode.clone())
    }
}

impl Drop for AlertWrite<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.store.0.exec(c"ROLLBACK");
        }
    }
}
