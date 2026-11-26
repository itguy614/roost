use roost_core::{ProjectId, event::Event};
use serde::Deserialize;

use crate::{
    Error, Store,
    metrics::{HistoryPage, HistoryState},
};

const CAPACITY: &str = "2048";

pub struct EventQuery<'a> {
    pub project: Option<ProjectId>,
    pub lineage: Option<&'a str>,
    pub after: u64,
    pub high: Option<u64>,
    pub limit: u32,
}

impl Store {
    /// Called inside the transaction committing the event's source record.
    pub(crate) fn append_event(&self, event: &Event) -> Result<(), Error> {
        let data = serde_json::to_string(event)?;
        self.0.query(
            c"INSERT INTO daemon_events(id,project_id,kind,data)
            VALUES (?1,?2,json_extract(?3,'$.kind'),?3) ON CONFLICT(id) DO NOTHING",
            &[
                Some(&event.id.to_string()),
                Some(&event.project_id.0.to_string()),
                Some(&data),
            ],
        )?;
        self.0.query(
            c"UPDATE daemon_event_state SET pruned = max(pruned,coalesce(
            (SELECT sequence FROM daemon_events ORDER BY sequence DESC LIMIT 1 OFFSET ?1),0))",
            &[Some(CAPACITY)],
        )?;
        self.0.exec(
            c"DELETE FROM daemon_events WHERE sequence <= (SELECT pruned FROM daemon_event_state)",
        )?;
        Ok(())
    }

    pub fn record_recovery_failure(&self, event: &Event) -> Result<(), Error> {
        self.transaction(|store| store.append_event(event))
    }

    pub fn event_history(&self, query: EventQuery<'_>) -> Result<HistoryPage, Error> {
        #[derive(Deserialize)]
        struct State {
            lineage: String,
            pruned: u64,
            high: u64,
        }
        self.transaction_with(c"BEGIN", |store| {
            let state = store.0.query(c"SELECT json_object('lineage',lineage,'pruned',pruned,
                'high',max(pruned,coalesce((SELECT max(sequence) FROM daemon_events),0))) FROM daemon_event_state", &[])?;
            let State { lineage, pruned, high } = serde_json::from_str(state.first().ok_or(Error::Schema)?)?;
            if query.lineage.is_some_and(|value| value != lineage) {
                return Ok(HistoryPage::DifferentLineage);
            }
            let state = HistoryState { lineage, pruned, high };
            if query.lineage.is_some() && query.after < pruned {
                return Ok(HistoryPage::Expired(state));
            }
            let rows = store.0.query(c"SELECT json_set(data,'$.sequence',CAST(sequence AS TEXT))
                FROM daemon_events WHERE (?1 IS NULL OR project_id=?1)
                AND sequence>?2 AND sequence<=?3 ORDER BY sequence LIMIT ?4",
                &[query.project.map(|id| id.0.to_string()).as_deref(), Some(&query.after.max(pruned).to_string()),
                Some(&query.high.unwrap_or(high).to_string()),Some(&query.limit.to_string())])?
                .iter().map(|row| serde_json::from_str(row).map_err(Error::from)).collect::<Result<_,_>>()?;
            Ok(HistoryPage::Rows {state,rows})
        })
    }
}
