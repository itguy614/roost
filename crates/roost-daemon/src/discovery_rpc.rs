use std::{
    collections::VecDeque,
    sync::{Arc, Weak},
};

use roost_core::{Project, ProjectId, Revision, discovery::ConnectionSnapshot};
use roost_ipc::{
    RpcError,
    discovery::{ResolvePhpParams, SuggestQueueParams},
    workers::ProjectParams,
};
use roost_providers::{Error, Resolver};
use roost_store::{Error as StoreError, Store};
use serde_json::Value;

use crate::{DaemonService, Empty, decode, encode, internal_error, rpc, store_error};

const MAX_CACHED_PROJECTS: usize = 64;

#[derive(Default)]
pub struct Snapshots {
    cached: VecDeque<(ProjectId, Revision, ConnectionSnapshot)>,
    probes: Vec<(ProjectId, Weak<()>)>,
}

impl Snapshots {
    fn get(&self, project: ProjectId, revision: Revision) -> Option<&ConnectionSnapshot> {
        self.cached
            .iter()
            .find_map(|(id, cached_revision, snapshot)| {
                (*id == project && *cached_revision == revision).then_some(snapshot)
            })
    }

    fn begin(&mut self, id: ProjectId) -> Arc<()> {
        // Failed requests drop their ticket. The discovery semaphore limits live probes.
        self.probes.retain(|(_, probe)| probe.strong_count() != 0);
        let ticket = Arc::new(());
        self.probes.push((id, Arc::downgrade(&ticket)));
        ticket
    }

    fn publish(
        &mut self,
        project: ProjectId,
        revision: Revision,
        ticket: &Arc<()>,
        snapshot: &ConnectionSnapshot,
    ) -> bool {
        let ticket = Arc::downgrade(ticket);
        let Some(index) = self
            .probes
            .iter()
            .position(|(id, probe)| *id == project && Weak::ptr_eq(probe, &ticket))
        else {
            return false;
        };
        self.probes.swap_remove(index);
        self.cached.retain(|(id, _, _)| *id != project);
        if self.cached.len() == MAX_CACHED_PROJECTS {
            self.cached.pop_front();
        }
        self.cached.push_back((project, revision, snapshot.clone()));
        true
    }

    pub(super) fn invalidate(&mut self, project: ProjectId) {
        self.cached.retain(|(id, _, _)| *id != project);
        self.probes.retain(|(id, _)| *id != project);
    }
}

pub enum DiscoveryCall {
    Providers,
    Detect(ProjectParams),
    Resolve(ResolvePhpParams),
    Connections(ProjectParams),
    Discover(ResolvePhpParams),
    Suggestions(SuggestQueueParams),
}

impl DiscoveryCall {
    pub fn decode(method: &str, params: Value) -> Result<Self, RpcError> {
        match method {
            "provider.list" => {
                decode::<Empty>(params)?;
                Ok(Self::Providers)
            }
            "provider.detect" => Ok(Self::Detect(decode(params)?)),
            "provider.resolvePhp" => Ok(Self::Resolve(decode(params)?)),
            "queue.connections" => Ok(Self::Connections(decode(params)?)),
            "queue.discover" => Ok(Self::Discover(decode(params)?)),
            "queue.suggestions" => Ok(Self::Suggestions(decode(params)?)),
            _ => Err(RpcError::new(rpc::METHOD_NOT_FOUND, "method not available")),
        }
    }

    pub fn needs_permit(&self) -> bool {
        !matches!(self, Self::Providers | Self::Connections(_))
    }

    pub fn execute(self, service: &DaemonService) -> Result<Value, RpcError> {
        if matches!(self, Self::Providers) {
            return encode(roost_providers::PROVIDERS);
        }
        let store = Store::open(&service.db).map_err(store_error)?;
        let resolver = Resolver::default();
        match self {
            Self::Providers => unreachable!(),
            Self::Detect(params) => {
                let project = project(&store, params.id)?;
                encode(resolver.detect(&project).map_err(provider_error)?)
            }
            Self::Resolve(params) => {
                let project = project(&store, params.id)?;
                encode(
                    resolver
                        .resolve(&project, params.provider, params.php.as_deref())
                        .map_err(provider_error)?,
                )
            }
            Self::Connections(params) => {
                let project = project(&store, params.id)?;
                let cache = service.snapshots.lock().map_err(|_| internal_error())?;
                encode(cache.get(project.id, project.revision))
            }
            Self::Discover(params) => {
                let project = project(&store, params.id)?;
                let ticket = service
                    .snapshots
                    .lock()
                    .map_err(|_| internal_error())?
                    .begin(project.id);
                let php = resolver
                    .resolve(&project, params.provider, params.php.as_deref())
                    .map_err(provider_error)?;
                let snapshot = roost_providers::discover_connections(&resolver, &project, &php)
                    .map_err(provider_error)?;
                let mut cache = service.snapshots.lock().map_err(|_| internal_error())?;
                if !cache.publish(project.id, project.revision, &ticket, &snapshot) {
                    return Err(RpcError::new(
                        rpc::DISCOVERY_UNAVAILABLE,
                        "watched files changed during queue discovery; retry the request",
                    ));
                }
                encode(snapshot)
            }
            Self::Suggestions(params) => {
                let project = project(&store, params.id)?;
                project
                    .validate_location()
                    .map_err(|_| provider_error(Error::Project))?;
                let profiles = store
                    .worker_profiles_for_suggestions(params.id)
                    .map_err(store_error)?;
                encode(roost_providers::suggest_queues(
                    &project.path,
                    &profiles,
                    &params.explicit,
                ))
            }
        }
    }
}

fn project(store: &Store, id: ProjectId) -> Result<Project, RpcError> {
    store
        .get_project(id)
        .map_err(store_error)?
        .ok_or_else(|| store_error(StoreError::NotFound))
}

fn provider_error(error: Error) -> RpcError {
    use roost_process::ProbeError;
    let code = match error {
        Error::Project | Error::ConflictingSelection | Error::InvalidExecutable => {
            rpc::INVALID_PARAMS
        }
        Error::HerdProbe(ProbeError::Timeout) | Error::DiscoveryProbe(ProbeError::Timeout) => {
            rpc::DISCOVERY_TIMEOUT
        }
        Error::HerdProbe(ProbeError::OutputLimit)
        | Error::DiscoveryProbe(ProbeError::OutputLimit) => rpc::DISCOVERY_OUTPUT_LIMIT,
        _ => rpc::DISCOVERY_UNAVAILABLE,
    };
    RpcError::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use roost_core::discovery::PhpSource;

    use super::*;

    #[test]
    fn invalidation_rejects_an_in_flight_probe_without_touching_other_projects() {
        let id = ProjectId(uuid::Uuid::new_v4());
        let other = ProjectId(uuid::Uuid::new_v4());
        let revision = Revision(1);
        let snapshot = ConnectionSnapshot {
            default: None,
            connections: vec![],
            omitted_connections: 0,
            observed_at: 0,
            php_source: PhpSource::Override,
        };
        let mut cache = Snapshots::default();
        let previous = cache.begin(id);
        assert!(cache.publish(id, revision, &previous, &snapshot));
        let in_flight = cache.begin(id);
        let unrelated = cache.begin(other);

        cache.invalidate(id);
        assert!(cache.get(id, revision).is_none());
        assert!(!cache.publish(id, revision, &in_flight, &snapshot));
        assert!(cache.publish(other, revision, &unrelated, &snapshot));
        let fresh = cache.begin(id);
        assert!(cache.publish(id, revision, &fresh, &snapshot));
        assert!(cache.get(id, revision).is_some());
        assert!(cache.get(other, revision).is_some());
    }
}
