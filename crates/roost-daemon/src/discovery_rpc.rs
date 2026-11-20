use std::collections::VecDeque;

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

pub type Snapshots = VecDeque<(ProjectId, Revision, ConnectionSnapshot)>;

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
                encode(cache.iter().find_map(|(id, revision, snapshot)| {
                    (*id == params.id && *revision == project.revision).then_some(snapshot)
                }))
            }
            Self::Discover(params) => {
                let project = project(&store, params.id)?;
                let php = resolver
                    .resolve(&project, params.provider, params.php.as_deref())
                    .map_err(provider_error)?;
                let snapshot = roost_providers::discover_connections(&resolver, &project, &php)
                    .map_err(provider_error)?;
                let mut cache = service.snapshots.lock().map_err(|_| internal_error())?;
                cache.retain(|(id, _, _)| *id != project.id);
                if cache.len() == MAX_CACHED_PROJECTS {
                    cache.pop_front();
                }
                cache.push_back((project.id, project.revision, snapshot.clone()));
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
